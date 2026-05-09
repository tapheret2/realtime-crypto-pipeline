"""Tests for the CoinGecko -> Kafka producer."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
import responses

from coingecko_client import CoinGeckoClient, CoinGeckoError
from config import ProducerConfig
from crypto_producer import to_event


@pytest.fixture
def sample_market_row() -> dict:
    return {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 67234.12,
        "market_cap": 1325000000000,
        "market_cap_rank": 1,
        "total_volume": 28000000000,
        "price_change_percentage_1h_in_currency": 0.42,
        "price_change_percentage_24h_in_currency": -1.05,
    }


def test_to_event_projects_canonical_schema(sample_market_row):
    ts = datetime(2026, 5, 9, 12, 30, tzinfo=timezone.utc)
    event = to_event(sample_market_row, ts)

    assert event["schema_version"] == 1
    assert event["asset_id"] == "bitcoin"
    assert event["symbol"] == "BTC"  # upper-cased
    assert event["price_usd"] == pytest.approx(67234.12)
    assert event["market_cap_rank"] == 1
    assert event["price_change_pct_1h"] == pytest.approx(0.42)
    assert event["event_time"] == ts.isoformat()
    assert event["ingested_at"] == ts.isoformat()


def test_to_event_handles_missing_optional_fields():
    ts = datetime(2026, 5, 9, 12, 30, tzinfo=timezone.utc)
    event = to_event({"id": "bitcoin", "symbol": None, "current_price": None}, ts)

    assert event["asset_id"] == "bitcoin"
    assert event["symbol"] == ""  # null symbol becomes empty string after upper()
    assert event["price_usd"] is None
    assert event["market_cap_rank"] is None


def test_config_validation_rejects_short_poll_interval(monkeypatch):
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "1")
    cfg = ProducerConfig()
    with pytest.raises(ValueError, match="POLL_INTERVAL_SECONDS"):
        cfg.validate()


def test_config_validation_rejects_empty_coins(monkeypatch):
    monkeypatch.setenv("COINS", "")
    cfg = ProducerConfig()
    with pytest.raises(ValueError, match="COINS"):
        cfg.validate()


@responses.activate
def test_coingecko_client_fetches_markets(sample_market_row):
    responses.add(
        method=responses.GET,
        url="https://api.coingecko.com/api/v3/coins/markets",
        json=[sample_market_row],
        status=200,
    )
    client = CoinGeckoClient(base_url="https://api.coingecko.com/api/v3")
    rows = client.fetch_markets(["bitcoin"])
    assert len(rows) == 1
    assert rows[0]["id"] == "bitcoin"


@responses.activate
def test_coingecko_client_raises_on_rate_limit():
    responses.add(
        method=responses.GET,
        url="https://api.coingecko.com/api/v3/coins/markets",
        status=429,
        json={"status": {"error_code": 429}},
    )
    client = CoinGeckoClient(base_url="https://api.coingecko.com/api/v3")
    with pytest.raises(CoinGeckoError):
        client.fetch_markets(["bitcoin"])
