"""Crypto price producer.

Polls CoinGecko on a fixed cadence and publishes one Kafka message per coin per
poll. Messages are JSON, keyed by coin symbol so partition-stable consumers can
maintain per-asset ordering.

Run::

    python -m crypto_producer

Required env vars are documented in ``.env.example`` and ``producer/config.py``.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from types import FrameType
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from coingecko_client import CoinGeckoClient
from config import ProducerConfig

log = logging.getLogger("crypto_producer")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    # quiet down noisy libs
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def build_kafka_producer(cfg: ProducerConfig) -> KafkaProducer:
    """Construct an idempotent JSON producer."""
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers.split(","),
        client_id=cfg.kafka_client_id,
        acks=cfg.kafka_acks,
        enable_idempotence=True,
        retries=10,
        linger_ms=50,
        compression_type="gzip",
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )


def to_event(market_row: dict[str, Any], event_time: datetime) -> dict[str, Any]:
    """Project a CoinGecko market row to our canonical event schema."""
    return {
        "schema_version": 1,
        "asset_id": market_row.get("id"),
        "symbol": (market_row.get("symbol") or "").upper(),
        "name": market_row.get("name"),
        "price_usd": market_row.get("current_price"),
        "market_cap_usd": market_row.get("market_cap"),
        "market_cap_rank": market_row.get("market_cap_rank"),
        "volume_24h_usd": market_row.get("total_volume"),
        "price_change_pct_1h": market_row.get("price_change_percentage_1h_in_currency"),
        "price_change_pct_24h": market_row.get("price_change_percentage_24h_in_currency"),
        "ingested_at": event_time.isoformat(),
        "event_time": event_time.isoformat(),
    }


class GracefulShutdown:
    """Tiny SIGTERM/SIGINT handler so docker stop doesn't drop in-flight messages."""

    def __init__(self) -> None:
        self.should_exit = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum: int, _frame: FrameType | None) -> None:
        log.info("received signal %s, shutting down", signum)
        self.should_exit = True


def run(cfg: ProducerConfig) -> int:
    configure_logging(cfg.log_level)
    cfg.validate()
    log.info(
        "starting producer coins=%s interval=%ss bootstrap=%s topic=%s",
        cfg.coins,
        cfg.poll_interval_seconds,
        cfg.kafka_bootstrap_servers,
        cfg.kafka_topic_raw,
    )

    client = CoinGeckoClient(cfg.coingecko_base_url, cfg.coingecko_api_key)
    producer = build_kafka_producer(cfg)
    shutdown = GracefulShutdown()

    sent = 0
    try:
        while not shutdown.should_exit:
            cycle_started_at = time.monotonic()
            event_time = datetime.now(tz=timezone.utc)
            try:
                rows = client.fetch_markets(cfg.coins)
            except Exception as exc:
                log.error("fetch_markets failed: %s", exc)
                _sleep_until_next_cycle(cycle_started_at, cfg.poll_interval_seconds, shutdown)
                continue

            for row in rows:
                event = to_event(row, event_time)
                key = event["symbol"] or event.get("asset_id") or "unknown"
                try:
                    producer.send(cfg.kafka_topic_raw, key=key, value=event)
                    sent += 1
                except KafkaError as exc:
                    log.error("kafka send failed for %s: %s", key, exc)

            producer.flush(timeout=10)
            log.info(
                "published %s rows (total=%s) for event_time=%s",
                len(rows),
                sent,
                event_time.isoformat(),
            )
            _sleep_until_next_cycle(cycle_started_at, cfg.poll_interval_seconds, shutdown)
    finally:
        log.info("flushing and closing producer (sent=%s)", sent)
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
        finally:
            client.close()
    return 0


def _sleep_until_next_cycle(
    cycle_started_at: float, interval_seconds: int, shutdown: GracefulShutdown
) -> None:
    elapsed = time.monotonic() - cycle_started_at
    remaining = max(0.0, interval_seconds - elapsed)
    # Sleep in small chunks so SIGTERM is responsive.
    deadline = time.monotonic() + remaining
    while not shutdown.should_exit and time.monotonic() < deadline:
        time.sleep(min(0.5, deadline - time.monotonic()))


if __name__ == "__main__":
    sys.exit(run(ProducerConfig()))
