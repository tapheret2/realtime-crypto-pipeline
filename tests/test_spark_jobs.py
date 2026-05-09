"""Pure-Python tests for the Spark batch transformation logic.

We don't exercise the streaming sink end-to-end (that would require Kafka and
Postgres). Instead, we test the deterministic transforms: the OHLCV-style
aggregation in ``aggregate_minute`` and the rollups in ``batch_transformer``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row  # noqa: E402

from batch_transformer import aggregate_daily, aggregate_hourly  # noqa: E402
from streaming_aggregator import EVENT_SCHEMA, aggregate_minute  # noqa: E402


def _ts(minute: int) -> datetime:
    return datetime(2026, 5, 9, 12, minute, tzinfo=timezone.utc)


def test_aggregate_minute_groups_by_window(spark):
    rows = [
        Row(
            schema_version=1,
            asset_id="bitcoin",
            symbol="BTC",
            name="Bitcoin",
            price_usd=100.0,
            market_cap_usd=1.0,
            market_cap_rank=1,
            volume_24h_usd=2.0,
            price_change_pct_1h=0.0,
            price_change_pct_24h=0.0,
            ingested_at=_ts(0),
            event_time=_ts(0),
        ),
        Row(
            schema_version=1,
            asset_id="bitcoin",
            symbol="BTC",
            name="Bitcoin",
            price_usd=110.0,
            market_cap_usd=1.0,
            market_cap_rank=1,
            volume_24h_usd=2.0,
            price_change_pct_1h=0.0,
            price_change_pct_24h=0.0,
            ingested_at=_ts(0) + timedelta(seconds=20),
            event_time=_ts(0) + timedelta(seconds=20),
        ),
        Row(
            schema_version=1,
            asset_id="bitcoin",
            symbol="BTC",
            name="Bitcoin",
            price_usd=120.0,
            market_cap_usd=1.0,
            market_cap_rank=1,
            volume_24h_usd=2.0,
            price_change_pct_1h=0.0,
            price_change_pct_24h=0.0,
            ingested_at=_ts(1) + timedelta(seconds=5),
            event_time=_ts(1) + timedelta(seconds=5),
        ),
    ]
    df = spark.createDataFrame(rows, schema=EVENT_SCHEMA)
    out = aggregate_minute(df).orderBy("window_start").collect()

    assert len(out) == 2
    first = out[0]
    assert first.symbol == "BTC"
    assert first.high_usd == 110.0
    assert first.low_usd == 100.0
    assert first.tick_count == 2

    second = out[1]
    assert second.high_usd == 120.0
    assert second.low_usd == 120.0
    assert second.tick_count == 1


def test_aggregate_hourly_collapses_60_minutes(spark):
    rows = [
        (
            "bitcoin",
            "BTC",
            _ts(0) + timedelta(minutes=i),
            _ts(0) + timedelta(minutes=i + 1),
            100.0 + i,
            105.0 + i,
            95.0 + i,
            102.0 + i,
            100.0 + i,
            1.0,
            5,
        )
        for i in range(60)
    ]
    cols = [
        "asset_id",
        "symbol",
        "window_start",
        "window_end",
        "open_usd",
        "high_usd",
        "low_usd",
        "close_usd",
        "avg_usd",
        "avg_volume_24h_usd",
        "tick_count",
    ]
    df = spark.createDataFrame(rows, cols)

    out = aggregate_hourly(df).collect()
    assert len(out) == 1
    row = out[0]
    assert row.tick_count == 5 * 60  # 5 ticks per minute, 60 minutes
    assert row.high_usd == pytest.approx(105.0 + 59)
    assert row.low_usd == pytest.approx(95.0)


def test_aggregate_daily_computes_change_pct(spark):
    rows = [
        (
            "bitcoin",
            "BTC",
            _ts(0) + timedelta(minutes=i),
            _ts(0) + timedelta(minutes=i + 1),
            100.0 if i == 0 else None,  # only the first row provides "open"
            100.0 + i,
            100.0 - i,
            110.0 if i == 59 else None,  # only the last row provides "close"
            100.0,
            1.0,
            1,
        )
        for i in range(60)
    ]
    cols = [
        "asset_id",
        "symbol",
        "window_start",
        "window_end",
        "open_usd",
        "high_usd",
        "low_usd",
        "close_usd",
        "avg_usd",
        "avg_volume_24h_usd",
        "tick_count",
    ]
    df = spark.createDataFrame(rows, cols)
    out = aggregate_daily(df).collect()
    assert len(out) == 1
    row = out[0]
    assert row.open_usd == pytest.approx(100.0)
    assert row.close_usd == pytest.approx(110.0)
    assert row.price_change_pct == pytest.approx(10.0)
