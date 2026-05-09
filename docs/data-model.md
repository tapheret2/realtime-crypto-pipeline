# Data model

The warehouse follows a **star schema** with a small slowly-changing dimension
(`dim_asset`) and one append-only fact table (`fact_price_tick`). Aggregates
are pre-computed at three grains so the dashboard never has to scan raw ticks.

```
                 ┌──────────────────┐
                 │    dim_asset     │
                 │ asset_id  (PK)   │
                 │ symbol           │
                 │ name             │
                 │ market_cap_rank  │
                 └────────┬─────────┘
                          │
       ┌──────────────────┼──────────────────────┬─────────────────────┐
       │                  │                      │                     │
┌──────▼──────┐  ┌────────▼─────────┐  ┌─────────▼─────────┐  ┌────────▼─────────┐
│fact_price   │  │agg_price_minute  │  │agg_price_hourly   │  │agg_price_daily   │
│   _tick     │  │  (asset,window)  │  │ (asset,window)    │  │ (asset,date)     │
│ tick_id PK  │  │ open/high/low/   │  │ open/high/low/    │  │ open/high/low/   │
│ event_time  │  │ close/avg/vol/   │  │ close/avg/vol/    │  │ close/avg/vol/   │
│ price_usd   │  │ tick_count       │  │ tick_count        │  │ price_change_pct │
└─────────────┘  └──────────────────┘  └───────────────────┘  └──────────────────┘
       ▲
       │ source of truth
       │
   producer → Kafka → Spark streaming
```

## Conventions

- All timestamps are **`TIMESTAMPTZ` in UTC**. No timezone funny business.
- Prices: `NUMERIC(20,8)` — precise enough for satoshi-level USD prices.
- Volumes / market caps: `NUMERIC(24,4)` — supports trillion-dollar caps.
- Aggregate primary key is **(asset_id, window_start)** for minute/hourly,
  **(asset_id, trade_date)** for daily. Upserts target the PK directly.
- `updated_at` columns are populated on every UPSERT so we can spot stale rows.

## Tables

### `dim_asset`
| column | type | notes |
|--------|------|-------|
| asset_id | TEXT PK | CoinGecko id, e.g. `bitcoin` |
| symbol | TEXT UNIQUE | uppercase ticker, e.g. `BTC` |
| name | TEXT | display name |
| market_cap_rank | INTEGER | as reported by CoinGecko |
| created_at, updated_at | TIMESTAMPTZ | bookkeeping |

Refreshed by `crypto_batch_etl.refresh_dim_asset` and seeded by
`postgres/init/02_seed_data.sql`.

### `fact_price_tick`
Append-only stream of raw events from the producer. Indexed on
`event_time DESC` and `(asset_id, event_time DESC)` for fast recent-window
scans by the dashboard.

### `agg_price_minute`
1-minute OHLCV-style aggregates produced by Spark Structured Streaming. PK is
`(asset_id, window_start)` so the upsert path is `INSERT ... ON CONFLICT
(asset_id, window_start) DO UPDATE`. `stg_agg_price_minute` is a flat copy
without primary key, used as the JDBC overwrite target before the MERGE.

### `agg_price_hourly`
Same shape as `agg_price_minute` but at 1-hour granularity. Built by
`spark/jobs/batch_transformer.py hourly` invoked from
`crypto_batch_etl`.

### `agg_price_daily`
Daily aggregate with an extra `price_change_pct` column. Built by
`spark/jobs/batch_transformer.py daily` from `crypto_daily_report`.

### `data_quality_results`
Append-only audit log for the periodic data quality DAG. Fields:
| column | type | example |
|--------|------|---------|
| check_name | TEXT | `raw_freshness` |
| check_target | TEXT | `fact_price_tick` |
| status | TEXT | `PASS` / `WARN` / `FAIL` |
| metric_value | NUMERIC | `42.7` |
| threshold | NUMERIC | `300` |
| details | TEXT | human-readable hint |
| checked_at | TIMESTAMPTZ | `NOW()` default |

## Partitioning roadmap

For a portfolio demo, ten coins at 30-second cadence produces ~28k ticks per
day — Postgres handles that comfortably. If we ever scale to thousands of
assets, the natural next step is **monthly partitioning of `fact_price_tick`
by `event_time`** (using PG declarative partitioning) plus dropping the
`stg_*` staging tables in favor of `COPY` + `INSERT ... SELECT`.
