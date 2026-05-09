-- =============================================================================
-- realtime-crypto-pipeline — initial schema bootstrap.
-- Postgres applies every .sql in /docker-entrypoint-initdb.d alphabetically the
-- first time the container starts. Re-creating the volume is the only way to
-- re-run this file, so keep it idempotent (CREATE IF NOT EXISTS everywhere).
-- =============================================================================

-- Airflow uses a separate logical database from the warehouse so its metadata
-- tables don't pollute the analytical schema.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec

\connect crypto

-- -----------------------------------------------------------------------------
-- dim_asset — slowly-changing dimension for the assets we track.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_asset (
    asset_id        TEXT PRIMARY KEY,
    symbol          TEXT NOT NULL UNIQUE,
    name            TEXT,
    market_cap_rank INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  dim_asset IS 'One row per crypto asset we ingest from CoinGecko.';
COMMENT ON COLUMN dim_asset.asset_id IS 'CoinGecko id, e.g. "bitcoin".';
COMMENT ON COLUMN dim_asset.symbol   IS 'Upper-case ticker, e.g. "BTC".';

-- -----------------------------------------------------------------------------
-- fact_price_tick — append-only raw events from the Spark streaming sink.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_price_tick (
    tick_id              BIGSERIAL PRIMARY KEY,
    asset_id             TEXT NOT NULL,
    symbol               TEXT,
    name                 TEXT,
    price_usd            NUMERIC(20, 8),
    market_cap_usd       NUMERIC(24, 4),
    market_cap_rank      INTEGER,
    volume_24h_usd       NUMERIC(24, 4),
    price_change_pct_1h  NUMERIC(10, 4),
    price_change_pct_24h NUMERIC(10, 4),
    ingested_at          TIMESTAMPTZ NOT NULL,
    event_time           TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_fact_price_tick_event_time
    ON fact_price_tick (event_time DESC);
CREATE INDEX IF NOT EXISTS ix_fact_price_tick_asset_event_time
    ON fact_price_tick (asset_id, event_time DESC);

COMMENT ON TABLE fact_price_tick IS 'Raw per-poll observations. Source of truth for the batch layer.';

-- -----------------------------------------------------------------------------
-- agg_price_minute — speed-layer 1-minute OHLCV-style aggregates.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agg_price_minute (
    asset_id           TEXT NOT NULL,
    symbol             TEXT,
    window_start       TIMESTAMPTZ NOT NULL,
    window_end         TIMESTAMPTZ NOT NULL,
    open_usd           NUMERIC(20, 8),
    high_usd           NUMERIC(20, 8),
    low_usd            NUMERIC(20, 8),
    close_usd          NUMERIC(20, 8),
    avg_usd            NUMERIC(20, 8),
    avg_volume_24h_usd NUMERIC(24, 4),
    tick_count         INTEGER,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_id, window_start)
);

CREATE INDEX IF NOT EXISTS ix_agg_price_minute_window
    ON agg_price_minute (window_start DESC);

CREATE TABLE IF NOT EXISTS stg_agg_price_minute (LIKE agg_price_minute INCLUDING ALL);
ALTER TABLE stg_agg_price_minute DROP CONSTRAINT IF EXISTS stg_agg_price_minute_pkey;

-- -----------------------------------------------------------------------------
-- agg_price_hourly — batch-layer 1-hour aggregates produced by Airflow.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agg_price_hourly (
    asset_id           TEXT NOT NULL,
    symbol             TEXT,
    window_start       TIMESTAMPTZ NOT NULL,
    open_usd           NUMERIC(20, 8),
    high_usd           NUMERIC(20, 8),
    low_usd            NUMERIC(20, 8),
    close_usd          NUMERIC(20, 8),
    avg_usd            NUMERIC(20, 8),
    avg_volume_24h_usd NUMERIC(24, 4),
    tick_count         INTEGER,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_id, window_start)
);

CREATE TABLE IF NOT EXISTS stg_agg_price_hourly (LIKE agg_price_hourly INCLUDING ALL);
ALTER TABLE stg_agg_price_hourly DROP CONSTRAINT IF EXISTS stg_agg_price_hourly_pkey;

-- -----------------------------------------------------------------------------
-- agg_price_daily — daily summary, includes price_change_pct.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agg_price_daily (
    asset_id           TEXT NOT NULL,
    symbol             TEXT,
    trade_date         DATE NOT NULL,
    open_usd           NUMERIC(20, 8),
    high_usd           NUMERIC(20, 8),
    low_usd            NUMERIC(20, 8),
    close_usd          NUMERIC(20, 8),
    avg_usd            NUMERIC(20, 8),
    avg_volume_24h_usd NUMERIC(24, 4),
    tick_count         INTEGER,
    price_change_pct   NUMERIC(10, 4),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_id, trade_date)
);

CREATE TABLE IF NOT EXISTS stg_agg_price_daily (LIKE agg_price_daily INCLUDING ALL);
ALTER TABLE stg_agg_price_daily DROP CONSTRAINT IF EXISTS stg_agg_price_daily_pkey;

-- -----------------------------------------------------------------------------
-- data_quality_results — recorded by the Airflow DQ DAG.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_results (
    check_id     BIGSERIAL PRIMARY KEY,
    check_name   TEXT NOT NULL,
    check_target TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('PASS', 'WARN', 'FAIL')),
    metric_value NUMERIC,
    threshold    NUMERIC,
    details      TEXT,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_dq_results_checked_at
    ON data_quality_results (checked_at DESC);
