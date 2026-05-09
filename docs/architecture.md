# Architecture

This pipeline implements a **Lambda Architecture** over crypto market data, with
a clean separation between the speed layer (Spark Structured Streaming) and the
batch layer (Airflow-orchestrated Spark jobs). PostgreSQL serves both layers
from a single warehouse schema so the dashboard and SQL consumers don't have to
care which path produced a given row.

## Components

```
                    ┌──────────────────────┐
                    │   CoinGecko REST API │
                    └──────────┬───────────┘
                               │ HTTP poll (every 30s)
                               ▼
                    ┌──────────────────────┐
                    │    Kafka Producer    │   producer/crypto_producer.py
                    │       (Python)       │
                    └──────────┬───────────┘
                               │
                               ▼
                  ┌────────────────────────────┐
                  │   Apache Kafka (KRaft)     │
                  │   topic: crypto.prices.raw │
                  └─────┬─────────────────┬────┘
                        │                 │
              streaming │                 │ batch
                        ▼                 ▼
            ┌────────────────────┐  ┌────────────────────┐
            │  Spark Structured  │  │  Spark Batch Job   │
            │     Streaming      │  │ (Airflow-triggered)│
            │ 1-min aggregations │  │ hourly + daily agg │
            └─────────┬──────────┘  └──────────┬─────────┘
                      │                        │
                      └──────────┬─────────────┘
                                 ▼
                      ┌────────────────────┐
                      │     PostgreSQL     │
                      │  raw + minute +    │
                      │  hourly + daily    │
                      └──────────┬─────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
       ┌────────────┐    ┌────────────┐     ┌────────────┐
       │  Airflow   │    │ Streamlit  │     │  Ad-hoc    │
       │ DAGs (DQ,  │    │ Dashboard  │     │   SQL      │
       │ ETL, daily)│    │            │     │            │
       └────────────┘    └────────────┘     └────────────┘
```

### Producer

A small Python service in `producer/`. Polls the public CoinGecko `/coins/markets`
endpoint on a configurable cadence (default 30s) and emits one Kafka record per
asset per cycle. Uses an idempotent producer with gzip compression and
exponential-backoff retries on transient HTTP failures.

Key design choices:
- **Schema-versioned JSON.** Cheap to debug, easy to evolve, no schema registry
  needed for the demo. ``schema_version`` is included so consumers can branch
  if the contract changes.
- **Symbol-based keys** so partitions remain stable for downstream stateful
  consumers.
- **Graceful shutdown** on SIGTERM ensures `docker compose down` doesn't drop
  in-flight messages.

### Kafka

Single-broker Confluent Kafka 7.5 in **KRaft mode** (no ZooKeeper). One topic:
`crypto.prices.raw` (configurable). Partitioning is left at defaults; the load
is small enough that one partition is fine for the demo.

### Spark Structured Streaming

`spark/jobs/streaming_aggregator.py` runs continuously. It maintains two
streaming queries on the same source:

1. **Raw sink** — appends every tick to `fact_price_tick` with a 20-second
   trigger. Acts as the system of record.
2. **Minute aggregator** — `groupBy(window(event_time, "1 minute"), symbol)`
   with a 2-minute watermark, then a `foreachBatch` that upserts each window
   into `agg_price_minute` via `INSERT ... ON CONFLICT DO UPDATE`.

Checkpoints are persisted under `/opt/spark/checkpoints/{raw,minute}` so the
job can resume exactly where it stopped after a restart.

### Airflow

Three DAGs orchestrate the batch layer and observability:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `crypto_batch_etl` | `5 * * * *` | Hourly rollup minute → hourly aggregates |
| `crypto_data_quality` | every 15 min | Freshness, null-rate, asset-coverage checks |
| `crypto_daily_report` | `10 0 * * *` | Daily aggregate + leaderboard |

The hourly + daily DAGs invoke `spark/jobs/batch_transformer.py` via
`docker exec` against the `crypto-spark-stream` container so we don't have to
ship a JVM in the Airflow image. Idempotency is enforced by `ON CONFLICT`
upserts in the staging-table SQL.

### PostgreSQL

Single Postgres 16 instance with two logical databases:
- `crypto` — analytical schema (see [data-model.md](data-model.md)).
- `airflow` — Airflow's own metadata.

Init scripts under `postgres/init/` are applied on first volume creation; the
schema is **idempotent**, so re-running the bootstrap is safe.

### Streamlit dashboard

`dashboard/app.py` reads directly from Postgres. Three tabs:
- **Live price** — 1-minute close price chart for a chosen symbol over a
  configurable window.
- **Daily movers** — leaderboard from `agg_price_daily`.
- **Data quality** — last 50 entries from `data_quality_results`.

The dashboard auto-refreshes via a `<meta http-equiv="refresh">` tag every
`DASHBOARD_REFRESH_SECONDS` seconds.

## Deployment topology

Everything is containerized via Docker Compose. The full graph:

```
postgres ──── airflow-init ──── airflow (scheduler + webserver)
            │
kafka ──────┼─── producer
            └─── spark-stream (streaming aggregator)
postgres ──── dashboard
```

`airflow-init` runs to completion once on `db migrate`, creates the admin
user, then exits. The main `airflow` service starts after `airflow-init`
finishes successfully.

## Failure modes

- **CoinGecko down / rate-limited.** Producer logs and retries with
  exponential backoff. No backpressure to Kafka because we treat each cycle
  as best-effort.
- **Kafka down.** Producer caches messages in its in-memory buffer; if the
  buffer fills, sends raise. The container exits, Docker restarts it.
- **Postgres down.** Spark `foreachBatch` raises and Structured Streaming
  retries from the last checkpoint after the next trigger.
- **DAG run hits a SQL error.** Airflow marks the run failed; on the next
  schedule the DAG retries (default `retries=2`).
