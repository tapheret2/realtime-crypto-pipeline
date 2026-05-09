# realtime-crypto-pipeline

End-to-end real-time data engineering pipeline that ingests cryptocurrency prices from the
public CoinGecko API, streams them through Kafka, processes them with Spark Structured
Streaming, persists curated tables in PostgreSQL, orchestrates batch jobs with Apache
Airflow, and serves analytics through a Streamlit dashboard. Everything is containerized
with Docker Compose so the whole stack starts with a single command.

[![CI](https://github.com/tapheret2/realtime-crypto-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/tapheret2/realtime-crypto-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![Kafka](https://img.shields.io/badge/kafka-7.5-black)
![Airflow](https://img.shields.io/badge/airflow-2.9-red)
![License](https://img.shields.io/badge/license-MIT-green)

## Architecture

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

The design follows a **Lambda Architecture**: Spark Structured Streaming handles the
speed layer (low-latency 1-minute aggregates), and Airflow-triggered Spark batch jobs
handle the batch layer (hourly and daily aggregates over the raw history). PostgreSQL
serves both layers from the same star schema.

## Tech stack

| Layer | Technology |
|-------|------------|
| Ingestion | Python 3.11, `requests`, `kafka-python` |
| Message bus | Apache Kafka 7.5 (Confluent) in KRaft mode |
| Stream / batch processing | Apache Spark 3.5 (PySpark, Structured Streaming) |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Storage | PostgreSQL 16 |
| Dashboard | Streamlit + Plotly |
| Containerization | Docker, Docker Compose |
| CI | GitHub Actions (lint with ruff, tests with pytest) |
| Tests | pytest, pyspark testing utilities |

## Features

- **Real-time ingestion** of 10 top crypto assets every 30 seconds.
- **Schema-validated** Kafka messages with idempotent producer.
- **Exactly-once-ish** Spark streaming sink to PostgreSQL using checkpointing and upserts.
- **Star schema** in PostgreSQL: `dim_asset`, `fact_price_tick`, `agg_price_minute`,
  `agg_price_hourly`, `agg_price_daily`.
- **Three Airflow DAGs**:
  - `crypto_batch_etl` – hourly rollup from raw ticks to hourly aggregates (cron `5 * * * *`).
  - `crypto_data_quality` – freshness and null-rate checks every 15 minutes.
  - `crypto_daily_report` – daily summary into `agg_price_daily` and Slack-friendly text report.
- **Streamlit dashboard** with live price chart, top movers, and 7-day OHLC.
- **Unit & integration tests** for the producer, Spark transformations, and DAG import sanity.
- **GitHub Actions CI** running ruff + pytest on every push.

## Quick start

```bash
# 1. Clone
git clone https://github.com/tapheret2/realtime-crypto-pipeline.git
cd realtime-crypto-pipeline

# 2. Configure environment
cp .env.example .env
# edit .env if you want to change coins / poll interval

# 3. Start the whole stack
docker compose up -d

# 4. Watch the producer publish to Kafka
docker compose logs -f producer

# 5. Open the UIs
# Airflow:    http://localhost:8080  (airflow / airflow)
# Spark UI:   http://localhost:4040
# Dashboard:  http://localhost:8501
# Postgres:   localhost:5432  (crypto / crypto / crypto)

# 6. Stop everything
docker compose down -v
```

## Data model

```
dim_asset                       fact_price_tick
─────────                       ───────────────
asset_id  PK                    tick_id        PK
symbol    UQ  ──────────────┐   asset_id       FK ──> dim_asset
name                        │   price_usd
market_cap_rank             │   market_cap_usd
                            │   volume_24h_usd
                            │   ingested_at  (UTC)
                            │   event_time   (UTC)

agg_price_minute            agg_price_hourly          agg_price_daily
────────────────            ────────────────          ───────────────
asset_id    FK              asset_id    FK            asset_id    FK
window_start                window_start              trade_date
open / high / low / close   open / high / low / close open / high / low / close
volume_usd                  volume_usd                volume_usd
tick_count                  tick_count                tick_count
                                                       price_change_pct
```

See [`docs/data-model.md`](docs/data-model.md) for the full DDL with comments.

## Running tests

```bash
# Local (no docker)
python -m venv .venv && source .venv/bin/activate
pip install -r producer/requirements.txt -r tests/requirements.txt
pytest -v
```

CI runs the same suite plus `ruff check` on every push.

## Project layout

```
realtime-crypto-pipeline/
├── docker-compose.yml          # one-command stack
├── .env.example                # configuration template
├── Makefile                    # common dev commands
├── .github/workflows/          # CI definitions
├── producer/                   # Kafka producer service
├── spark/                      # Spark streaming + batch jobs
├── airflow/                    # DAGs + plugins + Dockerfile
├── postgres/init/              # schema bootstrap SQL
├── dashboard/                  # Streamlit app
├── tests/                      # unit + integration tests
├── scripts/                    # helper shell scripts
└── docs/                       # architecture, data model, runbook
```

## Make targets

```bash
make up         # docker compose up -d
make down       # docker compose down -v
make logs       # tail every service
make test       # run pytest
make lint       # run ruff
make psql       # open psql shell
make smoke      # one-shot end-to-end smoke test
```

## Limitations / next steps

- Schema registry not yet wired up – messages are JSON, not Avro/Protobuf.
- No object store layer – raw events are not archived to S3/MinIO yet.
- Single-node Spark; for multi-node parity see `docs/runbook.md`.
- Slack/email notifications in `crypto_daily_report` are stubbed.

## License

MIT — see [LICENSE](LICENSE).

## Author

Built as a portfolio project for Data Engineering internship applications. Feedback welcome.
