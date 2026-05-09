# Runbook

Day-2 operations for the realtime-crypto-pipeline.

## Standard ops

### Start the stack
```bash
make up           # docker compose up -d
make logs         # tail every service
make ps           # who is running
```

### Stop and reset
```bash
make down         # docker compose down -v  (drops volumes!)
```

### Open a SQL shell
```bash
make psql
```

### Run the smoke test
```bash
make smoke        # bash scripts/smoke.sh
```

## Common issues

### Producer crashes with `KAFKA_BOOTSTRAP_SERVERS`
Check that the kafka container reached `healthy`:
```bash
docker inspect --format='{{.State.Health.Status}}' crypto-kafka
```
If `unhealthy`, look for KRaft cluster ID drift in the logs:
```bash
docker compose logs kafka | tail -100
```
Cluster IDs are stable so `make down` (which drops volumes) is the canonical
fix.

### Spark streaming sink can't write to Postgres
Symptoms: `org.postgresql.util.PSQLException: Connection refused`.
Fix:
```bash
docker compose restart spark-stream
```
The query resumes from the last checkpoint under
`/opt/spark/checkpoints/{raw,minute}`.

### Airflow shows stale DAG imports
Edit a DAG, then:
```bash
docker compose exec airflow airflow dags reserialize
```
Or just wait — the scheduler rescans the dags folder every 30 seconds.

### Dashboard returns no data
1. Did the producer emit anything?
   ```sql
   SELECT MAX(event_time), COUNT(*) FROM fact_price_tick;
   ```
2. Is the streaming sink running?
   ```bash
   docker compose logs --tail=50 spark-stream
   ```
3. Force a refresh in the sidebar and re-check.

### CoinGecko 429 rate limits
Public CoinGecko has aggressive rate limits. Knobs:
- Increase `POLL_INTERVAL_SECONDS` in `.env` (default 30).
- Reduce `COINS` to fewer entries.
- Provide `COINGECKO_API_KEY` for the Pro tier.

The producer already retries with exponential backoff, so transient 429s heal
themselves but burn cycles.

## Backfill a missed window

```bash
docker exec crypto-spark-stream \
    spark-submit --packages org.postgresql:postgresql:42.7.3 \
    /opt/spark/jobs/batch_transformer.py hourly \
    2026-05-09T10:00:00 2026-05-09T11:00:00
```

The upsert pattern is idempotent, so re-running is safe.

## Production hardening checklist

The current stack is sized for a single-node demo. For a real deployment:

- [ ] **Multi-broker Kafka** with `replication.factor=3`, dedicated controllers.
- [ ] **External Spark cluster** (or EMR / Databricks). The jobs already use
      `--master ${SPARK_MASTER}`.
- [ ] **CeleryExecutor or KubernetesExecutor** for Airflow.
- [ ] **Connection pooling** via PgBouncer in front of Postgres.
- [ ] **Schema registry** (Confluent / Karapace) and Avro/Protobuf payloads.
- [ ] **Object store archive** (S3 / GCS / MinIO) of raw events for replay.
- [ ] **Secret management** through Vault / AWS Secrets Manager — `.env` is
      fine for local but not for production.
- [ ] **Alerting** — Slack / PagerDuty webhooks on DQ failures.
