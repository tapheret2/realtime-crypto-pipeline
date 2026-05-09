"""Spark Structured Streaming job: Kafka -> 1-minute aggregates -> Postgres.

Reads JSON crypto price events from the ``crypto.prices.raw`` Kafka topic,
aggregates them into per-symbol 1-minute windows, and upserts the result into
``agg_price_minute``. Raw ticks are also persisted to ``fact_price_tick`` for
the batch layer.

Submit with::

    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
        streaming_aggregator.py
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

log = logging.getLogger("streaming_aggregator")


# ---------------------------------------------------------------------------
# Schema for the JSON payload produced by `producer/crypto_producer.py`.
# Keep this in lock-step with `producer.crypto_producer.to_event`.
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", LongType(), True),
        StructField("asset_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("market_cap_usd", DoubleType(), True),
        StructField("market_cap_rank", LongType(), True),
        StructField("volume_24h_usd", DoubleType(), True),
        StructField("price_change_pct_1h", DoubleType(), True),
        StructField("price_change_pct_24h", DoubleType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)


def build_spark(app_name: str = "crypto-streaming-aggregator") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn(
            "event_time",
            F.coalesce(F.col("event_time"), F.col("ingested_at"), F.current_timestamp()),
        )
    )


def aggregate_minute(events: DataFrame) -> DataFrame:
    """Per-symbol 1-minute OHLCV-style aggregation."""
    return (
        events.withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window(F.col("event_time"), "1 minute").alias("w"),
            F.col("asset_id"),
            F.col("symbol"),
        )
        .agg(
            F.first("price_usd", ignorenulls=True).alias("open_usd"),
            F.max("price_usd").alias("high_usd"),
            F.min("price_usd").alias("low_usd"),
            F.last("price_usd", ignorenulls=True).alias("close_usd"),
            F.avg("price_usd").alias("avg_usd"),
            F.avg("volume_24h_usd").alias("avg_volume_24h_usd"),
            F.count(F.lit(1)).alias("tick_count"),
        )
        .select(
            F.col("asset_id"),
            F.col("symbol"),
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("open_usd"),
            F.col("high_usd"),
            F.col("low_usd"),
            F.col("close_usd"),
            F.col("avg_usd"),
            F.col("avg_volume_24h_usd"),
            F.col("tick_count"),
        )
    )


def jdbc_url(env: dict[str, str]) -> str:
    return (
        f"jdbc:postgresql://{env['POSTGRES_HOST']}:{env['POSTGRES_PORT']}/"
        f"{env['POSTGRES_DB']}"
    )


def jdbc_props(env: dict[str, str]) -> dict[str, str]:
    return {
        "user": env["POSTGRES_USER"],
        "password": env["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }


def write_raw_ticks(env: dict[str, str]):
    """Returns a foreachBatch sink that appends raw ticks to fact_price_tick."""

    def _sink(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        log.info("[raw] batch=%s rows=%s", batch_id, batch_df.count())
        (
            batch_df.select(
                "asset_id",
                "symbol",
                "name",
                "price_usd",
                "market_cap_usd",
                "market_cap_rank",
                "volume_24h_usd",
                "price_change_pct_1h",
                "price_change_pct_24h",
                "ingested_at",
                "event_time",
            )
            .write.mode("append")
            .jdbc(
                url=jdbc_url(env),
                table="fact_price_tick",
                properties=jdbc_props(env),
            )
        )

    return _sink


def write_minute_aggregates(env: dict[str, str]):
    """foreachBatch upsert into agg_price_minute via a temp table + MERGE."""

    def _sink(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        rows = batch_df.count()
        log.info("[minute] batch=%s windows=%s", batch_id, rows)

        staging = "stg_agg_price_minute"
        (
            batch_df.write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(
                url=jdbc_url(env),
                table=staging,
                properties=jdbc_props(env),
            )
        )

        merge_sql = f"""
            INSERT INTO agg_price_minute (
                asset_id, symbol, window_start, window_end,
                open_usd, high_usd, low_usd, close_usd,
                avg_usd, avg_volume_24h_usd, tick_count
            )
            SELECT asset_id, symbol, window_start, window_end,
                   open_usd, high_usd, low_usd, close_usd,
                   avg_usd, avg_volume_24h_usd, tick_count
            FROM {staging}
            ON CONFLICT (asset_id, window_start) DO UPDATE
            SET symbol             = EXCLUDED.symbol,
                window_end         = EXCLUDED.window_end,
                open_usd           = EXCLUDED.open_usd,
                high_usd           = EXCLUDED.high_usd,
                low_usd            = EXCLUDED.low_usd,
                close_usd          = EXCLUDED.close_usd,
                avg_usd            = EXCLUDED.avg_usd,
                avg_volume_24h_usd = EXCLUDED.avg_volume_24h_usd,
                tick_count         = EXCLUDED.tick_count,
                updated_at         = NOW();
        """
        _execute(env, merge_sql)

    return _sink


def _execute(env: dict[str, str], sql: str) -> None:
    """Run a one-off statement against Postgres using psycopg2."""
    import psycopg2  # local import keeps PySpark startup fast

    with psycopg2.connect(
        host=env["POSTGRES_HOST"],
        port=int(env["POSTGRES_PORT"]),
        dbname=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def required_env() -> dict[str, str]:
    keys = [
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC_RAW",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise SystemExit(f"missing env vars: {missing}")
    return {k: os.environ[k] for k in keys} | {
        "SPARK_CHECKPOINT_DIR": os.getenv("SPARK_CHECKPOINT_DIR", "/opt/spark/checkpoints/streaming"),
        "SPARK_LOG_LEVEL": os.getenv("SPARK_LOG_LEVEL", "WARN"),
    }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    env = required_env()

    spark = build_spark()
    spark.sparkContext.setLogLevel(env["SPARK_LOG_LEVEL"])

    events = read_kafka_stream(
        spark, env["KAFKA_BOOTSTRAP_SERVERS"], env["KAFKA_TOPIC_RAW"]
    )

    raw_query = (
        events.writeStream.outputMode("append")
        .foreachBatch(write_raw_ticks(env))
        .option("checkpointLocation", f"{env['SPARK_CHECKPOINT_DIR']}/raw")
        .trigger(processingTime="20 seconds")
        .start()
    )

    minute_aggs = aggregate_minute(events)
    minute_query = (
        minute_aggs.writeStream.outputMode("update")
        .foreachBatch(write_minute_aggregates(env))
        .option("checkpointLocation", f"{env['SPARK_CHECKPOINT_DIR']}/minute")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("streaming queries started — awaiting termination")
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
