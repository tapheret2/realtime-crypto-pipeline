"""Spark batch transformer: rolls minute aggregates up to hourly + daily.

Triggered by Airflow's ``crypto_batch_etl`` DAG. Idempotent — re-running for the
same window simply replaces the rows. Operates entirely on PostgreSQL via JDBC,
so no Kafka offsets are involved.

Args (positional, all optional, default = current UTC hour rollup)::

    spark-submit batch_transformer.py [mode] [window_start_iso] [window_end_iso]

Where ``mode`` is one of ``hourly`` (default) or ``daily``.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("batch_transformer")


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def jdbc_url() -> str:
    return (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )


def jdbc_props() -> dict[str, str]:
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def read_minute(spark: SparkSession, start: datetime, end: datetime) -> DataFrame:
    pushdown = (
        f"(SELECT * FROM agg_price_minute "
        f"WHERE window_start >= TIMESTAMP '{start.isoformat()}' "
        f"AND window_start <  TIMESTAMP '{end.isoformat()}') AS m"
    )
    return spark.read.jdbc(url=jdbc_url(), table=pushdown, properties=jdbc_props())


def aggregate_hourly(minute_df: DataFrame) -> DataFrame:
    return (
        minute_df.groupBy(
            F.date_trunc("hour", F.col("window_start")).alias("window_start"),
            F.col("asset_id"),
            F.col("symbol"),
        )
        .agg(
            F.first("open_usd", ignorenulls=True).alias("open_usd"),
            F.max("high_usd").alias("high_usd"),
            F.min("low_usd").alias("low_usd"),
            F.last("close_usd", ignorenulls=True).alias("close_usd"),
            F.avg("avg_usd").alias("avg_usd"),
            F.avg("avg_volume_24h_usd").alias("avg_volume_24h_usd"),
            F.sum("tick_count").alias("tick_count"),
        )
    )


def aggregate_daily(minute_df: DataFrame) -> DataFrame:
    base = (
        minute_df.groupBy(
            F.to_date("window_start").alias("trade_date"),
            F.col("asset_id"),
            F.col("symbol"),
        )
        .agg(
            F.first("open_usd", ignorenulls=True).alias("open_usd"),
            F.max("high_usd").alias("high_usd"),
            F.min("low_usd").alias("low_usd"),
            F.last("close_usd", ignorenulls=True).alias("close_usd"),
            F.avg("avg_usd").alias("avg_usd"),
            F.avg("avg_volume_24h_usd").alias("avg_volume_24h_usd"),
            F.sum("tick_count").alias("tick_count"),
        )
    )
    return base.withColumn(
        "price_change_pct",
        F.when(
            F.col("open_usd").isNotNull() & (F.col("open_usd") != 0),
            (F.col("close_usd") - F.col("open_usd")) / F.col("open_usd") * 100,
        ).otherwise(F.lit(None)),
    )


def upsert(df: DataFrame, target_table: str, conflict_cols: list[str]) -> None:
    """Stage the dataframe into a temp table, then MERGE into the target."""
    if df.rdd.isEmpty():
        log.info("nothing to upsert into %s", target_table)
        return
    staging = f"stg_{target_table}"
    df.write.mode("overwrite").option("truncate", "true").jdbc(
        url=jdbc_url(), table=staging, properties=jdbc_props()
    )

    cols = df.columns
    set_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols
    )
    sql = (
        f"INSERT INTO {target_table} ({', '.join(cols)}) "
        f"SELECT {', '.join(cols)} FROM {staging} "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause}, updated_at = NOW();"
    )
    _execute(sql)


def _execute(sql: str) -> None:
    import psycopg2

    with psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def parse_window(args: list[str]) -> tuple[str, datetime, datetime]:
    """Pick mode + [start, end) window, defaulting to the previous full hour/day."""
    mode = args[0] if len(args) > 0 else "hourly"
    if mode not in {"hourly", "daily"}:
        raise SystemExit(f"unknown mode {mode!r}; expected 'hourly' or 'daily'")

    if len(args) >= 3:
        start = datetime.fromisoformat(args[1]).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(args[2]).replace(tzinfo=timezone.utc)
        return mode, start, end

    now = datetime.now(tz=timezone.utc)
    if mode == "hourly":
        end = now.replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(hours=1)
    else:
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
    return mode, start, end


def main(argv: list[str]) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    mode, start, end = parse_window(argv[1:])
    log.info("batch_transformer mode=%s window=[%s, %s)", mode, start, end)

    spark = build_spark(f"crypto-batch-{mode}")
    minute = read_minute(spark, start, end)

    if mode == "hourly":
        result = aggregate_hourly(minute)
        upsert(result, "agg_price_hourly", ["asset_id", "window_start"])
    else:
        result = aggregate_daily(minute)
        upsert(result, "agg_price_daily", ["asset_id", "trade_date"])

    log.info("done. rows_processed=%s", result.count())
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
