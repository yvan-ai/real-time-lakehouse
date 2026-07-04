"""Shared helpers for the batch Spark jobs (Bronze / Silver / Gold).

Centralises:
  - Spark session construction (Nessie catalog + MinIO S3A wiring)
  - Debezium CDC envelope parsing
  - Last-write-wins deduplication
  - Reusable business transformations (line totals, churn metrics)

All transformation helpers are pure DataFrame -> DataFrame functions so they
can be unit-tested with a plain local SparkSession (no Iceberg or Kafka jars).
"""

import logging
import os
import sys

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

WAREHOUSE = "s3a://lakehouse/warehouse"
CATALOG = "iceberg"

# Order statuses that count towards revenue metrics.
REVENUE_STATUSES = ("confirmed", "shipped", "delivered", "completed")

# A customer with no order in this many days is flagged as churned.
CHURN_THRESHOLD_DAYS = 90

# Debezium op codes that carry a row image in `after` (create / update / snapshot).
UPSERT_OPS = ("c", "u", "r")


def get_logger(name: str) -> logging.Logger:
    """Return a stdout logger configured once per process.

    Args:
        name: Logger name, usually the job module name.

    Returns:
        A configured ``logging.Logger`` writing to stdout.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def build_spark(app_name: str) -> SparkSession:
    """Build a SparkSession wired to the Nessie Iceberg catalog on MinIO.

    Reads MINIO_ACCESS_KEY / MINIO_SECRET_KEY (required) and NESSIE_URI /
    MINIO_ENDPOINT (optional, default to in-cluster service DNS) from the
    environment.

    Args:
        app_name: Spark application name shown in the UI and logs.

    Returns:
        A configured SparkSession.

    Raises:
        KeyError: If MinIO credentials are missing from the environment.
    """
    nessie_uri = os.environ.get("NESSIE_URI", "http://nessie.lakehouse.svc.cluster.local:19120/api/v1")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.lakehouse.svc.cluster.local:9000")
    minio_access = os.environ["MINIO_ACCESS_KEY"]
    minio_secret = os.environ["MINIO_SECRET_KEY"]

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", nessie_uri)
        .config(f"spark.sql.catalog.{CATALOG}.ref", "main")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Trino creates table locations with s3:// scheme; map it to S3AFileSystem
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )


def parse_debezium_envelope(raw: DataFrame) -> DataFrame:
    """Parse a Debezium JSON envelope into typed CDC columns.

    Expects a ``raw_json`` string column holding the Debezium payload
    (``schema-include=false``): ``{"before": ..., "after": ..., "op": ...,
    "ts_ms": ..., "source": {...}}``. Tombstones and empty values are dropped.

    Args:
        raw: DataFrame with a single ``raw_json`` string column.

    Returns:
        DataFrame with columns: cdc_id, op, ts_ms, source_db, source_table,
        txn_id, before, after, ingested_at.
    """
    non_empty = raw.filter(F.col("raw_json").isNotNull() & (F.col("raw_json") != ""))
    return non_empty.select(
        F.expr("uuid()").alias("cdc_id"),
        F.get_json_object("raw_json", "$.op").alias("op"),
        F.get_json_object("raw_json", "$.ts_ms").cast("bigint").alias("ts_ms"),
        F.get_json_object("raw_json", "$.source.db").alias("source_db"),
        F.get_json_object("raw_json", "$.source.table").alias("source_table"),
        F.get_json_object("raw_json", "$.source.txId").cast("bigint").alias("txn_id"),
        F.get_json_object("raw_json", "$.before").alias("before"),
        F.get_json_object("raw_json", "$.after").alias("after"),
        F.current_timestamp().alias("ingested_at"),
    )


def latest_per_key(df: DataFrame, key_col: str, order_col: str = "ts_ms") -> DataFrame:
    """Keep the latest row per key (last-write-wins CDC semantics).

    Args:
        df: Input DataFrame containing ``key_col`` and ``order_col``.
        key_col: Business key to deduplicate on (e.g. ``order_id``).
        order_col: Monotonic ordering column, highest value wins.

    Returns:
        DataFrame with exactly one row per key.
    """
    window = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def with_line_total(df: DataFrame) -> DataFrame:
    """Add ``line_total = quantity * unit_price`` as DECIMAL(18,2).

    Args:
        df: DataFrame with ``quantity`` and ``unit_price`` columns.

    Returns:
        Same DataFrame with an extra ``line_total`` column.
    """
    return df.withColumn(
        "line_total",
        (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"),
    )


def with_recency_metrics(df: DataFrame, as_of_date_col: Column) -> DataFrame:
    """Add ``days_since_last_order`` and ``is_churned`` columns.

    Args:
        df: DataFrame with a ``last_order_date`` date column.
        as_of_date_col: Column expression for the reference date (usually today).

    Returns:
        Same DataFrame with recency and churn columns appended.
    """
    return df.withColumn(
        "days_since_last_order",
        F.datediff(as_of_date_col, F.col("last_order_date")).cast("integer"),
    ).withColumn(
        "is_churned",
        F.col("days_since_last_order") > CHURN_THRESHOLD_DAYS,
    )
