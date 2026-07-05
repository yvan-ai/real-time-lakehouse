#!/usr/bin/env python3
"""Create all Iceberg tables in the Nessie catalog, with data on MinIO.

The catalog is named ``iceberg`` to match the Spark batch jobs and the Trino
catalog (see infra/kubernetes/base/trino/configmap.yaml). DDL files live in
data/models/iceberg/<layer>/*.sql.

Usage:
  export MINIO_ACCESS_KEY=<root-user>
  export MINIO_SECRET_KEY=<root-password>
  export MINIO_ENDPOINT=http://localhost:9000     # via kubectl port-forward
  export NESSIE_URI=http://localhost:19120/api/v1 # via kubectl port-forward
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
               org.apache.hadoop:hadoop-aws:3.3.4,\
               com.amazonaws:aws-java-sdk-bundle:1.12.262 \
               scripts/init_iceberg.py

Or simply: ./scripts/run-iceberg-init.sh (handles port-forwards and Docker).
"""

# The script runs inside apache/spark:3.5.3-python3 (Python 3.8) — keep
# annotations lazy so 3.9+ generic syntax does not break at runtime.
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("init_iceberg")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.lakehouse.svc.cluster.local:9000")
NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie.lakehouse.svc.cluster.local:19120/api/v1")
WAREHOUSE = "s3a://lakehouse/warehouse"
CATALOG = "iceberg"

SQL_DIR = Path(__file__).parent.parent / "data" / "models" / "iceberg"

LAYERS: list[tuple[str, list[str]]] = [
    ("bronze", ["kafka_events.sql", "cdc_orders.sql", "cdc_customers.sql", "cdc_order_items.sql"]),
    ("silver", ["orders.sql", "customers.sql", "order_items.sql"]),
    ("gold", ["daily_revenue.sql", "customer_metrics.sql"]),
]


def build_spark() -> SparkSession:
    """Build a SparkSession wired to the Nessie Iceberg catalog on MinIO."""
    minio_access_key = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key = os.environ["MINIO_SECRET_KEY"]

    return (
        SparkSession.builder.appName("iceberg-init")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Nessie REST catalog — same catalog the batch jobs and Trino use
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", NESSIE_URI)
        .config(f"spark.sql.catalog.{CATALOG}.ref", "main")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        # S3A / MinIO settings
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Driver memory — keep small for local k3s
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )


def create_namespaces(spark: SparkSession) -> None:
    """Create the raw / silver / gold namespaces if they do not exist."""
    for namespace in ("raw", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
        logger.info("namespace %s.%s ready", CATALOG, namespace)


def run_sql_file(spark: SparkSession, path: Path) -> None:
    """Execute one DDL file against the catalog."""
    spark.sql(path.read_text())
    logger.info("applied %s", path.relative_to(SQL_DIR.parent.parent))


def main() -> None:
    logger.info("=== Iceberg table initialisation ===")
    logger.info("Warehouse : %s", WAREHOUSE)
    logger.info("MinIO     : %s", MINIO_ENDPOINT)
    logger.info("Nessie    : %s", NESSIE_URI)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("[1/3] Creating namespaces...")
    create_namespaces(spark)

    logger.info("[2/3] Creating tables...")
    for layer, files in LAYERS:
        logger.info("[%s]", layer)
        for fname in files:
            run_sql_file(spark, SQL_DIR / layer / fname)

    logger.info("[3/3] Verification...")
    for namespace in ("raw", "silver", "gold"):
        tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{namespace}").collect()
        logger.info("%s.%s: %s", CATALOG, namespace, [row.tableName for row in tables])

    logger.info("Done. All Iceberg tables are ready.")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except KeyError as exc:
        logger.error("missing environment variable %s", exc)
        sys.exit(1)
