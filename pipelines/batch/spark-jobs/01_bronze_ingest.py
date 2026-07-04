"""Bronze ingest — Kafka CDC topics -> Iceberg raw tables.

Reads a bounded snapshot (earliest -> latest at job start) from:
  debezium.public.orders      -> iceberg.raw.cdc_orders
  debezium.public.customers   -> iceberg.raw.cdc_customers
  debezium.public.order_items -> iceberg.raw.cdc_order_items

Each row preserves the full Debezium envelope: op, ts_ms, before/after JSON strings.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from lakehouse_common import build_spark, get_logger, parse_debezium_envelope

logger = get_logger("bronze_ingest")

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    "kafka-dev-kafka-bootstrap.streaming.svc.cluster.local:9092",
)

TOPICS = {
    "debezium.public.orders": "iceberg.raw.cdc_orders",
    "debezium.public.customers": "iceberg.raw.cdc_customers",
    "debezium.public.order_items": "iceberg.raw.cdc_order_items",
}


def read_topic(spark: SparkSession, topic: str) -> DataFrame:
    """Read a bounded snapshot of a Kafka topic as raw JSON strings.

    Args:
        spark: Active SparkSession.
        topic: Kafka topic to read (earliest to latest offsets at job start).

    Returns:
        DataFrame with a single ``raw_json`` string column.
    """
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast(StringType()).alias("raw_json"))
    )


def ingest_topic(spark: SparkSession, topic: str, target_table: str) -> int:
    """Append the parsed CDC envelope of one topic to its Bronze table.

    Args:
        spark: Active SparkSession.
        topic: Source Kafka topic.
        target_table: Fully qualified Iceberg table (catalog.namespace.table).

    Returns:
        Number of records written.
    """
    logger.info("%s -> %s", topic, target_table)
    parsed = parse_debezium_envelope(read_topic(spark, topic))

    count = parsed.count()
    if count == 0:
        logger.info("no records — skipping")
        return 0

    parsed.writeTo(target_table).append()
    logger.info("wrote %d records", count)
    return count


def main() -> None:
    logger.info("=== Bronze Ingest ===")
    spark = build_spark("bronze-ingest")
    spark.sparkContext.setLogLevel("WARN")

    total = 0
    for topic, table in TOPICS.items():
        total += ingest_topic(spark, topic, table)

    logger.info("Done. Total records ingested: %d", total)
    spark.stop()


if __name__ == "__main__":
    main()
