"""Bronze ingest — Kafka topics -> Iceberg raw tables.

Reads a bounded snapshot (earliest -> latest at job start) from:
  debezium.public.orders      -> iceberg.raw.cdc_orders
  debezium.public.customers   -> iceberg.raw.cdc_customers
  debezium.public.order_items -> iceberg.raw.cdc_order_items
  raw.events                  -> iceberg.raw.kafka_events

CDC rows preserve the full Debezium envelope (op, ts_ms, before/after JSON);
raw.events rows (EL lane, ADR-0011) keep the whole Kafka record: topic,
partition, offset, timestamp, key and payload — no parsing at this layer.
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

# Generic-events lane (exchange rates loader & friends) — full record capture.
RAW_EVENTS_TOPIC = os.environ.get("RAW_EVENTS_TOPIC", "raw.events")
RAW_EVENTS_TABLE = "iceberg.raw.kafka_events"


def read_topic_records(spark: SparkSession, topic: str) -> DataFrame:
    """Read a bounded snapshot of a Kafka topic with full record metadata.

    Args:
        spark: Active SparkSession.
        topic: Kafka topic to read (earliest to latest offsets at job start).

    Returns:
        DataFrame with the native Kafka columns (key, value, topic,
        partition, offset, timestamp, ...).
    """
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def read_topic(spark: SparkSession, topic: str) -> DataFrame:
    """Read a bounded snapshot of a Kafka topic as raw JSON strings.

    Args:
        spark: Active SparkSession.
        topic: Kafka topic to read (earliest to latest offsets at job start).

    Returns:
        DataFrame with a single ``raw_json`` string column.
    """
    return read_topic_records(spark, topic).select(F.col("value").cast(StringType()).alias("raw_json"))


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


def ingest_raw_events(spark: SparkSession) -> int:
    """Append the generic-events topic to raw.kafka_events (EL lane, ADR-0011).

    Unlike the CDC path, the whole Kafka record is preserved: topic,
    partition, offset, timestamp, key and payload. No parsing here — the
    payload stays an opaque JSON string at the Bronze layer.

    Args:
        spark: Active SparkSession.

    Returns:
        Number of records written.
    """
    logger.info("%s -> %s", RAW_EVENTS_TOPIC, RAW_EVENTS_TABLE)
    events = read_topic_records(spark, RAW_EVENTS_TOPIC).select(
        F.expr("uuid()").alias("event_id"),
        F.col("topic"),
        F.col("partition").cast("int").alias("partition"),
        F.col("offset"),
        F.col("timestamp").alias("event_ts"),
        F.current_timestamp().alias("ingested_at"),
        F.col("key").cast(StringType()).alias("msg_key"),
        F.col("value").cast(StringType()).alias("payload"),
    )

    count = events.count()
    if count == 0:
        logger.info("no records — skipping")
        return 0

    events.writeTo(RAW_EVENTS_TABLE).append()
    logger.info("wrote %d records", count)
    return count


def main() -> None:
    logger.info("=== Bronze Ingest ===")
    spark = build_spark("bronze-ingest")
    spark.sparkContext.setLogLevel("WARN")

    total = 0
    for topic, table in TOPICS.items():
        total += ingest_topic(spark, topic, table)
    total += ingest_raw_events(spark)

    logger.info("Done. Total records ingested: %d", total)
    spark.stop()


if __name__ == "__main__":
    main()
