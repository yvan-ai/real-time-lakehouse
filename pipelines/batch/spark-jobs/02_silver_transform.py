"""Silver transform — Bronze raw CDC -> Silver deduplicated, typed tables.

Reads from:
  iceberg.raw.cdc_orders      -> iceberg.silver.orders
  iceberg.raw.cdc_customers   -> iceberg.silver.customers
  iceberg.raw.cdc_order_items -> iceberg.silver.order_items

Strategy: take the latest version of each entity by ts_ms (last-write-wins).
Rows whose last op is a delete (op=d) are excluded from Silver.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_common import (
    UPSERT_OPS,
    build_spark,
    get_logger,
    latest_per_key,
    with_line_total,
)

logger = get_logger("silver_transform")


def silver_orders(spark: SparkSession) -> None:
    """Build silver.orders from the raw CDC stream (dedup on order_id)."""
    logger.info("raw.cdc_orders -> silver.orders")
    raw = spark.table("iceberg.raw.cdc_orders").filter(F.col("op").isin(*UPSERT_OPS))

    orders = raw.select(
        F.get_json_object("after", "$.order_id").cast("string").alias("order_id"),
        F.get_json_object("after", "$.customer_id").cast("string").alias("customer_id"),
        F.get_json_object("after", "$.status").alias("status"),
        F.get_json_object("after", "$.total_amount").cast("decimal(18,2)").alias("total_amount"),
        F.lit(None).cast("string").alias("currency"),
        F.to_timestamp(F.get_json_object("after", "$.created_at")).alias("created_at"),
        F.to_timestamp(F.get_json_object("after", "$.updated_at")).alias("updated_at"),
        F.col("op").alias("_source_op"),
        F.col("ingested_at").alias("_ingested_at"),
        F.col("ts_ms"),
    )

    latest = latest_per_key(orders, "order_id")

    latest.drop("ts_ms").writeTo("iceberg.silver.orders").overwritePartitions()
    logger.info("%d rows written", latest.count())


def silver_customers(spark: SparkSession) -> None:
    """Build silver.customers from the raw CDC stream (dedup on customer_id)."""
    logger.info("raw.cdc_customers -> silver.customers")
    raw = spark.table("iceberg.raw.cdc_customers").filter(F.col("op").isin(*UPSERT_OPS))

    customers = raw.select(
        F.get_json_object("after", "$.customer_id").cast("string").alias("customer_id"),
        F.get_json_object("after", "$.name").alias("name"),
        F.get_json_object("after", "$.email").alias("email"),
        F.to_timestamp(F.get_json_object("after", "$.created_at")).alias("created_at"),
        F.to_timestamp(F.get_json_object("after", "$.updated_at")).alias("updated_at"),
        F.col("op").alias("_source_op"),
        F.col("ingested_at").alias("_ingested_at"),
        F.col("ts_ms"),
    )

    latest = latest_per_key(customers, "customer_id")

    latest.drop("ts_ms").writeTo("iceberg.silver.customers").overwritePartitions()
    logger.info("%d rows written", latest.count())


def silver_order_items(spark: SparkSession) -> None:
    """Build silver.order_items, enriched with order_created_at for partitioning."""
    logger.info("raw.cdc_order_items -> silver.order_items")
    raw = spark.table("iceberg.raw.cdc_order_items").filter(F.col("op").isin(*UPSERT_OPS))

    # Join with silver.orders to get order created_at for the partition column
    orders_ref = spark.table("iceberg.silver.orders").select(
        F.col("order_id"),
        F.col("created_at").alias("order_created_at"),
    )

    items = with_line_total(
        raw.select(
            F.get_json_object("after", "$.item_id").cast("string").alias("item_id"),
            F.get_json_object("after", "$.order_id").cast("string").alias("order_id"),
            F.get_json_object("after", "$.product_id").cast("string").alias("product_id"),
            F.get_json_object("after", "$.quantity").cast("integer").alias("quantity"),
            F.get_json_object("after", "$.unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.col("ingested_at").alias("_ingested_at"),
            F.col("ts_ms"),
        )
    )

    latest = latest_per_key(items, "item_id")

    enriched = latest.join(orders_ref, "order_id", "left").drop("ts_ms")

    enriched.writeTo("iceberg.silver.order_items").overwritePartitions()
    logger.info("%d rows written", enriched.count())


def main() -> None:
    logger.info("=== Silver Transform ===")
    spark = build_spark("silver-transform")
    spark.sparkContext.setLogLevel("WARN")

    silver_orders(spark)
    silver_customers(spark)
    silver_order_items(spark)

    logger.info("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
