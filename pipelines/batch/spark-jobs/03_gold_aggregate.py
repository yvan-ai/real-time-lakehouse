"""Gold aggregate — Silver -> Gold pre-aggregated tables.

silver.orders + silver.order_items -> gold.daily_revenue
silver.orders + silver.customers   -> gold.customer_metrics
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_common import (
    REVENUE_STATUSES,
    UPSERT_OPS,
    build_spark,
    get_logger,
    with_recency_metrics,
)

logger = get_logger("gold_aggregate")


def revenue_orders(spark: SparkSession) -> DataFrame:
    """Return silver.orders restricted to revenue-bearing statuses."""
    return spark.table("iceberg.silver.orders").filter(
        F.col("_source_op").isin(*UPSERT_OPS) & F.col("status").isin(*REVENUE_STATUSES)
    )


def gold_daily_revenue(spark: SparkSession) -> None:
    """Aggregate daily revenue KPIs per report_date and currency."""
    logger.info("silver.orders + silver.order_items -> gold.daily_revenue")

    orders = revenue_orders(spark)
    items = spark.table("iceberg.silver.order_items")

    # Item-level revenue per order; orders without items fall back to total_amount
    order_items = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("line_total").cast("decimal(18,2)").alias("gross_revenue"),
    )

    # New customers per day: customers whose first revenue order is on report_date
    first_orders = (
        orders.groupBy("customer_id")
        .agg(F.min(F.to_date("created_at")).alias("first_order_date"))
        .groupBy("first_order_date")
        .agg(F.count("customer_id").alias("new_customers"))
        .withColumnRenamed("first_order_date", "report_date")
    )

    daily = (
        orders.join(order_items, "order_id", "left")
        .withColumn("report_date", F.to_date("created_at"))
        .withColumn("currency", F.coalesce(F.col("currency"), F.lit("USD")))
        .withColumn(
            "line_revenue",
            F.coalesce(F.col("gross_revenue"), F.col("total_amount").cast("decimal(18,2)")),
        )
        .groupBy("report_date", "currency")
        .agg(
            F.count("order_id").alias("order_count"),
            F.coalesce(F.sum("item_count"), F.lit(0)).cast("bigint").alias("item_count"),
            F.sum("line_revenue").cast("decimal(18,2)").alias("gross_revenue"),
            F.sum("line_revenue").cast("decimal(18,2)").alias("net_revenue"),
            (F.sum("line_revenue") / F.count("order_id")).cast("decimal(18,2)").alias("avg_order_value"),
        )
        .join(first_orders, "report_date", "left")
        .withColumn("new_customers", F.coalesce(F.col("new_customers"), F.lit(0)).cast("bigint"))
        .withColumn("computed_at", F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"))
        .select(
            "report_date",
            "currency",
            "order_count",
            "item_count",
            "gross_revenue",
            "net_revenue",
            "avg_order_value",
            "new_customers",
            "computed_at",
        )
    )

    daily.writeTo("iceberg.gold.daily_revenue").overwritePartitions()
    logger.info("%d date-currency rows written", daily.count())


def gold_customer_metrics(spark: SparkSession) -> None:
    """Compute lifetime value, recency and churn KPIs per customer."""
    logger.info("silver.orders + silver.customers -> gold.customer_metrics")

    orders = revenue_orders(spark)
    customers = spark.table("iceberg.silver.customers").filter(F.col("_source_op").isin(*UPSERT_OPS))

    today = F.to_date(F.lit(datetime.now(timezone.utc).date().isoformat()))

    order_stats = with_recency_metrics(
        orders.groupBy("customer_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").cast("decimal(18,2)").alias("total_revenue"),
            (F.sum("total_amount") / F.count("order_id")).cast("decimal(18,2)").alias("avg_order_value"),
            F.min(F.to_date("created_at")).alias("first_order_date"),
            F.max(F.to_date("created_at")).alias("last_order_date"),
        ),
        today,
    )

    metrics = customers.join(order_stats, "customer_id", "left").select(
        F.col("customer_id"),
        F.lit(None).cast("string").alias("country_code"),
        F.lit(None).cast("string").alias("segment"),
        F.col("first_order_date"),
        F.col("last_order_date"),
        F.coalesce(F.col("total_orders"), F.lit(0)).alias("total_orders"),
        F.coalesce(F.col("total_revenue"), F.lit(0).cast("decimal(18,2)")).alias("total_revenue"),
        F.coalesce(F.col("avg_order_value"), F.lit(0).cast("decimal(18,2)")).alias("avg_order_value"),
        F.col("days_since_last_order"),
        F.coalesce(F.col("is_churned"), F.lit(False)).alias("is_churned"),
        F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp").alias("computed_at"),
    )

    metrics.writeTo("iceberg.gold.customer_metrics").overwritePartitions()
    logger.info("%d customer rows written", metrics.count())


def main() -> None:
    logger.info("=== Gold Aggregate ===")
    spark = build_spark("gold-aggregate")
    spark.sparkContext.setLogLevel("WARN")

    gold_daily_revenue(spark)
    gold_customer_metrics(spark)

    logger.info("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
