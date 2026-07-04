"""Unit tests for business transformations shared across Silver / Gold jobs."""

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from lakehouse_common import (
    CHURN_THRESHOLD_DAYS,
    REVENUE_STATUSES,
    with_line_total,
    with_recency_metrics,
)


def test_should_compute_line_total_as_quantity_times_unit_price(spark):
    df = spark.createDataFrame(
        [(2, Decimal("49.99")), (1, Decimal("30.01"))],
        "quantity INT, unit_price DECIMAL(18,2)",
    )

    totals = {row.quantity: row.line_total for row in with_line_total(df).collect()}

    assert totals[2] == Decimal("99.98")
    assert totals[1] == Decimal("30.01")


def test_should_keep_line_total_null_when_price_is_null(spark):
    df = spark.createDataFrame([(3, None)], "quantity INT, unit_price DECIMAL(18,2)")

    assert with_line_total(df).collect()[0].line_total is None


def test_should_flag_customer_as_churned_beyond_threshold(spark):
    as_of = F.to_date(F.lit("2026-07-01"))
    df = spark.createDataFrame(
        [
            ("c-recent", date(2026, 6, 20)),  # 11 days — active
            ("c-boundary", date(2026, 4, 2)),  # exactly 90 days — still active
            ("c-churned", date(2026, 4, 1)),  # 91 days — churned
        ],
        "customer_id STRING, last_order_date DATE",
    )

    rows = {r.customer_id: r for r in with_recency_metrics(df, as_of).collect()}

    assert rows["c-recent"].days_since_last_order == 11
    assert rows["c-recent"].is_churned is False
    assert rows["c-boundary"].days_since_last_order == CHURN_THRESHOLD_DAYS
    assert rows["c-boundary"].is_churned is False
    assert rows["c-churned"].is_churned is True


def test_should_keep_churn_flag_null_for_customers_without_orders(spark):
    as_of = F.to_date(F.lit("2026-07-01"))
    df = spark.createDataFrame([("c-no-orders", None)], "customer_id STRING, last_order_date DATE")

    row = with_recency_metrics(df, as_of).collect()[0]

    # NULL propagates — the Gold job coalesces this to False at write time
    assert row.days_since_last_order is None
    assert row.is_churned is None


def test_revenue_statuses_exclude_pending_and_cancelled():
    assert "pending" not in REVENUE_STATUSES
    assert "cancelled" not in REVENUE_STATUSES
    assert "completed" in REVENUE_STATUSES
