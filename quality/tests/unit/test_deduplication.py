"""Unit tests for last-write-wins CDC deduplication (Silver layer)."""

from lakehouse_common import latest_per_key


def _orders_df(spark, rows):
    return spark.createDataFrame(rows, ["order_id", "status", "ts_ms"])


def test_should_keep_only_latest_version_per_key(spark):
    df = _orders_df(
        spark,
        [
            ("o-1", "pending", 1000),
            ("o-1", "confirmed", 2000),
            ("o-1", "shipped", 3000),
            ("o-2", "pending", 1500),
        ],
    )

    result = {row.order_id: row.status for row in latest_per_key(df, "order_id").collect()}

    assert result == {"o-1": "shipped", "o-2": "pending"}


def test_should_handle_out_of_order_arrival(spark):
    # Late-arriving older event must not overwrite the newer state
    df = _orders_df(
        spark,
        [
            ("o-1", "confirmed", 2000),
            ("o-1", "pending", 1000),
        ],
    )

    result = latest_per_key(df, "order_id").collect()

    assert len(result) == 1
    assert result[0].status == "confirmed"


def test_should_return_one_row_per_key_when_no_duplicates(spark):
    df = _orders_df(spark, [("o-1", "pending", 1000), ("o-2", "pending", 1100)])

    assert latest_per_key(df, "order_id").count() == 2


def test_should_dedup_on_custom_order_column(spark):
    df = spark.createDataFrame(
        [("c-1", "v1", 1), ("c-1", "v2", 2)],
        ["customer_id", "value", "version"],
    )

    result = latest_per_key(df, "customer_id", order_col="version").collect()

    assert len(result) == 1
    assert result[0].value == "v2"
