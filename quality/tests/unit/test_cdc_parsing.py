"""Unit tests for the Debezium CDC envelope parsing (Bronze layer)."""

import json

from lakehouse_common import parse_debezium_envelope

ORDER_CREATE_EVENT = json.dumps(
    {
        "before": None,
        "after": {"order_id": 1, "customer_id": 10, "status": "pending", "total_amount": 49.50},
        "op": "c",
        "ts_ms": 1720000000000,
        "source": {"db": "lakehouse", "table": "orders", "txId": 4242},
    }
)

ORDER_DELETE_EVENT = json.dumps(
    {
        "before": {"order_id": 1, "customer_id": 10, "status": "pending", "total_amount": 49.50},
        "after": None,
        "op": "d",
        "ts_ms": 1720000060000,
        "source": {"db": "lakehouse", "table": "orders", "txId": 4243},
    }
)


def _parse(spark, raw_values):
    df = spark.createDataFrame([(v,) for v in raw_values], ["raw_json"])
    return parse_debezium_envelope(df)


def test_should_extract_envelope_fields_from_create_event(spark):
    row = _parse(spark, [ORDER_CREATE_EVENT]).collect()[0]

    assert row.op == "c"
    assert row.ts_ms == 1720000000000
    assert row.source_db == "lakehouse"
    assert row.source_table == "orders"
    assert row.txn_id == 4242
    assert row.before is None
    assert json.loads(row.after)["order_id"] == 1


def test_should_keep_before_image_on_delete_event(spark):
    row = _parse(spark, [ORDER_DELETE_EVENT]).collect()[0]

    assert row.op == "d"
    assert row.after is None
    assert json.loads(row.before)["order_id"] == 1


def test_should_drop_tombstones_and_empty_values(spark):
    parsed = _parse(spark, [ORDER_CREATE_EVENT, None, ""])

    assert parsed.count() == 1


def test_should_generate_unique_cdc_ids(spark):
    parsed = _parse(spark, [ORDER_CREATE_EVENT, ORDER_DELETE_EVENT])
    cdc_ids = [row.cdc_id for row in parsed.collect()]

    assert len(set(cdc_ids)) == 2


def test_should_add_ingestion_timestamp(spark):
    row = _parse(spark, [ORDER_CREATE_EVENT]).collect()[0]

    assert row.ingested_at is not None
