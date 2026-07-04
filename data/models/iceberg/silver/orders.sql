-- Table: silver.orders
-- Layer: Silver — deduplicated, typed orders built from raw.cdc_orders
-- Strategy: MERGE-ON-READ for efficient UPSERT from CDC stream
-- Partition: day(created_at) — aligns with typical query patterns (date range scans)

CREATE TABLE IF NOT EXISTS iceberg.silver.orders (
    order_id      STRING         NOT NULL COMMENT 'Business primary key',
    customer_id   STRING         NOT NULL COMMENT 'FK to silver.customers',
    status        STRING         COMMENT 'pending / confirmed / shipped / delivered / cancelled',
    total_amount  DECIMAL(18, 2) COMMENT 'Order total in native currency',
    currency      STRING         COMMENT 'ISO 4217 currency code',
    created_at    TIMESTAMP      COMMENT 'Order creation timestamp from source',
    updated_at    TIMESTAMP      COMMENT 'Last update timestamp from source',
    _source_op    STRING         COMMENT 'Last CDC op that produced this row: c / u / d',
    _ingested_at  TIMESTAMP      COMMENT 'Lakehouse ingestion timestamp'
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'write.format.default'                       = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.target-file-size-bytes'               = '134217728',
    'write.delete.mode'                          = 'merge-on-read',
    'write.update.mode'                          = 'merge-on-read',
    'write.merge.mode'                           = 'merge-on-read',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10',
    'history.expire.min-snapshots-to-keep'       = '5'
);
