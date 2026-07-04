-- Table: silver.customers
-- Layer: Silver — deduplicated, typed customer records from raw.cdc_customers
-- Strategy: MERGE-ON-READ for efficient UPSERT from CDC
-- Partition: day(created_at) — low-update-frequency table; date range queries are common

CREATE TABLE IF NOT EXISTS iceberg.silver.customers (
    customer_id   STRING    NOT NULL COMMENT 'Business primary key',
    email         STRING    COMMENT 'Customer email (PII — apply masking in gold)',
    first_name    STRING    COMMENT 'Given name',
    last_name     STRING    COMMENT 'Family name',
    country_code  STRING    COMMENT 'ISO 3166-1 alpha-2 country code',
    segment       STRING    COMMENT 'CRM segment: consumer / business / vip',
    created_at    TIMESTAMP COMMENT 'Account creation timestamp from source',
    updated_at    TIMESTAMP COMMENT 'Last update timestamp from source',
    _source_op    STRING    COMMENT 'Last CDC op: c / u / d',
    _ingested_at  TIMESTAMP COMMENT 'Lakehouse ingestion timestamp'
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
