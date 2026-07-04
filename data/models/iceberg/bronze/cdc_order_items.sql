-- Table: raw.cdc_order_items
-- Layer: Bronze — CDC stream from source order_items table via Debezium
-- Partition: day(ingested_at) — append-only, immutable after write
-- op values: c=create  u=update  d=delete  r=snapshot read

CREATE TABLE IF NOT EXISTS iceberg.raw.cdc_order_items (
    cdc_id       STRING    COMMENT 'UUID generated at ingestion',
    op           STRING    COMMENT 'Debezium op code: c / u / d / r',
    ts_ms        BIGINT    COMMENT 'Source DB commit timestamp (epoch ms)',
    source_db    STRING    COMMENT 'Source database name',
    source_table STRING    COMMENT 'Source table name',
    txn_id       BIGINT    COMMENT 'Source transaction id (nullable)',
    before       STRING    COMMENT 'JSON row snapshot before change (null for inserts)',
    after        STRING    COMMENT 'JSON row snapshot after change (null for deletes)',
    ingested_at  TIMESTAMP COMMENT 'Wall-clock time of lakehouse ingestion'
)
USING iceberg
PARTITIONED BY (days(ingested_at))
TBLPROPERTIES (
    'write.format.default'                       = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.target-file-size-bytes'               = '134217728',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10',
    'history.expire.min-snapshots-to-keep'       = '5',
    'history.expire.max-snapshot-age-ms'         = '604800000'
);
