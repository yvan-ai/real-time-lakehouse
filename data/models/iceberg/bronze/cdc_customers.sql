-- Table: raw.cdc_customers
-- Layer: Bronze — CDC stream from source customers table via Debezium
-- Partition: day(ingested_at) — append-only

CREATE TABLE IF NOT EXISTS iceberg.raw.cdc_customers (
    cdc_id       STRING    COMMENT 'UUID generated at ingestion',
    op           STRING    COMMENT 'Debezium op code: c / u / d / r',
    ts_ms        BIGINT    COMMENT 'Source DB commit timestamp (epoch ms)',
    source_db    STRING    COMMENT 'Source database name',
    source_table STRING    COMMENT 'Source table name',
    txn_id       BIGINT    COMMENT 'Source transaction id (nullable)',
    before       STRING    COMMENT 'JSON row snapshot before change',
    after        STRING    COMMENT 'JSON row snapshot after change',
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
