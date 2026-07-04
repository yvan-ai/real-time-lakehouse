-- Table: raw.kafka_events
-- Layer: Bronze — raw event capture from all Kafka topics
-- Partition: day(event_ts) + topic — enables per-topic pruning on time ranges
-- Retention: snapshots expire after 7 days; keep min 5

CREATE TABLE IF NOT EXISTS iceberg.raw.kafka_events (
    event_id    STRING    COMMENT 'UUID generated at ingestion',
    topic       STRING    COMMENT 'Kafka topic name',
    partition   INT       COMMENT 'Kafka partition number',
    `offset`    BIGINT    COMMENT 'Kafka offset within partition',
    event_ts    TIMESTAMP COMMENT 'Timestamp from Kafka record headers',
    ingested_at TIMESTAMP COMMENT 'Wall-clock time of lakehouse ingestion',
    msg_key     STRING    COMMENT 'Kafka message key (nullable)',
    payload     STRING    COMMENT 'Raw JSON payload — not parsed at this layer'
)
USING iceberg
PARTITIONED BY (days(event_ts), topic)
TBLPROPERTIES (
    'write.format.default'                       = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.target-file-size-bytes'               = '134217728',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10',
    'history.expire.min-snapshots-to-keep'       = '5',
    'history.expire.max-snapshot-age-ms'         = '604800000'
);
