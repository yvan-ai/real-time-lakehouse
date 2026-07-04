-- Table: gold.customer_metrics
-- Layer: Gold — customer lifetime value and behavioural KPIs
-- Strategy: COPY-ON-WRITE (full refresh per customer partition on batch run)
-- Partition: bucket(32, customer_id) — avoids data skew; no natural time dimension

CREATE TABLE IF NOT EXISTS iceberg.gold.customer_metrics (
    customer_id           STRING         NOT NULL COMMENT 'FK to silver.customers',
    country_code          STRING         COMMENT 'Denormalized for easy filtering',
    segment               STRING         COMMENT 'Denormalized from silver.customers',
    first_order_date      DATE           COMMENT 'Date of customer first confirmed order',
    last_order_date       DATE           COMMENT 'Date of most recent confirmed order',
    total_orders          BIGINT         COMMENT 'Lifetime confirmed order count',
    total_revenue         DECIMAL(18, 2) COMMENT 'Lifetime net revenue (USD-equivalent)',
    avg_order_value       DECIMAL(18, 2) COMMENT 'total_revenue / total_orders',
    days_since_last_order INT            COMMENT 'Recency indicator (today - last_order_date)',
    is_churned            BOOLEAN        COMMENT 'True if days_since_last_order > 90',
    computed_at           TIMESTAMP      COMMENT 'Batch run timestamp'
)
USING iceberg
PARTITIONED BY (bucket(32, customer_id))
TBLPROPERTIES (
    'write.format.default'                       = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.target-file-size-bytes'               = '67108864',
    'write.delete.mode'                          = 'copy-on-write',
    'write.update.mode'                          = 'copy-on-write',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '5',
    'history.expire.min-snapshots-to-keep'       = '3'
);
