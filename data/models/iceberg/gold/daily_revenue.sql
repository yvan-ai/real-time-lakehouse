-- Table: gold.daily_revenue
-- Layer: Gold — pre-aggregated daily revenue metrics for dashboards / BI
-- Strategy: COPY-ON-WRITE (full partition replacement on each Spark batch run)
-- Partition: month(report_date) — low cardinality; months() keeps file count manageable

CREATE TABLE IF NOT EXISTS iceberg.gold.daily_revenue (
    report_date     DATE           NOT NULL COMMENT 'Aggregation date (UTC)',
    currency        STRING         NOT NULL COMMENT 'ISO 4217 currency code',
    order_count     BIGINT         COMMENT 'Number of confirmed+ orders on this date',
    item_count      BIGINT         COMMENT 'Total units sold',
    gross_revenue   DECIMAL(18, 2) COMMENT 'Sum of line_total before discounts',
    net_revenue     DECIMAL(18, 2) COMMENT 'Sum of line_total after discounts',
    avg_order_value DECIMAL(18, 2) COMMENT 'net_revenue / order_count',
    new_customers   BIGINT         COMMENT 'Customers whose first order was on this date',
    computed_at     TIMESTAMP      COMMENT 'Batch run timestamp that produced this row'
)
USING iceberg
PARTITIONED BY (months(report_date))
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
