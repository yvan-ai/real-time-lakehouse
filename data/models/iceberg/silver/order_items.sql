-- Table: silver.order_items
-- Layer: Silver — order line items parsed from raw.cdc_order_items.after JSON
-- Strategy: COPY-ON-WRITE (items are immutable once an order is placed)
-- Partition: day(order_created_at) + bucket(16, order_id)
--   bucket() distributes small orders across files; day() keeps range scans fast
-- Columns mirror the source order_items table (see infra/kubernetes/base/postgres/init.sql)
-- plus derived line_total and lakehouse metadata.

CREATE TABLE IF NOT EXISTS iceberg.silver.order_items (
    item_id          STRING         NOT NULL COMMENT 'Line item surrogate key',
    order_id         STRING         NOT NULL COMMENT 'FK to silver.orders',
    product_id       STRING         NOT NULL COMMENT 'Product catalog identifier',
    quantity         INT            COMMENT 'Units ordered',
    unit_price       DECIMAL(18, 2) COMMENT 'Price per unit at time of order',
    line_total       DECIMAL(18, 2) COMMENT 'quantity * unit_price',
    order_created_at TIMESTAMP      COMMENT 'Denormalized from silver.orders for partitioning',
    _ingested_at     TIMESTAMP      COMMENT 'Lakehouse ingestion timestamp'
)
USING iceberg
PARTITIONED BY (days(order_created_at), bucket(16, order_id))
TBLPROPERTIES (
    'write.format.default'                       = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.target-file-size-bytes'               = '134217728',
    'write.delete.mode'                          = 'copy-on-write',
    'write.update.mode'                          = 'copy-on-write',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10',
    'history.expire.min-snapshots-to-keep'       = '5'
);
