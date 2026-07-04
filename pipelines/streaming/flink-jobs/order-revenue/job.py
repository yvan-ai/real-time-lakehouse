"""
Order Revenue Aggregation — PyFlink Table API

Pipeline:
  debezium.public.orders  (Kafka, debezium-json, event-time)
      └─ 1-minute tumbling window
          └─ gold.order-revenue-1m  (Kafka, JSON)

Debezium CDC envelope (schema-include=false):
  { "before": {...}, "after": {...}, "op": "c|u|d|r", "ts_ms": <epoch_ms> }

The debezium-json format unwraps the envelope automatically:
  - INSERT (op=c) and SNAPSHOT (op=r) rows are emitted as inserts.
  - UPDATE (op=u) rows are emitted as upserts (retract + insert).
  - DELETE (op=d) rows are emitted as retractions.
The aggregation therefore counts net live orders per window.
"""

import os

from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "kafka-dev-kafka-bootstrap.streaming.svc.cluster.local:9092"
)
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "debezium.public.orders")
SINK_TOPIC = os.environ.get("SINK_TOPIC", "gold.order-revenue-1m")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "flink-order-revenue")


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Mirror the checkpointing config in flink-deployment.yaml so local runs
    # (spark-submit / flink run) also honour exactly-once semantics.
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    env.get_checkpoint_config().set_checkpoint_timeout(300_000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # ── Source ─────────────────────────────────────────────────────────────────
    # watermark: 5-second bounded out-of-orderness on updated_at.
    # The debezium-json format handles CDC retraction for UPDATE / DELETE rows.
    t_env.execute_sql(f"""
        CREATE TABLE orders_cdc (
            order_id      BIGINT,
            customer_id   BIGINT,
            status        STRING,
            total_amount  DECIMAL(12, 2),
            created_at    TIMESTAMP_LTZ(3),
            updated_at    TIMESTAMP_LTZ(3),
            WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = '{CONSUMER_GROUP}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'debezium-json',
            'debezium-json.schema-include' = 'false',
            'debezium-json.timestamp-format.standard' = 'ISO-8601',
            'scan.watermark.idle-timeout' = '30 s'
        )
    """)

    # ── Sink ───────────────────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE order_revenue_1m (
            window_start    TIMESTAMP_LTZ(3),
            window_end      TIMESTAMP_LTZ(3),
            order_count     BIGINT,
            total_revenue   DECIMAL(14, 2),
            avg_order_value DECIMAL(12, 2)
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format'                       = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # ── Aggregation — 1-minute tumbling window on event time ───────────────────
    # TUMBLE windows emit one result per window once the watermark passes the
    # window end, providing low-latency yet bounded output.
    t_env.execute_sql("""
        INSERT INTO order_revenue_1m
        SELECT
            TUMBLE_START(updated_at, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(updated_at,   INTERVAL '1' MINUTE) AS window_end,
            COUNT(*)                                       AS order_count,
            SUM(total_amount)                              AS total_revenue,
            AVG(total_amount)                              AS avg_order_value
        FROM orders_cdc
        GROUP BY TUMBLE(updated_at, INTERVAL '1' MINUTE)
    """)


if __name__ == "__main__":
    main()
