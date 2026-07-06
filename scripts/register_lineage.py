#!/usr/bin/env python3
"""Register declarative lineage for the pipeline stages without a native emitter.

Spark emits its own OpenLineage events (openlineage-spark listener in the batch
Job), but two upstream stages cannot (ADR-0008):

  - Debezium (Postgres → debezium.public.* Kafka topics) has no OpenLineage
    integration.
  - The PyFlink Table API job (debezium.public.orders → gold.order-revenue-1m)
    is not covered by the Flink OpenLineage agent yet.

This script posts static START+COMPLETE run events describing those edges to
the Marquez HTTP API, so the full graph Postgres → Kafka → {Flink, Spark} →
Iceberg is browsable. Keep the DATASETS/JOBS definitions in sync with
pipelines/streaming/ — these edges are only as truthful as this file.

Usage:
    kubectl port-forward svc/marquez 5000:5000 -n lineage &
    python3 scripts/register_lineage.py                    # default localhost:5000
    python3 scripts/register_lineage.py --url http://marquez.lineage.svc.cluster.local:5000
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

PRODUCER = "https://github.com/yvan-ai/real-time-lakehouse/blob/main/scripts/register_lineage.py"
SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"
SCHEMA_FACET_URL = "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json"
DOC_FACET_URL = "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json"

# Dataset namespaces follow the OpenLineage naming spec (scheme://authority).
POSTGRES_NS = "postgres://postgres.streaming.svc.cluster.local:5432"
KAFKA_NS = "kafka://kafka-dev-kafka-bootstrap.streaming.svc.cluster.local:9092"

# Source table schemas — mirror infra/kubernetes/base/postgres/init.sql.
PG_TABLES: dict[str, list[tuple[str, str]]] = {
    "lakehouse.public.orders": [
        ("order_id", "bigint"),
        ("customer_id", "bigint"),
        ("status", "varchar"),
        ("total_amount", "numeric"),
        ("created_at", "timestamptz"),
        ("updated_at", "timestamptz"),
    ],
    "lakehouse.public.customers": [
        ("customer_id", "bigint"),
        ("name", "varchar"),
        ("email", "varchar"),
        ("created_at", "timestamptz"),
        ("updated_at", "timestamptz"),
    ],
    "lakehouse.public.order_items": [
        ("item_id", "bigint"),
        ("order_id", "bigint"),
        ("product_id", "bigint"),
        ("quantity", "int"),
        ("unit_price", "numeric"),
        ("created_at", "timestamptz"),
    ],
}

# CDC topics — Debezium envelope, one topic per captured table.
CDC_TOPICS = (
    "debezium.public.orders",
    "debezium.public.customers",
    "debezium.public.order_items",
)

# Flink sink schema — mirrors pipelines/streaming/flink-jobs/order-revenue/job.py.
FLINK_SINK_FIELDS: list[tuple[str, str]] = [
    ("window_start", "timestamp"),
    ("window_end", "timestamp"),
    ("order_count", "bigint"),
    ("total_revenue", "decimal(14,2)"),
    ("avg_order_value", "decimal(12,2)"),
]


def dataset(namespace: str, name: str, fields: list[tuple[str, str]] | None = None) -> dict:
    """Build an OpenLineage dataset reference with an optional schema facet."""
    ds: dict = {"namespace": namespace, "name": name}
    if fields:
        ds["facets"] = {
            "schema": {
                "_producer": PRODUCER,
                "_schemaURL": SCHEMA_FACET_URL,
                "fields": [{"name": n, "type": t} for n, t in fields],
            }
        }
    return ds


def run_events(
    namespace: str,
    job_name: str,
    description: str,
    inputs: list[dict],
    outputs: list[dict],
) -> list[dict]:
    """Build the START and COMPLETE events for one declarative job run."""
    run_id = str(uuid.uuid4())
    now = datetime.now(UTC).isoformat()
    job = {
        "namespace": namespace,
        "name": job_name,
        "facets": {
            "documentation": {
                "_producer": PRODUCER,
                "_schemaURL": DOC_FACET_URL,
                "description": description,
            }
        },
    }
    return [
        {
            "eventType": event_type,
            "eventTime": now,
            "run": {"runId": run_id},
            "job": job,
            "inputs": inputs,
            "outputs": outputs,
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
        }
        for event_type in ("START", "COMPLETE")
    ]


def build_events(namespace: str) -> list[dict]:
    """Assemble all declarative lineage events (Debezium + Flink edges)."""
    events: list[dict] = []

    # Debezium: one connector, three captured tables → three CDC topics.
    events.extend(
        run_events(
            namespace,
            "debezium-postgres-cdc",
            "Debezium PostgreSQL connector (KafkaConnector debezium-postgres): "
            "WAL logical replication → debezium.public.* topics. "
            "Declarative edge — Debezium has no OpenLineage emitter.",
            inputs=[dataset(POSTGRES_NS, name, fields) for name, fields in PG_TABLES.items()],
            outputs=[dataset(KAFKA_NS, topic) for topic in CDC_TOPICS],
        )
    )

    # Flink: 1-minute event-time revenue windows on the orders CDC topic.
    events.extend(
        run_events(
            namespace,
            "flink-order-revenue",
            "Flink order-revenue job: 1-minute tumbling event-time windows, "
            "exactly-once. Declarative edge — PyFlink Table API OpenLineage "
            "support is still limited upstream.",
            inputs=[dataset(KAFKA_NS, "debezium.public.orders")],
            outputs=[dataset(KAFKA_NS, "gold.order-revenue-1m", FLINK_SINK_FIELDS)],
        )
    )

    return events


def post_event(url: str, event: dict) -> None:
    """POST one RunEvent to the OpenLineage endpoint; raise on HTTP errors."""
    request = urllib.request.Request(
        f"{url.rstrip('/')}/api/v1/lineage",
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        # Marquez answers 201 Created; anything 2xx is fine.
        if response.status // 100 != 2:
            raise RuntimeError(f"Unexpected HTTP status {response.status}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Register declarative CDC/Flink lineage in Marquez")
    parser.add_argument(
        "--url",
        default=os.environ.get("MARQUEZ_URL", "http://localhost:5000"),
        help="Marquez base URL (default: $MARQUEZ_URL or http://localhost:5000)",
    )
    parser.add_argument(
        "--namespace",
        default="lakehouse",
        help="OpenLineage job namespace (default: lakehouse, same as the Spark jobs)",
    )
    args = parser.parse_args()

    events = build_events(args.namespace)
    print(f"Posting {len(events)} lineage events to {args.url} (namespace: {args.namespace})")

    for event in events:
        label = f"{event['job']['name']} [{event['eventType']}]"
        try:
            post_event(args.url, event)
            print(f"  ✓ {label}")
        except (urllib.error.URLError, RuntimeError) as exc:
            print(f"  ✗ {label}: {exc}", file=sys.stderr)
            print(
                "Is Marquez reachable? Try: kubectl port-forward svc/marquez 5000:5000 -n lineage",
                file=sys.stderr,
            )
            return 1

    print("\nDone. Explore the graph:")
    print("  kubectl port-forward svc/marquez-web 3000:3000 -n lineage  →  http://localhost:3000")
    return 0


if __name__ == "__main__":
    sys.exit(main())
