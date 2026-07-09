#!/usr/bin/env python3
"""Exchange-rates loader — the platform's second ingestion lane (ADR-0011).

EL, not CDC: pulls the latest reference rates from the free Frankfurter API
(ECB data, no key required), reshapes them with Polars and produces one JSON
event per currency to the ``raw.events`` Kafka topic. The Bronze batch job
then lands them in ``iceberg.raw.kafka_events`` — finally giving the
``currency`` columns a real source and the ``bronze_kafka_events`` GX suite
something to validate.

Usage:
    # Docker Compose dev stack (kafka on localhost:29092)
    python3 pipelines/ingestion/exchange_rates_loader.py

    # k3s: port-forward the broker first
    kubectl port-forward svc/kafka-dev-kafka-bootstrap 9092:9092 -n streaming &
    KAFKA_BOOTSTRAP=localhost:9092 python3 pipelines/ingestion/exchange_rates_loader.py

    # Inspect without producing
    python3 pipelines/ingestion/exchange_rates_loader.py --dry-run

Dependencies: pipelines/ingestion/requirements.txt (polars + kafka-python).
Lineage: the API → raw.events edge is registered declaratively by
scripts/register_lineage.py (this process has no OpenLineage emitter).
"""

import argparse
import json
import os
import sys
import urllib.request
from datetime import UTC, datetime

import polars as pl

API_URL = "https://api.frankfurter.dev/v1/latest"
DEFAULT_TOPIC = "raw.events"
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")

# Currencies the demo shop actually bills in — keep the topic signal-dense.
DEFAULT_CURRENCIES = ("USD", "GBP", "CHF", "JPY", "CAD")


def fetch_rates(base: str) -> dict:
    """Fetch the latest reference rates for one base currency.

    Args:
        base: ISO 4217 base currency code (e.g. ``EUR``).

    Returns:
        Decoded API payload: ``{"base": ..., "date": ..., "rates": {...}}``.
    """
    # Frankfurter (Cloudflare) rejects urllib's default User-Agent with a 403;
    # any identifying UA is accepted — verified live on 2026-07-09.
    request = urllib.request.Request(
        f"{API_URL}?base={base}",
        headers={"User-Agent": "real-time-lakehouse-loader/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def rates_to_events(payload: dict, currencies: tuple[str, ...] = DEFAULT_CURRENCIES) -> pl.DataFrame:
    """Reshape an API payload into one event row per quoted currency.

    Pure Polars transformation — unit-tested without network or Kafka.

    Args:
        payload: Frankfurter response (``base``, ``date``, ``rates`` mapping).
        currencies: Quoted currencies to keep; empty tuple keeps all.

    Returns:
        DataFrame with columns: event_type, base, currency, rate, rate_date,
        fetched_at — sorted by currency.
    """
    rates = pl.DataFrame({"currency": list(payload["rates"].keys()), "rate": list(payload["rates"].values())})
    if currencies:
        rates = rates.filter(pl.col("currency").is_in(list(currencies)))

    return (
        rates.with_columns(
            pl.lit("exchange_rate").alias("event_type"),
            pl.lit(payload["base"]).alias("base"),
            pl.col("rate").cast(pl.Float64).round(6),
            pl.lit(payload["date"]).alias("rate_date"),
            pl.lit(datetime.now(UTC).isoformat()).alias("fetched_at"),
        )
        .select("event_type", "base", "currency", "rate", "rate_date", "fetched_at")
        .sort("currency")
    )


def produce_events(events: pl.DataFrame, bootstrap: str, topic: str) -> int:
    """Produce one JSON message per row, keyed by currency.

    Args:
        events: Output of :func:`rates_to_events`.
        bootstrap: Kafka bootstrap servers.
        topic: Destination topic (``raw.events``).

    Returns:
        Number of messages acknowledged.
    """
    # Imported here so the pure transformation stays testable without kafka-python.
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        acks="all",
    )
    try:
        for row in events.to_dicts():
            producer.send(topic, key=row["currency"], value=row)
        producer.flush(timeout=30)
    finally:
        producer.close()
    return events.height


def main() -> int:
    parser = argparse.ArgumentParser(description="Load exchange rates into the raw.events topic")
    parser.add_argument("--base", default="EUR", help="Base currency (default: EUR)")
    parser.add_argument(
        "--currencies",
        default=",".join(DEFAULT_CURRENCIES),
        help="Comma-separated quoted currencies to keep; empty keeps all",
    )
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Destination topic")
    parser.add_argument("--dry-run", action="store_true", help="Print events instead of producing")
    args = parser.parse_args()

    currencies = tuple(c.strip().upper() for c in args.currencies.split(",") if c.strip())

    print(f"Fetching {args.base}-based rates from {API_URL}...")
    events = rates_to_events(fetch_rates(args.base), currencies)

    if events.is_empty():
        print("No matching currencies in the API response.", file=sys.stderr)
        return 1

    if args.dry_run:
        print(events)
        return 0

    count = produce_events(events, args.bootstrap, args.topic)
    print(f"✓ {count} exchange-rate events produced to {args.topic} ({args.bootstrap})")
    print("Next: ./scripts/run-batch.sh lands them in iceberg.raw.kafka_events")
    return 0


if __name__ == "__main__":
    sys.exit(main())
