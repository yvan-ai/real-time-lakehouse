# ADR-0011 — Lightweight EL loader instead of Airbyte

**Status**: Accepted · **Date**: 2026-07-07

## Context

The target stack has two ingestion lanes; this platform only had CDC
(Postgres → Debezium → Kafka). Consequences: the `raw.kafka_events` Bronze
table had no producer, its GX suite was disabled, and the `currency` columns
carried a hardcoded fallback instead of data.

The industry-standard answer is **Airbyte**, but its self-hosted footprint
is ≥ 2 Gi across several pods (server, worker, temporal, db, webapp) — it
does not fit the WSL2 cluster (~2 Gi headroom after the legacy cleanup), and
even the Docker Compose variant would dwarf the rest of the dev stack.

Alternatives considered:

- **Airbyte** — rejected locally for footprint; documented here as the
  industrial choice when RAM is not the constraint.
- **dlt** — good lightweight ELT library, but it wants to own the load path
  (destinations), while this platform's contract is "everything lands in
  Kafka first".
- **Plain Python + Polars** — a ~150-line loader with real DataFrame
  reshaping, producing to the existing `raw.events` topic; zero new
  infrastructure.

## Decision

Ship `pipelines/ingestion/exchange_rates_loader.py`:

1. **Source**: Frankfurter (ECB reference rates — free, keyless, stable).
2. **Transform**: Polars — unpivoted rates, currency filter, typed columns;
   the pure `rates_to_events()` function is unit-tested without network.
3. **Sink**: one JSON event per currency to `raw.events` (topic CR already
   existed), keyed by currency.
4. **Landing**: `01_bronze_ingest.py` gains a raw-events pass that captures
   the full Kafka record (topic/partition/offset/timestamp/key/payload)
   into `iceberg.raw.kafka_events`; the `bronze_kafka_events` suite is
   re-enabled in the bronze checkpoint.
5. **Lineage**: declarative edge API → `raw.events` in
   `scripts/register_lineage.py` (the loader has no OpenLineage emitter),
   Spark emits the Kafka → Iceberg edge automatically.

## Consequences

- The quality gate now validates a non-empty `raw.kafka_events` — run the
  loader before the batch (demo flow), or the row-count expectation fails
  by design (an empty second lane is a broken second lane).
- Exchange rates give the `currency` dimension real data to join against
  in future Gold models (e.g. EUR-normalised revenue).
- Airbyte remains the documented industrial alternative: on a cluster with
  RAM to spare, replace the loader with an Airbyte connection targeting the
  same `raw.events` topic and delete nothing else.
