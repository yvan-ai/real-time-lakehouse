# ADR-0004 — Log-based CDC with Debezium over batch polling

**Status**: Accepted · **Date**: 2026-07-04

## Context

The lakehouse needs every change from the operational Postgres database, including
updates and deletes, with minimal load on the source. Alternatives:

- **Batch polling** (`SELECT ... WHERE updated_at > ?`): misses deletes and intermediate
  states, requires reliable `updated_at` columns, and hammers the source on each run.
- **Triggers + outbox**: intrusive schema changes in the source database.
- **Log-based CDC (Debezium)**: reads the WAL — every commit, in order, with before/after
  images, and near-zero impact on the source.

## Decision

Use **Debezium** (Postgres connector, `pgoutput` plugin) on Kafka Connect. A dedicated
`dbz_publication` covers exactly the captured tables; a heartbeat table prevents WAL
accumulation on idle databases; failed records route to a DLQ topic (`dlq.cdc`).
The JSON envelope is kept **schema-less** (`schema-include=false`) — schemas are governed
in `data/schemas/` and `data/contracts/` instead of being repeated in every message.

## Consequences

- Deletes and every intermediate update reach the lakehouse (Bronze keeps the full
  envelope, enabling replay).
- The replication slot must be monitored: an offline connector makes the WAL grow.
- Without a schema registry, payload evolution is enforced by contract + data quality
  checks rather than broker-side validation — acceptable at this scale, revisit if
  producers multiply.
