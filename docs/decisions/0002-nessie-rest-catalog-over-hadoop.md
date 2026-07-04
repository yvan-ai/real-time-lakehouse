# ADR-0002 — Nessie REST catalog instead of Hadoop / Hive metastore

**Status**: Accepted · **Date**: 2026-07-04

## Context

Iceberg needs a catalog to track table metadata pointers. The initial prototype used the
`hadoop` catalog type (metadata files directly on S3, no service to run). Two problems
surfaced:

1. **Trino 435 removed support for the Hadoop catalog type** — the query layer could not
   see the tables at all.
2. The Hadoop catalog relies on atomic rename semantics that object stores like MinIO/S3
   do not guarantee, risking metadata corruption under concurrent writers (Spark batch
   and future compaction jobs).

Alternatives considered: Hive Metastore (heavy: needs its own RDBMS, ~1 GB RAM we don't
have), AWS Glue (cloud-only), JDBC catalog (extra Postgres schema, no engine-agnostic
REST layer), Nessie (lightweight REST catalog, single stateless pod, native Trino and
Spark support, plus git-like branching of table state).

## Decision

Use **Project Nessie** as the Iceberg catalog for all engines. The catalog is named
`iceberg` consistently in Spark (`spark.sql.catalog.iceberg`), Trino
(`iceberg.properties`) and the DDL files, so a table is `iceberg.silver.orders`
everywhere.

## Consequences

- One more service to run, but it fits the resource budget (256–512 Mi).
- Safe concurrent commits via Nessie's optimistic locking instead of S3 renames.
- Catalog-level versioning (branches/tags) becomes available for future WAP
  (write-audit-publish) patterns.
- The `init_iceberg.py` bootstrap and all jobs share a single catalog definition —
  the earlier hadoop/Nessie split (init created tables the jobs could not see) is gone.
