# ADR-0005 — Per-layer Iceberg write strategies

**Status**: Accepted · **Date**: 2026-07-04

## Context

Each medallion layer has a different write pattern: Bronze receives immutable event
streams, Silver receives frequent upserts from CDC, Gold is fully recomputed per batch
run. Iceberg supports both **copy-on-write** (CoW — rewrites data files, fast reads) and
**merge-on-read** (MoR — writes delete files, fast writes, reads merge at query time).

## Decision

| Layer | Mode | Rationale |
|---|---|---|
| Bronze | append-only | Events are immutable; append is the cheapest and preserves replayability |
| Silver | merge-on-read | Frequent CDC upserts; avoid rewriting large data files per batch |
| Gold | copy-on-write | Small pre-aggregated tables read by dashboards — read latency wins |

Partitioning follows query patterns: `days()` on time columns for range scans,
`bucket(N, key)` where a high-cardinality key would otherwise skew partitions
(order items, customer metrics). Target file sizes: 128 MB (Bronze/Silver),
64 MB (Gold). Metadata auto-cleanup and snapshot expiry are set per table in the DDL.

## Consequences

- Silver reads pay a small merge cost until compaction runs — an Iceberg maintenance
  job (rewrite_data_files / expire_snapshots) is on the roadmap.
- Gold rewrites whole partitions on each run, which is fine at aggregate scale and
  keeps BI queries free of merge overhead.
- All strategies are declared in versioned DDL (`data/models/iceberg/`), not in job code.
