# ADR-0008 — Data lineage with OpenLineage and Marquez

**Status**: Accepted · **Date**: 2026-07-06

## Context

The platform had no lineage: nothing answered "where does `gold.daily_revenue`
come from?" or "what breaks downstream if `public.orders` changes?". The
standard vocabulary for this is **OpenLineage** (run/job/dataset events emitted
by the processing engines themselves).

Constraints:

- **RAM**: ~2.3 Gi headroom on the WSL2 cluster — heavyweight catalogs are out.
- **Emitters**: Spark has a mature OpenLineage integration; Debezium has none;
  PyFlink (Table API) support is still limited upstream.

Alternatives considered:

- **DataHub** — much richer catalog, but multi-gigabyte footprint
  (Elasticsearch + Kafka + MySQL): does not fit the cluster.
- **Static docs only** (mermaid in the README) — no API, no graph exploration,
  goes stale immediately.
- **Marquez** — the OpenLineage reference implementation: HTTP API + graph UI,
  runs in one pod, stores in Postgres.

## Decision

Deploy **Marquez** as the lineage backend, sized for the local cluster:

1. **Shared Postgres** — Marquez uses a dedicated `marquez` database and role
   in the existing `postgres` instance (namespace `streaming`) instead of its
   own Postgres pod. Credentials come from `marquez.env` (gitignored, generated
   by `bootstrap.sh`) via the overlay `secretGenerator`.
2. **One pod, tight limits** — API (Java, 384 Mi) + web UI (128 Mi) as two
   containers in a single Deployment in the `lineage` namespace (~512 Mi total,
   quota-enforced).
3. **Spark: automatic lineage** — the batch Job loads
   `io.openlineage:openlineage-spark` and posts events to Marquez
   (`spark.openlineage.*` confs, namespace `lakehouse`): the Bronze→Silver→Gold
   graph, Kafka source included, is emitted on every run with schema facets.
4. **Debezium & Flink: declarative lineage** — honest workaround for the
   missing emitters. `scripts/register_lineage.py` posts static OpenLineage
   events describing Postgres → `debezium.public.*` topics (Debezium) and
   `debezium.public.orders` → `gold.order-revenue-1m` (Flink job). These edges
   are only as accurate as the script; they must be updated when connector or
   job topology changes. Revisit when upstream emitters land.

## Consequences

- The full graph Postgres → Kafka → {Flink, Spark} → Iceberg is browsable in
  Marquez (`kubectl port-forward svc/marquez-web 3000:3000 -n lineage`).
- One more `--packages` download in the batch Job (mitigated by the planned
  prebaked image).
- The shared Postgres becomes a shared failure domain: if it goes down, both
  CDC source and lineage are gone — acceptable locally, called out for prod.
- Declarative edges can drift from reality; the register script is the single
  place to keep them truthful.
