# ADR-0007 — Quality gate as a deployment blocker

**Status**: Accepted · **Date**: 2026-07-06

## Context

Great Expectations suites existed for all medallion layers but nothing ran them:
bad data could land in Gold and reach dashboards silently. The suites had also
drifted from the July 2026 CDC schema realignment (dropped columns, missing
`bronze_cdc_order_items`), so the first operational step was realignment, the
second making the checks impossible to skip.

Options considered for where validation runs:

- **Inside the Spark jobs** — couples validation to the transform runtime and
  re-tests via Spark instead of the serving engine users actually query.
- **CI only (pandas samples)** — validates logic, not the real lakehouse.
- **Dedicated Kubernetes Job after the batch** — validates the same tables
  Trino serves, with an observable pass/fail state.

## Decision

A `quality-gate` Kubernetes Job (namespace `data-quality`) runs
`runner.py --layer all` against `trino.lakehouse.svc:8080` after every batch:

1. `scripts/run-batch.sh` waits for the batch Job, then launches the gate via
   `scripts/run-quality-gate.sh` (also exposed as `make quality-gate`).
2. Any failed expectation ⇒ exit code 1 ⇒ Job `Failed` — visible in `kubectl`,
   alerted by `QualityGateFailed` (kube-state-metrics + Pushgateway `gx_*`
   metrics pushed by the runner).
3. **Blocker semantics**: while the gate is red, the affected Gold tables are
   considered unfit for consumption and no downstream promotion (dashboard
   refresh, export, demo) should happen. The alert runbook says so explicitly.
4. The GX project is mounted from ConfigMaps refreshed by the script — the gate
   always runs the suites committed in the repo, no image rebuild needed.
   Limits stay ≤ 512 Mi (WSL2 budget).

## Consequences

- Data quality is now an operational state (green/red), not documentation.
- The gate adds ~2–3 min after each batch (pip install at runtime — acceptable
  locally; a prebaked image is already on the roadmap alongside the batch one).
- Suites must evolve with the schemas: a legitimate schema change turns the
  gate red until the suite is updated — that friction is the point.
- GX stays pinned to 0.18 (legacy API); the 1.x migration is a separate item.
