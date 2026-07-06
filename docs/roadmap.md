# Roadmap — Data Quality, Observability & Lineage

Action plan for the next three platform pillars. Each pillar lists its current
state (audited 2026-07-06), the remaining work, concrete deliverables and a
definition of done. Effort scale: **S** < 2 h · **M** ≈ ½ day · **L** ≈ 1 day.

| Pillar | Current state | Goal | Effort |
|---|---|---|---|
| 1. Great Expectations | ~60 % — 8 suites, 3 checkpoints, runner | Operationalise: automated quality gate + published docs | M |
| 2. Prometheus & Grafana | ~70 % — deployments, 3 dashboards, alert rules | Consolidate: real targets, business dashboard, actionable alerts | M |
| 3. OpenLineage + Marquez | 0 % — empty placeholder directory | Build: lineage backend + Spark emitter + declarative upstream graph | L |

Recommended order: **1 → 2 → 3**. Step 2.4 depends on 1.4 (quality metrics
feed the alerting); pillar 3 has no dependency on the other two.

---

## Pillar 1 — Great Expectations: data validation

### 1.1 Realign expectation suites with the actual schemas — S *(prerequisite)*

The Silver DDLs were aligned with the CDC source in July 2026; the suites
still reference dropped columns and would fail on first run.

- [x] `quality/great-expectations/expectations/silver_customers.json`: drop
      `first_name`, `last_name`, `country_code`, `segment`; add `name`.
- [x] Audit `silver_order_items.json` against the trimmed DDL
      (`product_name`, `category`, `discount`, `currency` no longer exist).
- [x] Create the missing `bronze_cdc_order_items.json` suite (same envelope
      checks as `bronze_cdc_orders.json`) and register it in
      `quality/great-expectations/checkpoints/bronze.yml`.

**Done when**: `runner.py --layer all` passes against Trino on a freshly
batched lakehouse.

### 1.2 Automated quality gate after the batch — M

- [x] New Kubernetes Job `quality-gate` (`infra/kubernetes/base/quality/`):
      Python image + `requirements-quality.txt`, runs
      `runner.py --layer all` against `trino.lakehouse.svc:8080`.
- [x] Chain it in `scripts/run-batch.sh`: on batch success, launch the gate;
      failed expectations ⇒ Job `Failed` (visible in `kubectl`, alertable).
- [x] `make quality-gate` target.
- [x] Resource limits ≤ 512 Mi (WSL2 budget).

**Done when**: a deliberately broken expectation turns the gate red end-to-end.

### 1.3 Publish Data Docs — S

- [x] CI step generating GX Data Docs (config-only or pandas sample).
- [x] Publish to **GitHub Pages** (via `actions/deploy-pages`) — the deploy job
      is gated on the repo variable `ENABLE_PAGES` (set to `true`; flip it off
      if the repo ever goes private again, Pages needs a public repo on the
      free plan).
- [x] README: "Data Docs" badge/link next to the CI badge.

**Done when**: a public URL shows the browsable expectation results —
live at <https://yvan-ai.github.io/real-time-lakehouse/>.

### 1.4 Export quality metrics to Prometheus — S

- [x] `runner.py` writes `gx_expectations_passed/failed{layer=...}` counters
      (Pushgateway, or a textfile the gate Job exposes).
- [x] Feeds alert `QualityGateFailed` (see 2.4).

**Deliverables**: ~4 commits + **ADR-0007 — Quality gate as a deployment blocker**.

---

## Pillar 2 — Prometheus & Grafana: metrics & dashboards

### 2.1 Audit real scrape targets — S *(never verified live)*

Kafka JMX metrics only became active in July 2026 (metricsConfig placement
fix); `prometheus.yml` references `kube-state-metrics` and `node-exporter`
jobs whose deployments do not exist in `infra/kubernetes/base/monitoring/`.

- [x] Check `/targets` on the live Prometheus; list UP/DOWN.
- [x] Deploy `kube-state-metrics` (needed for Job-state alerts) and
      `node-exporter`, or remove the dead scrape jobs.

**Done when**: zero permanently-DOWN targets.

### 2.2 Missing critical exporters — M

- [x] **`postgres_exporter`** with a replication-slot query — the #1 CDC
      operational risk is silent WAL growth when the Debezium slot lags.
- [x] Enable MinIO's native Prometheus endpoint (`MINIO_PROMETHEUS_AUTH_TYPE=public`
      + scrape job).

### 2.3 "Lakehouse Pipeline" business dashboard — M

One Grafana view (provisioned ConfigMap, like the existing three) showing the
pipeline end-to-end:

- [x] CDC throughput per topic (messages/s) and Debezium connector status
- [x] Flink consumer lag (kafka-exporter) + checkpoint duration/failures
- [x] Windows emitted per minute (`gold.order-revenue-1m` producer rate)
- [x] Last successful batch Job + duration (kube-state-metrics)
- [x] Quality gate results (from 1.4)
- [x] Postgres replication slot lag (from 2.2)

### 2.4 Actionable alert rules — S

- [x] `DebeziumConnectorDown`, `ReplicationSlotLagHigh`,
      `FlinkCheckpointFailing`, `BatchJobFailed`, `QualityGateFailed` —
      each with a runbook comment in `observability/prometheus/rules/`.

### 2.5 README polish — S

- [ ] Grafana dashboard capture in `docs/img/` + README section.
      *(manual step: screenshot the "Lakehouse Pipeline" dashboard once traffic runs)*

**Deliverables**: ~4 commits, dashboard JSON + rules + captures.

---

## Pillar 3 — OpenLineage: data flow mapping

### 3.1 Architecture decision — ADR-0008 — S

- Backend: **Marquez** (OpenLineage reference implementation, graph UI).
- RAM constraint: ~2.3 Gi headroom in WSL2 ⇒ Marquez reuses the **existing
  Postgres** (dedicated `marquez` database) instead of a new Postgres pod;
  API + Web in one pod with tight limits (~512 Mi).
- Alternatives rejected: DataHub (footprint), static docs only (no graph).

### 3.2 Deploy Marquez — M

- [x] `infra/kubernetes/base/marquez/`: Deployment (marquez API+web),
      Service, DB init (database + role in the existing Postgres), quotas.
- [x] Wire into `overlays/local` + `bootstrap.sh`.

### 3.3 Automatic Spark lineage — S *(highest value/effort ratio)*

- [x] Add `io.openlineage:openlineage-spark_2.12` to the batch Job
      `--packages` + Spark confs (`spark.extraListeners`,
      `spark.openlineage.transport.url` → Marquez, namespace `lakehouse`).
- [x] Result: the Bronze→Silver→Gold graph (Kafka source included) is emitted
      automatically on every run, with schema facets.

### 3.4 Complete the upstream graph — M

- [x] Debezium has no native OpenLineage emitter ⇒ declarative registration
      script `scripts/register_lineage.py` pushing Postgres tables →
      `debezium.public.*` topics via the OpenLineage HTTP API.
- [x] Flink Table API (PyFlink) lineage support is still limited ⇒ same
      declarative approach for the `order-revenue` job
      (`debezium.public.orders` → `gold.order-revenue-1m`), documented
      honestly in the ADR.

### 3.5 End-to-end verification & docs — S

- [x] After `make demo` + a batch run, the full graph
      Postgres → Kafka → {Flink, Spark} → Iceberg is visible in Marquez
      (declarative upstream edges registered and verified via the API; the
      Spark edges appear on the next batch run).
- [ ] Capture for the README + `docs/architecture.md` update
      *(architecture.md updated; graph screenshot is a manual step)*.

**Deliverables**: ~5 commits + ADR-0008 + lineage graph capture.

---

## Risks & trade-offs

| Risk | Mitigation |
|---|---|
| RAM headroom (~2.3 Gi) too tight for Marquez | Shared Postgres, single pod, 512 Mi limit; scale Flink down during setup if needed |
| GX pinned to 0.18 (legacy API) | Stay on 0.18 for now; GX 1.x migration is a separate roadmap item |
| PyFlink OpenLineage support limited | Declarative lineage for the Flink job, revisit when upstream support lands |
| Runtime `--packages` downloads flaky (seen on Maven Central) | Prebaked batch image already on the roadmap; retry until then |
