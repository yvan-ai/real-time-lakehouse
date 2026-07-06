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

---
---

# Roadmap v2 — Orchestration, Transformation & Serving

Gap analysis against the "Full Stack Data Engineer Open Source" target stack
(audited 2026-07-07). Pillars 1–3 above are done; what follows closes the
remaining gaps, in recruiter-impact order. Same effort scale: **S** < 2 h ·
**M** ≈ ½ day · **L** ≈ 1 day.

| Pillar | Current state | Goal | Effort |
|---|---|---|---|
| 4. Airflow | 0 % — `orchestration` ns reserved, batch chained by shell script | DAG-driven pipeline with blocking quality gate | L |
| 5. dbt Core on Trino | 0 % — Gold layer is PySpark | SQL-first Gold models, tested, lineage-emitting | M |
| 6. ArgoCD (GitOps effectif) | 40 % — manifests + CD image bumps, no controller running | Cluster reconciled from git automatically | M |
| 7. Terraform (IaC) | 0 % — operators installed by shell scripts | Declarative bootstrap, CI-validated | M |
| 8. Second ingestion path | 0 % — Postgres CDC is the only source | External API/files → `raw.kafka_events` (suite re-enabled) | M |
| 9. Superset (BI serving) | 0 % — Streamlit demo only | Superset dashboard on Trino/Gold | M |

Recommended order: **4 → 5 → 6**, then 7–9 independently. Prerequisite for
4 and 9: reclaim the RAM/CPU wasted by the legacy namespaces
(`ingestion`/`processing`/`storage`, ~600m CPU / >1.5 Gi — cleanup in
progress). Deliberate deviations from the target stack, already argued in
ADRs: **Marquez instead of DataHub** (footprint, ADR-0008), **Trino instead
of DuckDB** (federated, already serving), **GX instead of Soda** (gate
operational since pillar 1).

---

## Pillar 4 — Apache Airflow: orchestration

### 4.1 Architecture decision — ADR-0009 — S

- Airflow over Dagster (market standard, matches the target stack); justify
  the trade-off honestly.
- RAM constraint ⇒ **standalone-style deployment** (scheduler + webserver in
  one pod, LocalExecutor + `KubernetesPodOperator`), metadata DB as a new
  `airflow` database in the **existing Postgres** (same pattern as Marquez).
  Target ≤ 1.5 Gi limits in the `orchestration` namespace (quota exists).

### 4.2 Deploy Airflow — L

- [ ] `infra/kubernetes/base/airflow/`: Deployment, Service, db-init Job
      (role + database in shared Postgres), `airflow.env` secretGenerator,
      RBAC for `KubernetesPodOperator` (create pods in `spark`/`data-quality`).
- [ ] Wire into `overlays/local` + `bootstrap.sh`; quotas already reserved.

### 4.3 The pipeline DAG — M

- [ ] `pipelines/orchestration/dags/lakehouse_batch.py`:
      `batch_bronze_silver_gold` (KubernetesPodOperator reusing the batch Job
      spec) → `quality_gate` (blocking — failed expectations fail the DAG run,
      exactly the PDF's month-5 target) → `register_lineage`.
- [ ] Daily schedule + manual trigger; `scripts/run-batch.sh` stays as the
      no-Airflow fallback.
- [ ] Airflow task failures visible in the Lakehouse Pipeline dashboard
      (kube-state-metrics already scrapes the spawned pods).

**Done when**: one `airflow dags trigger lakehouse_batch` runs
batch → gate → lineage end-to-end, and a red gate turns the DAG run red.

---

## Pillar 5 — dbt Core: SQL-first Gold layer

### 5.1 dbt project on Trino — M

- [ ] `pipelines/dbt/` project with `dbt-trino`: `gold.daily_revenue` and
      `gold.customer_metrics` as incremental/table models reading
      `iceberg.silver.*` — replaces `03_gold_aggregate.py` (Spark stays for
      Bronze/Silver CDC parsing).
- [ ] dbt tests (unique, not_null, relationships) complementing — not
      replacing — the GX gate.

### 5.2 Integration — S

- [ ] DAG task `dbt_build` between silver and the quality gate (pillar 4).
- [ ] `openlineage-dbt` emitter → the Marquez graph gains the dbt edges.
- [ ] CI: `dbt parse` + `sqlfluff` lint (no cluster needed).
- [ ] `dbt docs generate` published next to the GX Data Docs on Pages.

**Done when**: the gate passes on a Gold layer produced by dbt, and the
lineage graph shows silver → dbt → gold.

---

## Pillar 6 — ArgoCD: GitOps effectif

- [ ] Install ArgoCD **core** (no UI pod, ~512 Mi) via `bootstrap.sh`;
      apply the existing `infra/argocd/` project + application manifests.
- [ ] Auto-sync the `lakehouse-local` application on `overlays/local`
      (prune: false at first — Jobs and script-managed ConfigMaps must be
      ignored via `argocd.argoproj.io/sync-options`).
- [ ] CD workflow: replace the optional sync step with a real one
      (`ARGOCD_SERVER` secret) or rely on auto-sync polling.

**Done when**: merging an image bump to `main` changes the running pods with
no `kubectl apply` from a human.

---

## Pillar 7 — Terraform: infrastructure as code

- [ ] `infra/terraform/local/`: replace `install-strimzi.sh` /
      `install-flink-operator.sh` with `helm_release` resources + namespaces
      via the kubernetes provider (k3s stays provisioned by `setup-k3s.sh`).
- [ ] `infra/terraform/aws/` skeleton (EKS + MSK + S3) — `plan`-only,
      documented as the cloud path; no credentials in the repo.
- [ ] CI: `terraform fmt -check` + `validate` + `tflint`.
- [ ] ADR-0010 — IaC boundaries: what Terraform owns vs kustomize vs ArgoCD.

**Done when**: a fresh cluster reaches operator-ready state with
`terraform apply` instead of the two install scripts.

---

## Pillar 8 — Second ingestion path (EL) + Polars

The target stack has two ingestion lanes; only CDC exists here. Bonus: this
feeds `raw.kafka_events`, whose GX suite is currently disabled for lack of a
producer.

- [ ] ADR-0011 — lightweight EL over Airbyte: Airbyte needs ≥ 2 Gi (does not
      fit WSL2); use a **Polars-based loader** (`pipelines/ingestion/`)
      pulling a public API (e.g. exchange rates — finally fills the `currency`
      column) into the `raw-events` Kafka topic. Document Airbyte as the
      industrial alternative, optionally in `docker-compose.dev.yml`.
- [ ] Bronze job ingests `raw-events` → `raw.kafka_events`; re-enable the
      `bronze_kafka_events` validation in the bronze checkpoint.
- [ ] Declarative lineage edge in `register_lineage.py` (API → topic).

**Done when**: the gate validates a non-empty `raw.kafka_events` and the
Polars loader shows up in the Marquez graph.

---

## Pillar 9 — Superset: BI serving

- [ ] Superset in `docker-compose.dev.yml` (profile `bi`, ~1.5 Gi — Docker
      side, not the k3s budget) connected to Trino via the port-forward.
- [ ] One dashboard mirroring the Streamlit demo on `gold.daily_revenue` +
      `gold.customer_metrics`; export its JSON to `observability/superset/`.
- [ ] README: serving section + capture. In-cluster deployment only if the
      legacy cleanup frees enough RAM (quota math first).

**Done when**: a Superset dashboard renders the Gold tables through Trino.

---

## Platform hardening (folded from the README backlog)

- [ ] Prebaked Spark batch image (jars bundled) — kills the Maven/PyPI
      flakiness that bit pillars 1 and 3 — **S/M, do first**.
- [ ] Iceberg maintenance DAG (compaction + snapshot expiry) — natural
      Airflow follow-up to pillar 4 — M.
- [ ] External Secrets Operator (the commented `ExternalSecret` manifests
      already exist) — M.
- [ ] GX 1.x migration — L, keep last.

## Risks & trade-offs (v2)

| Risk | Mitigation |
|---|---|
| Airflow footprint on WSL2 | Standalone mode, shared Postgres, LocalExecutor + KubernetesPodOperator; legacy-namespace cleanup first |
| Airbyte does not fit locally | Polars/dlt loader + ADR; Airbyte in docker-compose as documented alternative |
| ArgoCD fighting script-managed resources (Jobs, GX ConfigMaps) | Sync-options ignore annotations, prune disabled initially |
| Two Gold producers during dbt migration | Cut over table by table; GX gate validates parity before removing the Spark job |
