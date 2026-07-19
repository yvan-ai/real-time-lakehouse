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

- [x] `infra/kubernetes/base/airflow/`: Deployment, Service, db-init Job
      (role + database in shared Postgres), `airflow.env` secretGenerator,
      RBAC for `KubernetesPodOperator` (create pods in
      `spark`/`data-quality`/`lineage`).
- [x] Wire into `overlays/local` + `bootstrap.sh`; orchestration quota
      raised to the 1.5 Gi target.

### 4.3 The pipeline DAG — M

- [x] `pipelines/orchestration/dags/lakehouse_batch.py`:
      `batch_bronze_silver` (KubernetesPodOperator on the prebaked image) →
      `dbt_build_gold` (5.2) → `quality_gate` (blocking — failed
      expectations fail the DAG run, exactly the PDF's month-5 target) →
      `register_lineage`.
- [x] Daily schedule + manual trigger; `scripts/run-batch.sh` stays as the
      no-Airflow fallback.
- [x] Airflow task failures visible in the Lakehouse Pipeline dashboard
      (failed pods are kept — `delete_succeeded_pod` — so
      kube-state-metrics exposes them).

**Done when**: one `airflow dags trigger lakehouse_batch` runs
batch → gate → lineage end-to-end, and a red gate turns the DAG run red.
*(✓ verified live 2026-07-12: full green run batch → dbt → gate → lineage;
the red-gate case fired organically — the 24 h retention on `raw.events`
had emptied the EL lane and the gate failed the DAG exactly as designed)*

---

## Pillar 5 — dbt Core: SQL-first Gold layer

### 5.1 dbt project on Trino — M

- [x] `pipelines/dbt/` project with `dbt-trino`: `gold.daily_revenue` and
      `gold.customer_metrics` as table models reading `iceberg.silver.*` —
      replaces `03_gold_aggregate.py` on the DAG path (Spark stays for
      Bronze/Silver CDC parsing and remains the Gold fallback in
      `run-batch.sh` during the cutover).
- [x] dbt tests (unique, not_null, relationships + a singular grain test)
      complementing — not replacing — the GX gate.

### 5.2 Integration — S

- [x] DAG task `dbt_build_gold` between silver and the quality gate (pillar 4).
- [x] `openlineage-dbt` emitter (`dbt-ol build`) → the Marquez graph gains
      the dbt edges.
- [x] CI: `dbt parse` + `sqlfluff` lint (no cluster needed).
- [x] `dbt docs generate --empty-catalog` published under `/dbt/` next to
      the GX Data Docs on Pages.

**Done when**: the gate passes on a Gold layer produced by dbt, and the
lineage graph shows silver → dbt → gold.

---

## Pillar 6 — ArgoCD: GitOps effectif

- [x] Install ArgoCD **core** (no UI/API pods) via
      `scripts/install-argocd.sh`, wired into `bootstrap.sh`; project +
      application manifests applied.
- [x] Auto-sync the `lakehouse-local` application on `overlays/local`
      (prune: false + selfHeal). Prerequisite solved: secretGenerators moved
      to `overlays/local/secrets/` (gitignored env files made the overlay
      unbuildable for the repo-server); Jobs and script-managed ConfigMaps
      live outside git so prune-off leaves them alone.
- [x] CD workflow: relies on auto-sync polling (core install has no API
      server for a CLI sync — the tag-bump commit is all CD needs).

**Done when**: merging an image bump to `main` changes the running pods with
no `kubectl apply` from a human.
*(✓ verified live 2026-07-12: selfHeal recreated a deleted Deployment in
~20 s, and reverted an uncommitted ConfigMap drift within minutes — DAG
changes only exist once merged, as designed)*

---

## Pillar 7 — Terraform: infrastructure as code

- [x] `infra/terraform/local/`: replaces `install-strimzi.sh` /
      `install-flink-operator.sh` with `helm_release` resources + the
      `flink-operator` namespace (k3s and `namespaces.yaml` stay with
      `setup-k3s.sh`; scripts remain the no-CLI fallback in bootstrap).
- [x] `infra/terraform/aws/` skeleton (VPC + EKS + MSK + S3) — `plan`-only,
      documented as the cloud path; no credentials in the repo.
- [x] CI: `terraform fmt -check` + `validate` + `tflint`.
- [x] ADR-0010 — IaC boundaries: what Terraform owns vs kustomize vs ArgoCD
      vs scripts.

**Done when**: a fresh cluster reaches operator-ready state with
`terraform apply` instead of the two install scripts.

---

## Pillar 8 — Second ingestion path (EL) + Polars

The target stack has two ingestion lanes; only CDC exists here. Bonus: this
feeds `raw.kafka_events`, whose GX suite is currently disabled for lack of a
producer.

- [x] ADR-0011 — lightweight EL over Airbyte: Airbyte needs ≥ 2 Gi (does not
      fit WSL2); a **Polars-based loader** (`pipelines/ingestion/`) pulls
      Frankfurter exchange rates (finally fills the `currency` column) into
      the `raw.events` Kafka topic. Airbyte documented as the industrial
      alternative.
- [x] Bronze job ingests `raw.events` → `raw.kafka_events` (full-record
      capture); `bronze_kafka_events` validation re-enabled in the bronze
      checkpoint. The pure Polars transform is unit-tested.
- [x] Declarative lineage edge in `register_lineage.py` (API → topic).

**Done when**: the gate validates a non-empty `raw.kafka_events` and the
Polars loader shows up in the Marquez graph.
*(✓ verified live 2026-07-12: gate 96/96 with 5 FX events landed in Bronze;
`exchange-rates-loader` and the per-model dbt jobs visible in Marquez)*

---

## Pillar 9 — Superset: BI serving

- [x] Superset in `docker-compose.dev.yml` (profile `bi`, ~1.5 Gi — Docker
      side, not the k3s budget) connected to Trino via the port-forward;
      `Trino-Iceberg` connection pre-registered at boot.
- [ ] One dashboard mirroring the Streamlit demo on `gold.daily_revenue` +
      `gold.customer_metrics`; export its JSON to `observability/superset/`
      *(manual step once traffic runs — round-trip documented in
      `observability/superset/README.md`)*.
- [x] README: serving section. In-cluster deployment only if the
      legacy cleanup frees enough RAM (quota math first).

**Done when**: a Superset dashboard renders the Gold tables through Trino.

---

# Roadmap v3 — Multi-environment CD

## Pillar 10 — ArgoCD ApplicationSets: dev → staging → prod promotion

### 10.1 Architecture decision — ADR-0012 — S

- [x] Environments are **folders on `main`** (no env branches), one parameter
      file each (`infra/argocd/envs/*.yaml`), consumed by an ApplicationSet
      **git file generator**; per-env sync policy via `templatePatch`.
- [x] **Asymmetric materialisation** for the 16 GB node: dev = full platform
      (overlays/dev wraps local); staging/prod = verification slice — own
      GitOps-owned namespace/quota (zero steady-state pods) + PostSync smoke
      Job on the *promoted* spark-batch image, read-only against Trino.

### 10.2 Control plane — M

- [x] App-of-apps: `bootstrap/root.yaml` (only hand-applied object) reconciles
      `control-plane/` (AppProject + `lakehouse-envs` ApplicationSet), which
      generates `lakehouse-dev/-staging/-prod`; `lakehouse-local/-cloud`
      retired (dev adopted the 80 platform resources with no downtime).
- [x] ApplicationSet controller re-enabled (was scaled to 0 since pillar 6)
      with quota-sized resources; `argocd` namespace quota raised
      (640 Mi requests / 1536 Mi limits).

### 10.3 Promotion mechanics — M

- [x] `scripts/promote.sh` — a promotion IS a git commit: copies the source
      env's tag into the target overlay (`kustomize edit set image`),
      strict chain (bases → staging → prod). `Promote` workflow_dispatch
      mirrors it from the GitHub UI.
- [x] **prod gate**: no automated sync policy — the promotion commit leaves
      the app OutOfSync until `promote.sh prod --sync` patches the
      Application with a sync operation (ArgoCD core has no API server).
- [x] CI validates all four overlays + the ArgoCD control plane manifests.

### 10.4 Live verification — S

- [x] **Verified live 2026-07-19**: staging promoted `sha-01c1779 →
      sha-f41896b` by commit, auto-synced, smoke re-ran green **on the
      promoted image**; prod stayed OutOfSync (no namespace!) until the gate
      opened, then synced and its smoke passed 2/2 in 4 s.
- [x] Two defects found & fixed live: the git file generator **reserves the
      `path` parameter** (shadowed the env files' key), and **hooks are
      excluded from the sync diff** — a promotion changing only the hook
      image never turned the app OutOfSync ⇒ tracked `release-info`
      ConfigMap derived from the pinned tag via kustomize `replacements`.

**Done when**: a release moves dev → staging → prod exclusively through git
commits, each environment re-verifies on its own promoted image, and prod
never changes without a human opening the gate. *(✓ all three shown live
2026-07-19 — `kubectl get applications -n argocd` shows the three generated
apps Synced/Healthy, prod's smoke ran the staged tag.)*

---

## Platform hardening (folded from the README backlog)

- [x] Prebaked Spark batch image (jars + job scripts bundled,
      `pipelines/batch/Dockerfile`) — kills the Maven/PyPI flakiness that
      bit pillars 1 and 3; CI pushes it, CD bumps the tag.
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
