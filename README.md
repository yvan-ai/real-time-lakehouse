# Real-Time Lakehouse

[![CI](https://github.com/yvan-ai/real-time-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/yvan-ai/real-time-lakehouse/actions/workflows/ci.yml)
[![Data Docs](https://img.shields.io/badge/GX-Data%20Docs-ff6310.svg)](https://yvan-ai.github.io/real-time-lakehouse/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Spark 3.5](https://img.shields.io/badge/Apache%20Spark-3.5-e25a1c.svg)](https://spark.apache.org/)
[![Flink 1.20](https://img.shields.io/badge/Apache%20Flink-1.20-d22128.svg)](https://flink.apache.org/)
[![Iceberg 1.5](https://img.shields.io/badge/Apache%20Iceberg-1.5-1c5b8e.svg)](https://iceberg.apache.org/)

A production-grade **streaming + batch lakehouse** that fits on a single 16 GB laptop:
**~60 s from a Postgres commit to a live dashboard** on the hot path, **9 Iceberg tables**
across a medallion architecture, and **96 GX expectations + 10 dbt tests** gating every
run — the whole loop **verified end to end on the running cluster** under simulated
e-commerce load (see [Results & metrics](#results--metrics)).

Postgres changes are captured by **Debezium CDC** and streamed through **Kafka**; **Flink**
aggregates them in real time (exactly-once, 1-minute event-time windows) while **Spark** and
**dbt on Trino** build the Bronze→Silver→Gold **Iceberg** layers on **MinIO**. A daily
**Airflow** DAG chains batch → dbt → a **blocking quality gate** → lineage. The 90+
Kubernetes resources are reconciled from git by **ArgoCD across three environments** —
an ApplicationSet generates `dev`/`staging`/`prod`, releases move between them as
**git-committed promotions** with post-sync smoke verification, and prod only changes
after a human opens the gate — with operators provisioned by **Terraform**.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        PG[(PostgreSQL<br/>orders, customers)]
        FX[Exchange-rates API]
    end

    subgraph Ingestion["Ingestion (namespace: streaming)"]
        DBZ[Debezium<br/>Kafka Connect]
        PL[Polars EL loader]
        K[(Kafka 4.2<br/>Strimzi / KRaft)]
        FL[Flink 1.20<br/>1-min tumbling windows<br/>exactly-once]
    end

    subgraph Transform["Transformation"]
        SP[Spark 3.5<br/>Bronze → Silver]
        DBT[dbt Core<br/>Gold models on Trino]
    end

    subgraph Lakehouse["Lakehouse (namespace: lakehouse)"]
        NS[Nessie<br/>Iceberg REST catalog]
        MINIO[(MinIO<br/>S3-compatible)]
        ICE[Iceberg tables<br/>Bronze / Silver / Gold]
        TR[Trino 435<br/>SQL engine]
    end

    subgraph Orchestration["Orchestration (namespace: orchestration)"]
        AF[Airflow DAG<br/>batch → dbt → gate → lineage]
        GX[GX quality gate<br/>blocking]
    end

    subgraph Serving
        BI[Superset / Streamlit]
        RT[Real-time consumers]
    end

    PG -- WAL --> DBZ --> K
    FX --> PL --> K
    K -- CDC topics --> FL
    FL -- revenue aggregates --> K
    K -- bounded read --> SP
    SP -- write --> ICE
    DBT -- SQL --> TR
    ICE --- NS
    ICE --- MINIO
    TR --> ICE
    AF -.-> SP
    AF -.-> DBT
    AF -.-> GX
    GX -. validates via .-> TR
    TR --> BI
    K --> RT
```

Two paths consume the same CDC stream:

- **Hot path** — Flink aggregates order revenue in 1-minute event-time tumbling windows
  (watermarks, exactly-once checkpoints) and publishes to a `gold.order-revenue-1m` topic.
- **Cold path** — Spark lands the raw CDC envelopes in Bronze and deduplicates into Silver
  (last-write-wins on `ts_ms`); dbt builds the Gold KPI tables on Trino. A daily Airflow DAG
  chains both with a blocking Great Expectations gate and lineage registration.

## Tech stack

| Layer | Technology | Role |
|---|---|---|
| CDC | Debezium 3.1 (Kafka Connect) | Streams Postgres WAL changes as events |
| Event bus | Kafka 4.2 (Strimzi operator, KRaft) | Durable, replayable transport |
| Stream processing | Flink 1.20 (PyFlink Table API) | Real-time windowed aggregation, exactly-once |
| Batch processing | Spark 3.5 (PySpark) | Medallion transformations Bronze→Silver→Gold |
| Table format | Iceberg 1.5 | ACID tables, schema evolution, time travel |
| Catalog | Nessie 0.62 | Iceberg REST catalog (no Hive metastore), RocksDB persisted on a PVC |
| Object store | MinIO | S3-compatible warehouse storage |
| Query engine | Trino 435 | Federated SQL over Iceberg |
| Transformation | dbt Core (dbt-trino) | SQL-first Gold models + tests, docs on Pages |
| Orchestration | Airflow 2.10 (standalone) | Daily DAG: batch → dbt → quality gate → lineage |
| EL ingestion | Polars loader | Exchange-rates API → `raw.events` topic (second lane) |
| Data quality | Great Expectations | Suites per layer + post-batch quality gate (K8s Job) |
| Lineage | OpenLineage + Marquez | Automatic Spark lineage, dbt emitter, declarative edges |
| Deployment | Kubernetes (k3s) + Kustomize | Declarative manifests, WSL2-friendly |
| IaC | Terraform | Operators via Helm releases (local), EKS+MSK+S3 skeleton (cloud) |
| GitOps / CD | ArgoCD core + ApplicationSet | 3 environments from git; gated promotions with smoke verification |
| CI | GitHub Actions | Lint, types, tests, dbt, Terraform, kubeconform, sha-tagged images, Data Docs |
| Observability | Prometheus + Grafana | Pipeline / Kafka / Flink dashboards & runbook'd alerts |
| BI serving | Superset (Compose profile `bi`) | Dashboards on the Gold tables through Trino |

## Key features

- **Exactly-once streaming** — Flink checkpoints (60 s interval, RocksDB state backend on S3)
  with event-time watermarks and idle-source handling.
- **Medallion architecture with intentional write strategies** — append-only Bronze,
  merge-on-read Silver (CDC upserts), copy-on-write Gold (full refresh), each with tuned
  partitioning (`days()`, `bucket()`) and file sizes.
- **Single source of truth for schemas** — Iceberg DDL in `data/models/iceberg/`, JSON Schemas
  in `data/schemas/`, and a data contract in `data/contracts/` all describe the same entities.
- **Orchestrated, gated, reconciled** — a daily Airflow DAG chains Spark, dbt and a
  blocking Great Expectations gate; ArgoCD core auto-syncs the cluster from `main`;
  Terraform owns the operators (ADR-0010 draws the IaC boundaries).
- **Multi-environment promotion, GitOps-native** — one ApplicationSet generates
  `dev`/`staging`/`prod`; a promotion is a git commit that moves the image tag one
  environment forward, a PostSync smoke Job re-verifies **on the promoted image**, and
  prod has no automated sync — a human opens the gate (ADR-0012, proven live).
- **Everything fits in 16 GB** — every pod defines requests/limits; the full stack is tuned
  for WSL2 (see [docs/architecture.md](docs/architecture.md#resource-budget)).
- **No plaintext secrets — audited and enforced** — every credential comes from gitignored
  env files rendered into Kubernetes Secrets by Kustomize (CI validates with placeholders);
  the one legacy hardcoded password found in an audit was moved to a Secret and **rotated
  live** (2026-07-19).

## Quick start

### Option A — Full stack on k3s (recommended)

```bash
# 1. Provision everything: k3s, operators (Terraform or scripts), MinIO,
#    Postgres, Kafka, Nessie, Trino, Airflow, ArgoCD, monitoring, Debezium
./scripts/bootstrap.sh

# 2. Build the prebaked Spark image and import it into k3s containerd
#    (all jars bundled — no runtime Maven downloads; the table init and the
#    batch both run on it, so flaky egress never touches the data path)
./scripts/build-batch-image.sh

# 3. Create the Iceberg tables (runs on the prebaked image, no local Spark)
./scripts/run-iceberg-init.sh

# 4. Run the batch pipeline Bronze → Silver → Gold
#    (on success the Great Expectations quality gate runs automatically)
./scripts/run-batch.sh
#    …or let Airflow drive it: batch → dbt → gate → lineage
kubectl port-forward svc/airflow 8081:8080 -n orchestration
# → http://localhost:8081 (admin / see overlays/local/secrets/airflow.env),
#   unpause the lakehouse_batch DAG — the scheduler runs it daily

# 5. Query with Trino
kubectl port-forward svc/trino 8080:8080 -n lakehouse
# → SELECT * FROM iceberg.gold.daily_revenue;

# 6. (optional) Walk the release through the environments — see "GitOps" below
./scripts/promote.sh staging        # dev → staging (auto-sync + smoke verification)
./scripts/promote.sh prod --sync    # staging → prod, through the manual gate
```

### Option B — Lightweight dev stack (no Kubernetes)

```bash
cp .env.example .env          # set your own dev credentials
make dev-up                   # Postgres + Kafka + MinIO + Nessie via Docker Compose
```

## Live demo

With the stack running, one command starts an **e-commerce traffic simulator** and a
**real-time dashboard**:

```bash
pip install -r demo/requirements.txt
make demo        # → dashboard at http://localhost:8501
```

![Real-time dashboard — Flink 1-minute revenue windows](docs/img/dashboard-hot-path.png)

- The generator ([demo/generate_traffic.py](demo/generate_traffic.py)) creates customers
  and orders, progresses statuses and cancels a few — every commit becomes a CDC event.
- The **hot path tab** streams Flink's 1-minute revenue windows straight from Kafka
  (via a Strimzi nodeport listener) — new aggregates appear ~1 minute after the orders.
- The **cold path tab** queries the Gold Iceberg tables through Trino
  (run `./scripts/run-batch.sh` to refresh them mid-demo).

### Development workflow

```bash
make setup      # install dev dependencies + pre-commit hooks
make lint       # ruff + yamllint
make typecheck  # mypy
make test       # pytest (local PySpark, no cluster needed)
make validate   # kustomize build + kubeconform
```

## Project structure

```
├── .github/workflows/        # CI (lint, tests, kubeconform, image builds) + CD (ArgoCD)
├── data/
│   ├── models/iceberg/       # Iceberg DDL — bronze / silver / gold
│   ├── schemas/              # JSON Schemas for CDC payloads
│   └── contracts/            # Data contracts (SLA, ownership, quality)
├── demo/                     # Traffic simulator + Streamlit dashboard (make demo)
├── docs/
│   ├── architecture.md       # Detailed design & resource budget
│   └── decisions/            # Architecture Decision Records (ADRs)
├── infra/
│   ├── kubernetes/           # Kustomize bases + env overlays dev/staging/prod (secrets split out)
│   ├── argocd/               # GitOps control plane: root app + ApplicationSet → 3 envs
│   └── terraform/            # local: operators via Helm · aws: EKS+MSK+S3 skeleton
├── observability/            # Prometheus rules, Grafana dashboards, Superset exports
├── pipelines/
│   ├── streaming/            # Flink job, Kafka topics, Debezium/Connect
│   ├── batch/                # Bronze/Silver/Gold PySpark jobs + prebaked image
│   ├── dbt/                  # SQL-first Gold models on Trino + dbt tests
│   ├── ingestion/            # Polars EL loader (exchange rates → raw.events)
│   └── orchestration/dags/   # Airflow lakehouse_batch DAG
├── quality/
│   ├── great-expectations/   # Expectation suites & checkpoints per layer
│   └── tests/                # Unit tests (pytest + local SparkSession)
└── scripts/                  # bootstrap, deploy, test, batch & init runners
```

## Data model (medallion)

| Layer | Namespace | Tables | Write mode | Partitioning |
|---|---|---|---|---|
| Bronze | `raw` | `kafka_events`, `cdc_orders`, `cdc_customers`, `cdc_order_items` | append-only | `days(ingested_at)` |
| Silver | `silver` | `orders`, `customers`, `order_items` | merge-on-read upsert | `days(created_at)` ± `bucket(16, order_id)` |
| Gold | `gold` | `daily_revenue`, `customer_metrics` | copy-on-write refresh | `months(report_date)` / `bucket(32, customer_id)` |

Bronze keeps the **full Debezium envelope** (`op`, `ts_ms`, `before`, `after`) so any Silver
logic can be replayed from raw history. Silver applies **last-write-wins deduplication** per
business key. Gold serves dashboards with pre-aggregated KPIs (daily revenue, customer
lifetime value, churn flags).

## Ingestion — two lanes

- **CDC (log-based)** — Debezium turns every Postgres commit into an event on the
  `debezium.public.*` topics; nothing polls the database
  ([ADR-0004](docs/decisions/0004-debezium-cdc-over-polling.md)).
- **EL (API pull)** — a **Polars** loader
  ([pipelines/ingestion/exchange_rates_loader.py](pipelines/ingestion/exchange_rates_loader.py))
  pulls ECB exchange rates from the Frankfurter API, reshapes them into one event per
  currency and produces to the `raw.events` topic. The Bronze job captures the full Kafka
  record (topic, partition, offset, timestamp, key, payload) into `raw.kafka_events`, whose
  GX suite gates the run. Airbyte is documented as the industrial alternative — it does not
  fit the 16 GB budget ([ADR-0011](docs/decisions/0011-lightweight-el-over-airbyte.md)).

```bash
python3 pipelines/ingestion/exchange_rates_loader.py --dry-run                    # inspect the events
python3 pipelines/ingestion/exchange_rates_loader.py --bootstrap localhost:32100  # produce (k3s nodeport)
```

`raw.events` keeps 24 h of history: rerun the loader before a batch if the
events have aged out — otherwise the gate fails the run on an empty lane,
which is exactly what it is there for (proven live).

## Orchestration (Airflow)

A daily `lakehouse_batch` DAG
([pipelines/orchestration/dags/lakehouse_batch.py](pipelines/orchestration/dags/lakehouse_batch.py))
drives the cold path end to end ([ADR-0009](docs/decisions/0009-airflow-orchestration.md)):

```
batch_bronze_silver (Spark) → dbt_build_gold (Trino) → quality_gate (blocking) → register_lineage
```

- **Standalone-style deployment** — scheduler + webserver in one pod (LocalExecutor),
  metadata in a dedicated `airflow` database inside the shared Postgres; the pod stays
  under 1.25 Gi.
- **Work runs elsewhere** — every task is a `KubernetesPodOperator` pod spawned in the
  namespace that owns the work (`spark`, `data-quality`, `lineage`) under its own quota,
  with namespace-scoped RBAC only.
- **The gate blocks — proven live** — one failed expectation fails the gate pod, the
  task and the DAG run (verified 2026-07-12: an empty EL lane turned the run red;
  refilling it turned the rerun green, 96/96). Failed pods are kept so
  kube-state-metrics can alert on them.
- **DAGs deploy like everything else** — shipped as a kustomize-generated ConfigMap
  (with an `.airflowignore` pruning the mount's `..data` entries — Airflow's directory
  walker loops on them otherwise); `scripts/run-batch.sh` remains the no-Airflow fallback.

```bash
kubectl port-forward svc/airflow 8081:8080 -n orchestration   # UI → http://localhost:8081
# CLI trigger — the DB conn string is built in the container command, not the
# pod env, so an exec'd CLI must recreate it:
kubectl exec -n orchestration deploy/airflow -- bash -c 'export \
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@postgres.streaming.svc.cluster.local:5432/airflow" \
  && airflow dags trigger lakehouse_batch'
```

## SQL-first Gold layer (dbt)

The Gold tables are built by **dbt Core on Trino** ([pipelines/dbt/](pipelines/dbt/)):
`gold.daily_revenue` and `gold.customer_metrics` are table models reading
`iceberg.silver.*`, replacing the Spark Gold job on the orchestrated path (Spark keeps the
Bronze/Silver CDC parsing, and stays as the Gold fallback in `run-batch.sh` during the
cutover).

- **Tested** — `unique` / `not_null` / `relationships` schema tests plus a singular
  grain test, complementing (not replacing) the GX gate.
- **Lineage-emitting** — the DAG task runs `dbt-ol build`, adding the silver → dbt → gold
  edges to the Marquez graph.
- **Documented** — `dbt docs generate` publishes the model docs on
  [Pages under `/dbt/`](https://yvan-ai.github.io/real-time-lakehouse/dbt/), next to the
  GX Data Docs; CI runs `dbt parse` + `sqlfluff` on every commit, no cluster needed.

```bash
kubectl port-forward svc/trino 8080:8080 -n lakehouse &
cd pipelines/dbt && pip install dbt-trino && dbt build --profiles-dir . --target local
```

## Technical decisions

Key choices are documented as ADRs in [docs/decisions/](docs/decisions/):

- [ADR-0002](docs/decisions/0002-nessie-rest-catalog-over-hadoop.md) — Nessie REST catalog instead of Hadoop/Hive metastore
- [ADR-0003](docs/decisions/0003-strimzi-kafka-on-k3s.md) — Strimzi-operated Kafka in KRaft mode
- [ADR-0004](docs/decisions/0004-debezium-cdc-over-polling.md) — Log-based CDC with Debezium over batch polling
- [ADR-0005](docs/decisions/0005-iceberg-write-strategies-per-layer.md) — Per-layer Iceberg write strategies
- [ADR-0006](docs/decisions/0006-gitops-deployment-with-argocd.md) — GitOps deployment with ArgoCD
- [ADR-0007](docs/decisions/0007-quality-gate-as-deployment-blocker.md) — Quality gate as a deployment blocker
- [ADR-0008](docs/decisions/0008-openlineage-marquez.md) — Data lineage with OpenLineage and Marquez
- [ADR-0009](docs/decisions/0009-airflow-orchestration.md) — Airflow (standalone-style) for orchestration
- [ADR-0010](docs/decisions/0010-iac-boundaries.md) — IaC boundaries: Terraform vs kustomize vs ArgoCD
- [ADR-0011](docs/decisions/0011-lightweight-el-over-airbyte.md) — Lightweight Polars EL loader over Airbyte
- [ADR-0012](docs/decisions/0012-multi-env-promotion-applicationsets.md) — Multi-environment promotion with ArgoCD ApplicationSets

## Testing & quality

- **Unit tests** (`quality/tests/`) — CDC envelope parsing, last-write-wins deduplication,
  revenue/churn business rules, run against a local SparkSession in CI.
- **Data quality gate** (`quality/great-expectations/`) — one expectation suite per table,
  checkpoints per layer, plus a custom cross-column churn-consistency check via Trino.
  After every batch, a `quality-gate` Kubernetes Job replays all suites against Trino:
  any failed expectation turns the Job red, pushes `gx_*` metrics to Prometheus and fires
  the `QualityGateFailed` alert ([ADR-0007](docs/decisions/0007-quality-gate-as-deployment-blocker.md)).
  Browsable suites: [Data Docs](https://yvan-ai.github.io/real-time-lakehouse/), published by CI.
- **dbt tests** (`pipelines/dbt/`) — schema tests (`unique`, `not_null`, `relationships`)
  and a singular grain test run inside `dbt build`, failing the DAG before the GX gate even
  starts.
- **Manifest validation** — every PR renders the full Kustomize overlay and validates it
  with kubeconform.
- **Static analysis** — ruff (lint + format), mypy, yamllint, sqlfluff (dbt SQL),
  `terraform fmt`/`validate`/tflint, pre-commit hooks.

## Data lineage

Marquez (namespace `lineage`) stores the OpenLineage graph
([ADR-0008](docs/decisions/0008-openlineage-marquez.md)):

- **Spark — automatic**: the batch Job loads `openlineage-spark` and emits the
  Bronze→Silver→Gold graph (Kafka source included, schema facets) on every run.
- **dbt — automatic**: the `dbt_build_gold` DAG task runs `dbt-ol build`, adding the
  silver → dbt → gold edges with model metadata.
- **Debezium, Flink & the EL loader — declarative**:
  [scripts/register_lineage.py](scripts/register_lineage.py) registers
  Postgres → `debezium.public.*` (connector),
  `debezium.public.orders` → `gold.order-revenue-1m` (Flink job) and
  Frankfurter API → `raw.events` (exchange-rates loader) via the OpenLineage API.

```bash
kubectl port-forward svc/marquez 5000:5000 -n lineage &
python3 scripts/register_lineage.py                        # upstream edges
kubectl port-forward svc/marquez-web 3000:3000 -n lineage  # → http://localhost:3000
```

## BI serving (Superset)

Superset runs in the Compose dev stack under the opt-in `bi` profile — Docker-side RAM,
not the k3s budget — with the `Trino-Iceberg` connection pre-registered against the
port-forwarded cluster Trino:

```bash
kubectl port-forward svc/trino 8080:8080 -n lakehouse --address 0.0.0.0 &
docker compose -f docker-compose.dev.yml --profile bi up -d superset
# → http://localhost:8088 (admin / $SUPERSET_ADMIN_PASSWORD)
```

Dashboards on `gold.daily_revenue` and `gold.customer_metrics` are versioned as native
exports in [observability/superset/](observability/superset/) (import/export round-trip
documented there).

## GitOps & infrastructure as code

**ArgoCD core** (controller + repo-server + ApplicationSet controller — no UI/API pods,
RAM budget) reconciles **three environments** from `main`. One hand-applied root app
(app-of-apps) syncs the control plane; the `lakehouse-envs` **ApplicationSet** reads
`infra/argocd/envs/{dev,staging,prod}.yaml` and generates one application per
environment ([ADR-0012](docs/decisions/0012-multi-env-promotion-applicationsets.md)):

```mermaid
flowchart LR
    subgraph Git["git (main)"]
        ENV["envs/*.yaml<br/>1 file = 1 environment"]
        OV["overlays/{dev,staging,prod}<br/>per-env image pins"]
    end

    ROOT[root app<br/>app-of-apps] --> APPSET[ApplicationSet<br/>lakehouse-envs]
    ENV --> APPSET
    APPSET --> DEV[lakehouse-dev<br/>full platform<br/>auto-sync]
    APPSET --> STG[lakehouse-staging<br/>verification slice<br/>auto-sync + smoke]
    APPSET --> PROD[lakehouse-prod<br/>verification slice<br/>manual gate 🔒]

    CI[CI: sha-tagged images] -->|CD bumps base tags| DEV
    DEV -->|promote.sh staging<br/>= git commit| STG
    STG -->|promote.sh prod<br/>= git commit + gate| PROD
    OV -.-> STG & PROD
```

```
merge to main ──► CI builds sha-tagged images ──► CD bumps base tags ──► dev  (auto-sync)
./scripts/promote.sh staging   # copies dev's tag → staging (auto-sync + PostSync smoke Job)
./scripts/promote.sh prod      # copies staging's tag → prod … stays OutOfSync on purpose
./scripts/promote.sh prod --sync   # the gate: a human opens it (k8s patch — no API server)
```

- **dev** materialises the full platform; **staging/prod** materialise a verification
  slice (own namespace + quota, zero steady-state pods, and a **PostSync smoke Job that
  runs on the promoted image**, read-only against the shared warehouse) — three full
  stacks don't fit in 16 GB, and the promotion mechanics don't care.
- **A promotion is a git commit** — `git log` on the overlay kustomizations is the
  release history per environment; rollback = `git revert`
  ([ADR-0006](docs/decisions/0006-gitops-deployment-with-argocd.md)).
- **Secrets live outside the tracked overlays** (`overlays/local/secrets/`, gitignored env
  files applied only by `bootstrap.sh`/`deploy.sh`) — so the repo-server can always build
  the overlays from a clean checkout.
- **Prune stays off in dev** — immutable Jobs (`batch-pipeline`, `quality-gate`,
  `*-db-init`) and script-refreshed ConfigMaps are not in git and must survive syncs.
  staging/prod namespaces are fully git-defined, so they prune.

**Terraform** owns the cluster add-ons ([ADR-0010](docs/decisions/0010-iac-boundaries.md)
draws the boundaries between Terraform, kustomize, ArgoCD and the scripts):

- [infra/terraform/local](infra/terraform/local) — Strimzi + Flink Kubernetes operator as
  `helm_release` resources (bootstrap falls back to the install scripts when the CLI is
  absent).
- [infra/terraform/aws](infra/terraform/aws) — plan-only EKS + MSK + S3 skeleton mirroring
  the local stack (k3s→EKS, Kafka→MSK, MinIO→S3); validated in CI, never applied, no
  credentials in the repo.

## Results & metrics

Verified **live on the running cluster** across two verification campaigns
(2026-07-12 and 2026-07-19) under simulated e-commerce load, on a single
16 GB WSL2 laptop:

| Metric | Value |
|---|---|
| End-to-end Airflow run (Spark → dbt → GX gate → lineage) | ✅ green (re-verified 2026-07-19 on fresh traffic) — and a **red-gate run proven**: an empty EL lane failed the whole DAG, by design |
| Data-quality checks gating each run | 96 GX expectations (9 suites) + 10 dbt tests, blocking |
| Gold served through Trino (built by dbt) | 7 days of demo load · 419 customer-metric rows, refreshed live 2026-07-19 |
| Live Flink window under load | 52 orders · 27,003.17 revenue aggregated in one 1-minute event-time window |
| Lineage graph in Marquez | 22 jobs — Spark, per-model dbt, Debezium, Flink, EL loader |
| GitOps selfHeal (ArgoCD core) | deleted Deployment recreated in ~20 s; uncommitted drift reverted in minutes |
| Multi-env promotion chain (2026-07-19) | dev → staging → prod via one ApplicationSet; promotions are git commits; PostSync smoke green **on the promoted image** in both envs; prod moved only after a human opened the gate |
| Hot-path latency (Postgres commit → dashboard) | ~60 s (1-minute event-time windows) |
| Iceberg tables (Bronze / Silver / Gold) | 9 (4 / 3 / 2), catalog surviving pod restarts (RocksDB on PVC) |
| Kubernetes resources under GitOps | 90+ across 11 quota-guarded namespaces, 3 environments + control plane |
| CI checks on every commit | 10 jobs — lint, types, unit tests, dbt, Terraform, kubeconform, images, docs |
| Architecture Decision Records | 12 |

**Battle-tested** — the two live-verification campaigns surfaced and fixed
**thirteen real defects**: a Nessie catalog silently vanishing on every pod restart,
Airflow parsing zero DAGs because of the ConfigMap mount layout, an ArgoCD controller
starved below its first sync, an egress link that truncates large downloads, an
ApplicationSet git generator silently shadowing a template parameter, ArgoCD hooks
being invisible to the sync diff (a promotion that never rolled out), a CD workflow
bumping tags no image had been built for, an OOM-looping scheduler, a hardcoded
Grafana password found in an audit and **rotated live** — and more. Each fix is an
atomic commit carrying its own post-mortem.

## Roadmap

The three pillars of [docs/roadmap.md](docs/roadmap.md) are implemented:

- [x] **Data quality**: automated Great Expectations gate after each batch + published Data Docs
- [x] **Observability**: pipeline business dashboard, postgres/MinIO exporters, actionable alerts
- [x] **Lineage**: OpenLineage + Marquez — automatic Spark lineage, declarative CDC graph

Roadmap v2 — delivered and **verified live** on the cluster, 2026-07-12 (detailed plan
in [docs/roadmap.md](docs/roadmap.md#roadmap-v2--orchestration-transformation--serving)):

- [x] **Airflow** — DAG-driven pipeline with the quality gate as a blocking task
- [x] **dbt Core on Trino** — SQL-first Gold layer, tested, emitting lineage to Marquez
- [x] **ArgoCD effectif** — core install, cluster auto-synced from git
- [x] **Terraform** — operators via Helm releases (local) + EKS/MSK/S3 skeleton (cloud path)
- [x] **Second ingestion path** — Polars loader for exchange rates → `raw.kafka_events`
- [x] **Superset** — BI serving profile on the Gold tables through Trino
- [x] Hardening: prebaked Spark batch image (jars + jobs bundled)

Roadmap v3 — multi-environment CD, delivered and **verified live** (2026-07-19,
[ADR-0012](docs/decisions/0012-multi-env-promotion-applicationsets.md)):

- [x] **ArgoCD ApplicationSets** — `lakehouse-dev/-staging/-prod` generated from one
  env file each; promotion = a git commit (`scripts/promote.sh` / Promote workflow);
  PostSync smoke verifies each env on its promoted image; prod behind a manual gate
- [ ] Next: Iceberg maintenance DAG, External Secrets Operator, GX 1.x migration,
  per-env data isolation on Nessie branches

## Contact

**Yvan Kenne** — [GitHub @yvan-ai](https://github.com/yvan-ai) · kenneyvan65@gmail.com

Licensed under the [MIT License](LICENSE).
