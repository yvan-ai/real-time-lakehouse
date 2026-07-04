# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **real-time lakehouse** platform combining stream and batch processing into a
unified data architecture: Postgres → Debezium CDC → Kafka → Flink (hot path) / Spark
(cold path) → Iceberg on MinIO (Nessie catalog) → Trino. It runs fully on a local k3s
cluster (WSL2, 16 GB RAM). See `README.md` and `docs/architecture.md` for details.

## Development Commands

```bash
make help          # list all targets
make setup         # install dev + test dependencies, pre-commit hooks
make lint          # ruff check + ruff format --check + yamllint (config: .yamllint)
make typecheck     # mypy (config in pyproject.toml)
make test          # pytest quality/tests (local PySpark — needs Java 17)
make validate      # kustomize build + kubeconform
make dev-up        # lightweight Docker Compose stack (no k8s)

./scripts/bootstrap.sh        # provision k3s + operators + full overlay
./scripts/deploy.sh           # kubectl apply -k the local overlay
./scripts/run-iceberg-init.sh # create Iceberg tables (Spark in Docker)
./scripts/run-batch.sh        # run Bronze→Silver→Gold batch Job
./scripts/test.sh             # run the same checks as CI locally
```

Tests without local Java: run pytest inside the Spark image —
`docker run --rm --user root -v "$PWD":/app -w /app apache/spark:3.5.3-python3 bash -c
'export PYTHONPATH=/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*-src.zip); pip install -q pytest && python3 -m pytest quality/tests'`

## Architecture

### Data Flow

```
PostgreSQL (WAL)
  → Debezium on Kafka Connect (CDC, topics debezium.public.*)
  → Kafka (Strimzi, KRaft)
  → Flink (1-min event-time windows, exactly-once) → gold.order-revenue-1m topic
  → Spark batch (Bronze → Silver → Gold)
  → Iceberg tables (Nessie REST catalog, data on MinIO s3a://lakehouse/warehouse)
  → Trino (SQL) → dashboards / BI
```

### Key Directory Layout

| Path | Purpose |
|---|---|
| `pipelines/streaming/` | Flink job, Kafka topic CRs, Kafka Connect / Debezium |
| `pipelines/batch/spark-jobs/` | PySpark jobs + `lakehouse_common.py` shared lib |
| `data/models/iceberg/` | Iceberg DDL (bronze / silver / gold) — source of truth |
| `data/schemas/` | JSON Schemas for CDC payloads |
| `data/contracts/` | Data contracts (SLA, ownership) |
| `quality/great-expectations/` | GX suites, checkpoints, runner.py |
| `quality/tests/` | Unit tests (pytest + local SparkSession) |
| `observability/` | Prometheus rules, Grafana dashboards |
| `infra/kubernetes/` | Kustomize bases + `overlays/local` |
| `infra/argocd/` | GitOps project and applications |
| `docs/decisions/` | ADRs — read before changing architecture |

## Agent Routing (`.system/agents/`)

Each domain has a dedicated agent context file: infra-agent, streaming-agent,
data-engineer-agent, data-quality-agent, devops-agent, observability-agent.
When working in a specific domain, read the relevant agent file first.

## Constraints & Non-Negotiables

- **No plaintext secrets** — env files are gitignored (`*.env`); only `*.env.example`
  templates are committed. Kustomize `secretGenerator` renders Kubernetes Secrets.
- **All pods must define `resources.requests` and `resources.limits`** — WSL2, 16 GB RAM.
- **Flink jobs must use checkpointing and exactly-once semantics** where possible.
- **Iceberg**: partition pruning + Parquet/Snappy; plan schema evolution from the start.
- **Catalog consistency**: the Iceberg catalog is named `iceberg` (Nessie) in Spark,
  Trino and all DDL. Never reintroduce a second catalog name.
- **Commits**: conventional commits (`feat:`, `fix:`, `docs:`, ...), atomic. The commit
  author is the repository owner only — no co-author trailers.

## Service Access

- **MinIO S3 endpoint (in-cluster)**: `http://minio.lakehouse.svc.cluster.local:9000`
- **Nessie catalog (in-cluster)**: `http://nessie.lakehouse.svc.cluster.local:19120/api/v1`
- **Trino**: `kubectl port-forward svc/trino 8080:8080 -n lakehouse` → catalog `iceberg`
- **MinIO console**: `kubectl port-forward svc/minio 9001:9001 -n lakehouse`
- **Credentials**: `minio-credentials` Secret, generated from
  `infra/kubernetes/overlays/local/minio.env` (gitignored — never commit it)

### Iceberg medallion layers

| Layer | Namespace | Tables | Write mode |
|---|---|---|---|
| Bronze | `raw` | `kafka_events`, `cdc_orders`, `cdc_customers`, `cdc_order_items` | append-only |
| Silver | `silver` | `orders`, `customers`, `order_items` | merge-on-read upsert |
| Gold | `gold` | `daily_revenue`, `customer_metrics` | copy-on-write refresh |

Table initialisation: `./scripts/run-iceberg-init.sh` (port-forwards MinIO + Nessie,
runs `scripts/init_iceberg.py` in the Spark Docker image).
