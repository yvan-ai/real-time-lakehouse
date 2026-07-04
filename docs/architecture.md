# Architecture

This document describes the design of the real-time lakehouse in more depth than the
[README](../README.md). Decision rationale lives in [decisions/](decisions/).

## Overview

The platform implements a **kappa-leaning hybrid**: a single CDC stream from Postgres
feeds both a low-latency streaming path (Flink) and a replayable batch path (Spark)
that materialises the medallion layers in Iceberg.

```
PostgreSQL ──WAL──▶ Debezium ──▶ Kafka ──┬──▶ Flink ──▶ gold.order-revenue-1m (topic)
                                          │
                                          └──▶ Spark (bounded reads)
                                                 ├─▶ Bronze  raw.cdc_*        (append)
                                                 ├─▶ Silver  silver.*        (dedup upsert)
                                                 └─▶ Gold    gold.*          (full refresh)
                                                        ▲
                                     Trino ◀── Nessie ──┘        MinIO (S3 data files)
```

## Streaming path (hot)

- **Source**: Debezium publishes the Postgres WAL for `orders`, `customers` and
  `order_items` to `debezium.public.*` topics (JSON envelope, `schema-include=false`).
- **Processing**: the PyFlink job
  ([pipelines/streaming/flink-jobs/order-revenue/job.py](../pipelines/streaming/flink-jobs/order-revenue/job.py))
  reads the CDC stream with the `debezium-json` format — inserts, upserts and deletes are
  converted to retractions automatically — and aggregates order count, revenue and average
  order value in **1-minute tumbling event-time windows**.
- **Correctness**:
  - Watermarks with 5 s bounded out-of-orderness on `updated_at`.
  - `scan.watermark.idle-timeout = 30 s` so an idle source cannot stall the pipeline.
  - Checkpointing every 60 s in `EXACTLY_ONCE` mode; RocksDB state backend with
    checkpoints/savepoints on MinIO (`s3a://lakehouse/flink/...`).
- **Sink**: JSON results to the `gold.order-revenue-1m` topic for real-time consumers.

## Batch path (cold)

Three idempotent PySpark jobs run sequentially in a Kubernetes Job
([infra/kubernetes/base/spark/batch-job.yaml](../infra/kubernetes/base/spark/batch-job.yaml)),
sharing a common library ([lakehouse_common.py](../pipelines/batch/spark-jobs/lakehouse_common.py)):

1. **Bronze ingest** — bounded Kafka read (earliest→latest at job start); stores the full
   Debezium envelope per topic in `raw.cdc_*`. Append-only: Bronze is the replay log of
   the lakehouse.
2. **Silver transform** — parses `after` JSON, casts types, and deduplicates per business
   key with **last-write-wins on `ts_ms`** (window + `row_number`). Deleted rows
   (last op = `d`) never reach Silver.
3. **Gold aggregate** — daily revenue per currency (with new-customer counts) and customer
   lifetime metrics (LTV, recency, churn flag at 90 days).

Idempotence: Silver and Gold use `overwritePartitions()` so re-running a job replaces the
affected partitions instead of duplicating rows; Bronze appends are bounded by Kafka offsets.

## Storage layout

**Catalog**: Nessie (Iceberg REST) — see
[ADR-0002](decisions/0002-nessie-rest-catalog-over-hadoop.md). All engines (Spark, Trino)
address tables through the same catalog name `iceberg`.

**Warehouse**: `s3a://lakehouse/warehouse` on MinIO.

```
s3a://lakehouse/
├── warehouse/
│   ├── raw/       kafka_events, cdc_orders, cdc_customers, cdc_order_items
│   ├── silver/    orders, customers, order_items
│   └── gold/      daily_revenue, customer_metrics
└── flink/         checkpoints/, savepoints/
```

Table properties (see [data/models/iceberg/](../data/models/iceberg/)):

| Property | Bronze | Silver | Gold |
|---|---|---|---|
| Write mode | append | merge-on-read | copy-on-write |
| Target file size | 128 MB | 128 MB | 64 MB |
| Partitioning | `days(ingested_at)` | `days(created_at)` ± `bucket(16, order_id)` | `months(report_date)` / `bucket(32, customer_id)` |
| Snapshot expiry | 7 days | keep ≥ 5 | keep ≥ 3 |
| Format | Parquet + Snappy | Parquet + Snappy | Parquet + Snappy |

## Data quality

- **Great Expectations** — one suite per table (8 suites), one checkpoint per layer,
  executed by [quality/great-expectations/runner.py](../quality/great-expectations/runner.py)
  against Trino. A custom cross-column check asserts churn-flag consistency in Gold.
- **Unit tests** — the shared transformation functions are pure DataFrame→DataFrame and
  tested with a local SparkSession in CI (no cluster needed).
- **Contracts** — [data/contracts/](../data/contracts/) defines ownership, SLAs and schema
  guarantees for the CDC streams; [data/schemas/](../data/schemas/) holds the JSON Schemas.

## Deployment & GitOps

- **Kustomize** — bases per component under `infra/kubernetes/base/`, assembled by the
  `local` overlay. Secrets are generated from gitignored `.env` files (`secretGenerator`).
- **CI** (GitHub Actions) — yamllint, ruff/mypy, pytest, kustomize+kubeconform, then Docker
  image builds (Flink job, Kafka Connect) pushed to GHCR on `main`.
- **CD** — bumps image tags in kustomization files and (when configured) triggers an
  ArgoCD sync. ArgoCD watches the repo and reconciles the cluster to git state.

## Resource budget

The whole stack runs inside WSL2 with a 16 GB host. Every pod defines
`resources.requests` and `resources.limits`; approximate steady-state memory:

| Component | Request | Limit |
|---|---|---|
| Kafka (1 broker, KRaft) | 1 Gi | 2 Gi |
| Kafka Connect + Debezium | 512 Mi | 1 Gi |
| Flink (JM + TM) | 1 Gi | 2 Gi |
| Spark batch Job (ephemeral) | 1 Gi | 2 Gi |
| Trino (single node) | 1 Gi | 1.5 Gi |
| MinIO | 256 Mi | 512 Mi |
| Nessie | 256 Mi | 512 Mi |
| Postgres | 256 Mi | 512 Mi |
| Prometheus + Grafana | 512 Mi | 1 Gi |

Rule of thumb: run the batch Job while Flink is idle (or scale the FlinkDeployment down)
when RAM is tight.

## Security

- No credentials in git: `minio.env`, `.env` and all `*.env` files are gitignored;
  only `*.env.example` templates are committed.
- Kubernetes Secrets are rendered locally by Kustomize from those env files.
- Roadmap: External Secrets Operator (the `ExternalSecret` manifest for Flink already
  exists, commented out until ESO CRDs are installed).
