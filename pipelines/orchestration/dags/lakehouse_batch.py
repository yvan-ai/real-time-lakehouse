"""Lakehouse batch pipeline DAG (ADR-0009).

    batch_bronze_silver -> dbt_build_gold -> quality_gate -> register_lineage

Every task is a KubernetesPodOperator pod spawned in the namespace that owns
the work (spark / data-quality / lineage), so the Airflow pod stays small and
the per-namespace quotas keep applying. Successful pods are deleted; FAILED
pods are kept so kube-state-metrics exposes them to the Lakehouse Pipeline
dashboard and alerts.

Failure semantics: any failed expectation makes the quality-gate pod exit
non-zero, which fails the task and the DAG run — the gate blocks the pipeline.

Gold cutover (roadmap pillar 5): on this DAG the Gold layer is built by dbt
on Trino (dbt_build_gold); the Spark job 03_gold_aggregate.py remains only in
the no-Airflow fallback path (scripts/run-batch.sh).

Pod specs deliberately mirror the standalone manifests — keep in sync with:
  - infra/kubernetes/base/spark/batch-job.yaml        (batch_bronze_silver)
  - pipelines/dbt/                                    (dbt_build_gold)
  - infra/kubernetes/base/quality/quality-gate-job.yaml (quality_gate)
  - scripts/register_lineage.py — shipped as the register-lineage-script
    ConfigMap by scripts/deploy.sh                    (register_lineage)
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# CI pushes :latest on every main build; the sha-pinned tag only exists in
# the spark kustomization, which a DAG cannot read. Override per-deployment
# with the SPARK_BATCH_IMAGE env var on the Airflow pod if needed.
SPARK_BATCH_IMAGE = "ghcr.io/yvan-ai/real-time-lakehouse/spark-batch:latest"

MARQUEZ_URL = "http://marquez.lineage.svc.cluster.local:5000"

COMMON_KPO_ARGS = {
    "in_cluster": True,
    "get_logs": True,
    "image_pull_policy": "IfNotPresent",
    "startup_timeout_seconds": 300,
    # Keep failed pods around for inspection + kube-state-metrics alerting.
    "on_finish_action": "delete_succeeded_pod",
}

# ── batch: Bronze + Silver (Spark, prebaked image) ───────────────────────────
BATCH_SCRIPT = """
set -euo pipefail

OL_CONFS=()
if [[ -n "${OPENLINEAGE_URL:-}" ]]; then
  OL_CONFS=(
    --conf "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener"
    --conf "spark.openlineage.transport.type=http"
    --conf "spark.openlineage.transport.url=${OPENLINEAGE_URL}"
    --conf "spark.openlineage.transport.timeoutInMillis=30000"
    --conf "spark.openlineage.namespace=lakehouse"
  )
fi

run_job() {
  /opt/spark/bin/spark-submit \
    --master "local[2]" \
    --py-files /opt/spark/jobs/lakehouse_common.py \
    "${OL_CONFS[@]}" \
    "$1"
}

echo "[1/2] Bronze ingest — Kafka → raw Iceberg"
run_job /opt/spark/jobs/01_bronze_ingest.py

echo "[2/2] Silver transform — Bronze → Silver"
run_job /opt/spark/jobs/02_silver_transform.py
"""

# ── dbt: Gold models on Trino (pillar 5) ─────────────────────────────────────
DBT_SCRIPT = """
set -euo pipefail

echo "[1/3] Installing dbt-trino + openlineage-dbt..."
# Full-command retry loop: pod egress drops large downloads mid-stream on
# this cluster (same pattern as the quality gate).
for attempt in 1 2 3 4 5; do
  pip install --no-cache-dir --quiet --retries 10 --timeout 60 \
    "dbt-trino==1.8.4" "openlineage-dbt==1.24.2" && break
  echo "pip install failed (attempt ${attempt}/5) — retrying in 15s..."
  [[ "${attempt}" == "5" ]] && exit 1
  sleep 15
done

echo "[2/3] Assembling dbt project..."
mkdir -p /tmp/dbt/models/gold /tmp/dbt/tests
cp /dbt-src/dbt_project.yml /dbt-src/profiles.yml /tmp/dbt/
cp /dbt-models/*.sql /dbt-models/*.yml /tmp/dbt/models/gold/
cp /dbt-tests/*.sql /tmp/dbt/tests/

echo "[3/3] dbt build (with OpenLineage emission)..."
cd /tmp/dbt && dbt-ol build --profiles-dir . --target k8s
"""

# ── quality gate: Great Expectations against Trino ──────────────────────────
GATE_SCRIPT = """
set -euo pipefail

echo "[1/3] Installing quality dependencies..."
for attempt in 1 2 3 4 5; do
  pip install --no-cache-dir --quiet --retries 10 --timeout 60 \
    -r /gx-src/requirements-quality.txt && break
  echo "pip install failed (attempt ${attempt}/5) — retrying in 15s..."
  [[ "${attempt}" == "5" ]] && exit 1
  sleep 15
done

echo "[2/3] Assembling GX project..."
mkdir -p /gx/expectations /gx/checkpoints /gx/uncommitted
cp /gx-src/great_expectations.yml /gx-src/runner.py /gx/
cp /gx-src-expectations/*.json /gx/expectations/
cp /gx-src-checkpoints/*.yml /gx/checkpoints/

echo "[3/3] Running checkpoints against Trino..."
cd /gx && python runner.py --layer all
"""


def _configmap_volume(name: str) -> k8s.V1Volume:
    return k8s.V1Volume(name=name, config_map=k8s.V1ConfigMapVolumeSource(name=name))


def _secret_env(name: str, secret: str, key: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key)),
    )


with DAG(
    dag_id="lakehouse_batch",
    description="Bronze→Silver (Spark) → Gold (dbt) → GX quality gate → lineage",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "batch"],
) as dag:
    batch_bronze_silver = KubernetesPodOperator(
        task_id="batch_bronze_silver",
        namespace="spark",
        image=SPARK_BATCH_IMAGE,
        name="airflow-batch-bronze-silver",
        cmds=["/bin/bash", "-c"],
        arguments=[BATCH_SCRIPT],
        env_vars=[
            _secret_env("MINIO_ACCESS_KEY", "minio-credentials", "MINIO_ROOT_USER"),
            _secret_env("MINIO_SECRET_KEY", "minio-credentials", "MINIO_ROOT_PASSWORD"),
            k8s.V1EnvVar(
                name="KAFKA_BOOTSTRAP",
                value="kafka-dev-kafka-bootstrap.streaming.svc.cluster.local:9092",
            ),
            k8s.V1EnvVar(
                name="NESSIE_URI",
                value="http://nessie.lakehouse.svc.cluster.local:19120/api/v1",
            ),
            k8s.V1EnvVar(
                name="MINIO_ENDPOINT",
                value="http://minio.lakehouse.svc.cluster.local:9000",
            ),
            k8s.V1EnvVar(name="OPENLINEAGE_URL", value=MARQUEZ_URL),
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "100m"},
            limits={"memory": "2Gi", "cpu": "2"},
        ),
        **COMMON_KPO_ARGS,
    )

    dbt_build_gold = KubernetesPodOperator(
        task_id="dbt_build_gold",
        namespace="data-quality",
        image="python:3.11-slim",
        name="airflow-dbt-build-gold",
        cmds=["/bin/bash", "-c"],
        arguments=[DBT_SCRIPT],
        env_vars=[
            k8s.V1EnvVar(name="OPENLINEAGE_URL", value=MARQUEZ_URL),
            k8s.V1EnvVar(name="OPENLINEAGE_NAMESPACE", value="lakehouse"),
        ],
        volumes=[
            _configmap_volume("dbt-project"),
            _configmap_volume("dbt-models"),
            _configmap_volume("dbt-tests"),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name="dbt-project", mount_path="/dbt-src"),
            k8s.V1VolumeMount(name="dbt-models", mount_path="/dbt-models"),
            k8s.V1VolumeMount(name="dbt-tests", mount_path="/dbt-tests"),
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "128Mi", "cpu": "50m"},
            limits={"memory": "512Mi", "cpu": "500m"},
        ),
        **COMMON_KPO_ARGS,
    )

    quality_gate = KubernetesPodOperator(
        task_id="quality_gate",
        namespace="data-quality",
        image="python:3.11-slim",
        name="airflow-quality-gate",
        cmds=["/bin/bash", "-c"],
        arguments=[GATE_SCRIPT],
        env_vars=[
            k8s.V1EnvVar(
                name="GX_TRINO_CONNECTION_STRING",
                value="trino://admin@trino.lakehouse.svc.cluster.local:8080/iceberg",
            ),
            k8s.V1EnvVar(
                name="PUSHGATEWAY_URL",
                value="http://pushgateway.monitoring.svc.cluster.local:9091",
            ),
        ],
        volumes=[
            _configmap_volume("quality-gate-project"),
            _configmap_volume("quality-gate-expectations"),
            _configmap_volume("quality-gate-checkpoints"),
            k8s.V1Volume(name="gx-workdir", empty_dir=k8s.V1EmptyDirVolumeSource()),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name="quality-gate-project", mount_path="/gx-src"),
            k8s.V1VolumeMount(name="quality-gate-expectations", mount_path="/gx-src-expectations"),
            k8s.V1VolumeMount(name="quality-gate-checkpoints", mount_path="/gx-src-checkpoints"),
            k8s.V1VolumeMount(name="gx-workdir", mount_path="/gx"),
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "256Mi", "cpu": "50m"},
            limits={"memory": "512Mi", "cpu": "500m"},
        ),
        **COMMON_KPO_ARGS,
    )

    register_lineage = KubernetesPodOperator(
        task_id="register_lineage",
        namespace="lineage",
        image="python:3.11-slim",
        name="airflow-register-lineage",
        cmds=["python", "/scripts/register_lineage.py", "--url", MARQUEZ_URL],
        volumes=[_configmap_volume("register-lineage-script")],
        volume_mounts=[
            k8s.V1VolumeMount(name="register-lineage-script", mount_path="/scripts"),
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "64Mi", "cpu": "50m"},
            limits={"memory": "128Mi", "cpu": "200m"},
        ),
        **COMMON_KPO_ARGS,
    )

    batch_bronze_silver >> dbt_build_gold >> quality_gate >> register_lineage
