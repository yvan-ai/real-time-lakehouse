#!/usr/bin/env bash
# Trigger a fresh batch pipeline run (Bronze → Silver → Gold).
# Deletes any previous Job and recreates it — Kubernetes Jobs are immutable.
set -euo pipefail

JOB_YAML="infra/kubernetes/base/spark/batch-job.yaml"
NAMESPACE="spark"

echo "Updating ConfigMap with latest job scripts..."
kubectl create configmap batch-spark-jobs -n "${NAMESPACE}" \
  --from-file=lakehouse_common.py=pipelines/batch/spark-jobs/lakehouse_common.py \
  --from-file=01_bronze_ingest.py=pipelines/batch/spark-jobs/01_bronze_ingest.py \
  --from-file=02_silver_transform.py=pipelines/batch/spark-jobs/02_silver_transform.py \
  --from-file=03_gold_aggregate.py=pipelines/batch/spark-jobs/03_gold_aggregate.py \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Deleting previous Job (if any)..."
kubectl delete job batch-pipeline -n "${NAMESPACE}" --ignore-not-found --wait=false

echo "Waiting for old pod to terminate..."
kubectl wait pod -n "${NAMESPACE}" -l app=batch-pipeline \
  --for=delete --timeout=60s 2>/dev/null || true

echo "Submitting new batch Job..."
kubectl apply -f "${JOB_YAML}"

echo ""
echo "Job submitted. Follow logs with:"
echo "  kubectl logs -n ${NAMESPACE} -l app=batch-pipeline -f"
echo ""
echo "Check status:"
echo "  kubectl get pod -n ${NAMESPACE}"
