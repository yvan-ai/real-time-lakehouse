#!/usr/bin/env bash
# Trigger a fresh batch pipeline run (Bronze → Silver → Gold).
# Deletes any previous Job and recreates it — Kubernetes Jobs are immutable.
# On batch success the Great Expectations quality gate is launched
# (scripts/run-quality-gate.sh); skip it with SKIP_QUALITY_GATE=1.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_KUSTOMIZATION="infra/kubernetes/base/spark"
NAMESPACE="spark"
BATCH_TIMEOUT_SECONDS="${BATCH_TIMEOUT:-2400}"

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
# -k, not -f: the kustomization pins the prebaked spark-batch image tag
# (bumped by CD on main, or set to spark-batch:local for a local build).
kubectl apply -k "${JOB_KUSTOMIZATION}"

echo ""
echo "Job submitted. Follow logs with:"
echo "  kubectl logs -n ${NAMESPACE} -l app=batch-pipeline -f"
echo ""

echo "Waiting for the batch to finish (timeout ${BATCH_TIMEOUT_SECONDS}s)..."
elapsed=0
while true; do
  succeeded="$(kubectl get job batch-pipeline -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo '')"
  failed="$(kubectl get job batch-pipeline -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null || echo '')"
  if [[ "${succeeded:-0}" -ge 1 ]]; then
    echo "✓ Batch pipeline complete."
    break
  fi
  if [[ "${failed:-0}" -ge 1 ]]; then
    echo "✗ Batch pipeline FAILED — inspect logs with:" >&2
    echo "  kubectl logs -n ${NAMESPACE} -l app=batch-pipeline --tail=100" >&2
    exit 1
  fi
  if (( elapsed >= BATCH_TIMEOUT_SECONDS )); then
    echo "✗ Batch pipeline timed out after ${BATCH_TIMEOUT_SECONDS}s" >&2
    exit 1
  fi
  sleep 15
  elapsed=$((elapsed + 15))
done

if [[ "${SKIP_QUALITY_GATE:-0}" == "1" ]]; then
  echo "SKIP_QUALITY_GATE=1 — quality gate not launched."
  exit 0
fi

echo ""
echo "Launching the Great Expectations quality gate..."
"${SCRIPT_DIR}/run-quality-gate.sh"
