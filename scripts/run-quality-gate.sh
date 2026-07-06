#!/usr/bin/env bash
# Run the Great Expectations quality gate as a Kubernetes Job.
# Refreshes the GX ConfigMaps from quality/great-expectations/, recreates the
# Job (immutable), waits for it to finish, and exits with the Job's verdict:
#   0 — all expectations passed
#   1 — expectations failed or the Job errored
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"
GX_DIR="${REPO_ROOT}/quality/great-expectations"
JOB_YAML="${REPO_ROOT}/infra/kubernetes/base/quality/quality-gate-job.yaml"
NAMESPACE="data-quality"
TIMEOUT_SECONDS="${QUALITY_GATE_TIMEOUT:-1800}"

echo "Ensuring namespace ${NAMESPACE} exists..."
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 \
  || kubectl create namespace "${NAMESPACE}"

echo "Refreshing GX ConfigMaps from ${GX_DIR}..."
kubectl create configmap quality-gate-project -n "${NAMESPACE}" \
  --from-file=great_expectations.yml="${GX_DIR}/great_expectations.yml" \
  --from-file=runner.py="${GX_DIR}/runner.py" \
  --from-file=requirements-quality.txt="${GX_DIR}/requirements-quality.txt" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap quality-gate-expectations -n "${NAMESPACE}" \
  --from-file="${GX_DIR}/expectations/" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap quality-gate-checkpoints -n "${NAMESPACE}" \
  --from-file="${GX_DIR}/checkpoints/" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Deleting previous quality-gate Job (if any)..."
kubectl delete job quality-gate -n "${NAMESPACE}" --ignore-not-found --wait=true

echo "Submitting quality-gate Job..."
kubectl apply -f "${JOB_YAML}"

echo "Waiting for the gate to finish (timeout ${TIMEOUT_SECONDS}s)..."
elapsed=0
while true; do
  succeeded="$(kubectl get job quality-gate -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo '')"
  failed="$(kubectl get job quality-gate -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null || echo '')"
  if [[ "${succeeded:-0}" -ge 1 ]]; then
    echo ""
    kubectl logs -n "${NAMESPACE}" job/quality-gate --tail=40 || true
    echo ""
    echo "✓ Quality gate PASSED"
    exit 0
  fi
  if [[ "${failed:-0}" -ge 1 ]]; then
    echo ""
    kubectl logs -n "${NAMESPACE}" job/quality-gate --tail=80 || true
    echo ""
    echo "✗ Quality gate FAILED — expectations did not pass" >&2
    exit 1
  fi
  if (( elapsed >= TIMEOUT_SECONDS )); then
    echo "✗ Quality gate timed out after ${TIMEOUT_SECONDS}s" >&2
    kubectl logs -n "${NAMESPACE}" job/quality-gate --tail=40 2>/dev/null || true
    exit 1
  fi
  sleep 10
  elapsed=$((elapsed + 10))
done
