#!/usr/bin/env bash
# deploy.sh — Apply the lakehouse manifests to the current kubectl context.
#
#   ./scripts/deploy.sh          # apply the local overlay
#   ./scripts/deploy.sh --check  # dry-run (server-side) without applying
#
# Prerequisites: cluster bootstrapped (./scripts/bootstrap.sh) so the Strimzi
# and Flink operator CRDs exist, and minio.env present in the local overlay.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"
OVERLAY="${REPO_ROOT}/infra/kubernetes/overlays/local"

if [[ ! -f "${OVERLAY}/minio.env" ]]; then
  echo "ERROR: ${OVERLAY}/minio.env not found." >&2
  echo "  cp ${OVERLAY}/minio.env.example ${OVERLAY}/minio.env  # then edit" >&2
  exit 1
fi

if [[ "${1:-}" == "--check" ]]; then
  echo "Dry-run (server-side) of the local overlay..."
  kubectl apply -k "${OVERLAY}" --dry-run=server
  exit 0
fi

echo "Applying local overlay..."
kubectl apply -k "${OVERLAY}"

echo ""
echo "Deployment applied. Watch rollout with:"
echo "  kubectl get pods -A -w"
