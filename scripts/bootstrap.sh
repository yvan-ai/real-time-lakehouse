#!/usr/bin/env bash
# bootstrap.sh — Provision the full local lakehouse from a clean WSL2 machine.
#
# Steps:
#   1. Install k3s (tuned for WSL2 / 16 GB RAM)
#   2. Install the Strimzi Kafka operator
#   3. Install the Flink Kubernetes operator
#   4. Generate MinIO credentials file if missing
#   5. Apply the full local overlay (MinIO, Postgres, Kafka, Flink, Nessie,
#      Trino, monitoring, topics, Kafka Connect)
#
# Idempotent: safe to re-run. Each step skips work already done.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"
MINIO_ENV="${REPO_ROOT}/infra/kubernetes/overlays/local/minio.env"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[BOOTSTRAP]${NC} $*"; }
warn() { echo -e "${YELLOW}[BOOTSTRAP]${NC} $*"; }

info "[1/5] k3s cluster"
"${SCRIPT_DIR}/setup-k3s.sh"

info "[2/5] Strimzi Kafka operator"
"${SCRIPT_DIR}/install-strimzi.sh"

info "[3/5] Flink Kubernetes operator"
"${SCRIPT_DIR}/install-flink-operator.sh"

info "[4/5] MinIO credentials"
if [[ ! -f "${MINIO_ENV}" ]]; then
  warn "No minio.env found — generating one with a random password."
  MINIO_PASSWORD="$(head -c 32 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 24)"
  cat > "${MINIO_ENV}" <<EOF
MINIO_ROOT_USER=lakehouse-admin
MINIO_ROOT_PASSWORD=${MINIO_PASSWORD}
EOF
  warn "Credentials written to ${MINIO_ENV} (gitignored). Keep them safe."
else
  info "minio.env already present — keeping existing credentials."
fi

info "[5/5] Applying local overlay (all services)"
kubectl apply -k "${REPO_ROOT}/infra/kubernetes/overlays/local/"

info "Done. Next steps:"
echo "  - Wait for pods:         kubectl get pods -A -w"
echo "  - Create Iceberg tables: ./scripts/run-iceberg-init.sh"
echo "  - Run the batch:         ./scripts/run-batch.sh"
