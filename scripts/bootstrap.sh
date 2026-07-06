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
MARQUEZ_ENV="${REPO_ROOT}/infra/kubernetes/overlays/local/marquez.env"
AIRFLOW_ENV="${REPO_ROOT}/infra/kubernetes/overlays/local/airflow.env"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[BOOTSTRAP]${NC} $*"; }
warn() { echo -e "${YELLOW}[BOOTSTRAP]${NC} $*"; }

info "[1/7] k3s cluster"
"${SCRIPT_DIR}/setup-k3s.sh"

info "[2/7] Strimzi Kafka operator"
"${SCRIPT_DIR}/install-strimzi.sh"

info "[3/7] Flink Kubernetes operator"
"${SCRIPT_DIR}/install-flink-operator.sh"

info "[4/7] MinIO credentials"
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

info "[5/7] Marquez credentials"
if [[ ! -f "${MARQUEZ_ENV}" ]]; then
  warn "No marquez.env found — generating one with a random password."
  MARQUEZ_PASSWORD="$(head -c 32 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 24)"
  cat > "${MARQUEZ_ENV}" <<EOF
MARQUEZ_DB_USER=marquez
MARQUEZ_DB_PASSWORD=${MARQUEZ_PASSWORD}
EOF
  warn "Credentials written to ${MARQUEZ_ENV} (gitignored). Keep them safe."
else
  info "marquez.env already present — keeping existing credentials."
fi

info "[6/7] Airflow credentials"
if [[ ! -f "${AIRFLOW_ENV}" ]]; then
  warn "No airflow.env found — generating one with random values."
  AIRFLOW_DB_PASSWORD="$(head -c 32 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 24)"
  AIRFLOW_ADMIN_PASSWORD="$(head -c 32 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 24)"
  AIRFLOW_WEBSERVER_SECRET_KEY="$(head -c 32 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 32)"
  # Fernet needs exactly 32 url-safe base64-encoded bytes.
  AIRFLOW_FERNET_KEY="$(head -c 32 /dev/urandom | base64 | tr '+/' '-_')"
  cat > "${AIRFLOW_ENV}" <<EOF
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
AIRFLOW_WEBSERVER_SECRET_KEY=${AIRFLOW_WEBSERVER_SECRET_KEY}
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY}
EOF
  warn "Credentials written to ${AIRFLOW_ENV} (gitignored). Keep them safe."
else
  info "airflow.env already present — keeping existing credentials."
fi

info "[7/7] Applying local overlay (all services)"
kubectl apply -k "${REPO_ROOT}/infra/kubernetes/overlays/local/"

info "Initialising the marquez database in the shared Postgres..."
kubectl delete job marquez-db-init -n streaming --ignore-not-found --wait=true
kubectl apply -f "${REPO_ROOT}/infra/kubernetes/base/marquez/db-init-job.yaml"

info "Initialising the airflow database in the shared Postgres..."
kubectl delete job airflow-db-init -n streaming --ignore-not-found --wait=true
kubectl apply -f "${REPO_ROOT}/infra/kubernetes/base/airflow/db-init-job.yaml"

info "Done. Next steps:"
echo "  - Wait for pods:         kubectl get pods -A -w"
echo "  - Create Iceberg tables: ./scripts/run-iceberg-init.sh"
echo "  - Run the batch:         ./scripts/run-batch.sh (or trigger the lakehouse_batch DAG)"
echo "  - Airflow UI:            kubectl port-forward svc/airflow 8081:8080 -n orchestration"
echo "  - Register CDC lineage:  python3 scripts/register_lineage.py (port-forward marquez first)"
