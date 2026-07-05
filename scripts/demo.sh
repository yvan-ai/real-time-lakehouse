#!/usr/bin/env bash
# demo.sh — One-command live demo against the k3s stack.
#
# Starts:
#   - kubectl port-forwards (Postgres 5432, Trino 8080)
#   - the e-commerce traffic generator (background)
#   - the Streamlit dashboard (foreground — Ctrl-C stops everything)
#
# Prereqs: cluster bootstrapped and healthy, plus:
#   pip install -r demo/requirements.txt
#
# Env overrides: DEMO_RATE (actions/min, default 40)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"
cd "${REPO_ROOT}"

# ── Preflight ──────────────────────────────────────────────────────────────────
kubectl get ns lakehouse &>/dev/null \
  || { echo "ERROR: cluster not reachable — run ./scripts/bootstrap.sh first" >&2; exit 1; }

python3 - <<'EOF' || { echo "ERROR: missing demo deps — pip install -r demo/requirements.txt" >&2; exit 1; }
import kafka, psycopg2, streamlit, trino  # noqa: F401
EOF

# ── Credentials & tunnels ──────────────────────────────────────────────────────
PGUSER=$(kubectl get secret postgres-credentials -n streaming -o jsonpath='{.data.username}' | base64 -d)
PGPASSWORD=$(kubectl get secret postgres-credentials -n streaming -o jsonpath='{.data.password}' | base64 -d)
export PGUSER PGPASSWORD PGHOST=localhost PGPORT=5432 PGDATABASE=lakehouse
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:32100}"   # Strimzi nodeport listener
export TRINO_HOST=localhost TRINO_PORT=8080

PIDS=()
cleanup() {
  echo ""
  echo "Stopping generator and port-forwards..."
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

echo "Port-forwarding Postgres (5432) and Trino (8080)..."
kubectl port-forward svc/postgres 5432:5432 -n streaming &>/dev/null &
PIDS+=($!)
kubectl port-forward svc/trino 8080:8080 -n lakehouse &>/dev/null &
PIDS+=($!)
sleep 3

echo "Starting traffic generator (${DEMO_RATE:-40} actions/min) — log: /tmp/demo-traffic.log"
python3 demo/generate_traffic.py --rate "${DEMO_RATE:-40}" >/tmp/demo-traffic.log 2>&1 &
PIDS+=($!)

echo ""
echo "Dashboard: http://localhost:8501  (Ctrl-C stops everything)"
echo "Tip: run ./scripts/run-batch.sh in another terminal to refresh the Gold tables."
echo ""
streamlit run demo/dashboard.py --server.headless true
