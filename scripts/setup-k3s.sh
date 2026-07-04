#!/usr/bin/env bash
# setup-k3s.sh — Install k3s and configure namespaces for the real-time lakehouse
# Target: WSL2, 16GB RAM, max 10GB used by k3s + data stack
set -euo pipefail

K3S_VERSION="v1.29.4+k3s1"
KUBECONFIG_PATH="${HOME}/.kube/config"
K3S_CONFIG_SRC="$(dirname "$0")/../infra/kubernetes/config/k3s-config.yaml"
K3S_CONFIG_DEST="/etc/rancher/k3s/config.yaml"
NAMESPACES_MANIFEST="$(dirname "$0")/../infra/kubernetes/base/namespaces.yaml"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Preflight ──────────────────────────────────────────────────────────────────
check_wsl() {
  if ! grep -qi microsoft /proc/version 2>/dev/null; then
    warn "Not running in WSL2 — continuing anyway, but this script is tuned for WSL2."
  fi
}

check_ram() {
  local total_mb
  total_mb=$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo)
  info "Detected ${total_mb} MB RAM total."
  if [[ "$total_mb" -lt 8000 ]]; then
    error "Less than 8 GB RAM detected (${total_mb} MB). Minimum required for a minimal cluster + data stack."
  fi
  if [[ "$total_mb" -lt 12000 ]]; then
    warn "Less than 12 GB RAM (${total_mb} MB). Running in constrained mode — deploy one namespace at a time, not the full stack simultaneously."
  fi
}

check_deps() {
  for cmd in curl kubectl; do
    if ! command -v "$cmd" &>/dev/null; then
      warn "'$cmd' not found — will be installed or may fail."
    fi
  done
}

# ── k3s install ────────────────────────────────────────────────────────────────
install_k3s() {
  if command -v k3s &>/dev/null; then
    info "k3s already installed: $(k3s --version | head -1)"
    return
  fi

  info "Installing k3s ${K3S_VERSION}..."

  sudo mkdir -p /etc/rancher/k3s
  sudo cp "$K3S_CONFIG_SRC" "$K3S_CONFIG_DEST"
  info "k3s config written to ${K3S_CONFIG_DEST}"

  curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="${K3S_VERSION}" sh -
  info "k3s installed."
}

# ── kubeconfig ─────────────────────────────────────────────────────────────────
setup_kubeconfig() {
  mkdir -p "$(dirname "$KUBECONFIG_PATH")"
  sudo cp /etc/rancher/k3s/k3s.yaml "$KUBECONFIG_PATH"
  sudo chown "$(id -u):$(id -g)" "$KUBECONFIG_PATH"
  chmod 600 "$KUBECONFIG_PATH"
  export KUBECONFIG="$KUBECONFIG_PATH"
  info "KUBECONFIG set to ${KUBECONFIG_PATH}"
  echo "export KUBECONFIG=${KUBECONFIG_PATH}" >> "${HOME}/.bashrc"
}

# ── Wait for cluster ───────────────────────────────────────────────────────────
wait_for_cluster() {
  info "Waiting for k3s API server..."
  local retries=30
  until kubectl cluster-info &>/dev/null || [[ $retries -eq 0 ]]; do
    sleep 5
    ((retries--))
  done
  [[ $retries -gt 0 ]] || error "k3s API server did not become ready in time."
  info "Cluster is up."

  info "Waiting for core nodes to be Ready..."
  kubectl wait --for=condition=Ready node --all --timeout=120s
}

# ── Namespaces & resource policies ────────────────────────────────────────────
apply_namespaces() {
  info "Applying namespace definitions and resource policies..."
  kubectl apply -f "$NAMESPACES_MANIFEST"
  info "Namespaces ready."
}

# ── Smoke test ─────────────────────────────────────────────────────────────────
smoke_test() {
  info "Running smoke test..."
  kubectl get nodes -o wide
  echo ""
  kubectl get namespaces | grep -E "NAME|streaming|lakehouse|spark|orchestration|monitoring|argocd|data-quality"
  echo ""
  kubectl top nodes 2>/dev/null || warn "metrics-server not yet ready — run 'kubectl top nodes' later."
  info "Setup complete. Cluster is ready for data stack deployment."
}

# ── Main ───────────────────────────────────────────────────────────────────────
main() {
  info "=== Real-Time Lakehouse — k3s Setup ==="
  check_wsl
  check_ram
  check_deps
  install_k3s
  setup_kubeconfig
  wait_for_cluster
  apply_namespaces
  smoke_test
}

main "$@"
