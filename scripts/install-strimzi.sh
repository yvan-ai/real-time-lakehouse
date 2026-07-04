#!/usr/bin/env bash
# Install Strimzi cluster operator (namespace-scoped to 'streaming').
# Run this once before applying the kafka Kustomize overlay.
# Idempotent: safe to re-run.
#
# Requirements: Kubernetes >= 1.30 (k3s latest satisfies this).

set -euo pipefail

STRIMZI_VERSION="0.51.0"
NAMESPACE="streaming"
BASE_URL="https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}"
CRDS_URL="${BASE_URL}/strimzi-crds-${STRIMZI_VERSION}.yaml"
OPERATOR_URL="${BASE_URL}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml"

CRDS_TMP="$(mktemp /tmp/strimzi-crds-XXXXXX.yaml)"
OPERATOR_TMP="$(mktemp /tmp/strimzi-operator-XXXXXX.yaml)"
trap 'rm -f "${CRDS_TMP}" "${OPERATOR_TMP}"' EXIT

echo "Installing Strimzi ${STRIMZI_VERSION} operator in namespace '${NAMESPACE}'..."

# Ensure namespaces exist first
kubectl apply -f infra/kubernetes/base/namespaces.yaml

# Step 1 — CRDs (cluster-scoped, no namespace rewrite needed)
echo "Downloading CRDs from ${CRDS_URL}..."
curl -fsSL "${CRDS_URL}" -o "${CRDS_TMP}"
kubectl apply --server-side --force-conflicts -f "${CRDS_TMP}"

# Step 2 — Cluster operator (namespace-scoped to NAMESPACE)
echo "Downloading cluster operator from ${OPERATOR_URL}..."
curl -fsSL "${OPERATOR_URL}" -o "${OPERATOR_TMP}"

# Verify we got YAML, not an HTML error page
if ! grep -q "^apiVersion:" "${OPERATOR_TMP}"; then
  echo "ERROR: Downloaded file does not look like Kubernetes YAML." >&2
  head -5 "${OPERATOR_TMP}" >&2
  exit 1
fi

# Rewrite every namespace reference from the Strimzi default ('myproject') to our target.
# Covers: metadata.namespace, subjects[].namespace, and STRIMZI_NAMESPACE env var value.
sed -i \
  -e "s/namespace: myproject/namespace: ${NAMESPACE}/g" \
  -e "s/value: myproject/value: ${NAMESPACE}/g" \
  "${OPERATOR_TMP}"

# -n flag is required: the Deployment has no namespace in its metadata and defaults
# to whatever the kubeconfig context says (usually 'default') without this flag.
kubectl apply --server-side --force-conflicts -n "${NAMESPACE}" -f "${OPERATOR_TMP}"

echo "Waiting for Strimzi operator to be ready..."
kubectl rollout status deployment/strimzi-cluster-operator \
  -n "${NAMESPACE}" \
  --timeout=180s

echo ""
echo "Strimzi ${STRIMZI_VERSION} ready in namespace '${NAMESPACE}'."
echo "Next: kubectl apply -k infra/kubernetes/overlays/local/"
