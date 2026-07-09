#!/usr/bin/env bash
# Install ArgoCD *core* — controller + repo-server only, no API server / UI /
# Dex (roadmap pillar 6: the WSL2 RAM budget has no room for them). The
# cluster reconciles the lakehouse-local Application from git automatically;
# inspect state with `kubectl get applications -n argocd` or
# `argocd admin dashboard` (runs the UI locally, off-cluster).
# Idempotent: safe to re-run; re-running upgrades in place.
set -euo pipefail

ARGOCD_VERSION="v2.13.3"
NAMESPACE="argocd"
MANIFEST_URL="https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/core-install.yaml"

echo "Installing ArgoCD core ${ARGOCD_VERSION} in namespace '${NAMESPACE}'..."

# Namespace (+ quota/limits) comes from the shared manifest.
kubectl apply -f "$(dirname "$0")/../infra/kubernetes/base/namespaces.yaml"

kubectl apply -n "${NAMESPACE}" -f "${MANIFEST_URL}"

# The ApplicationSet controller ships with core-install but nothing here uses
# ApplicationSets — scale it away to stay inside the namespace quota.
kubectl scale deployment argocd-applicationset-controller -n "${NAMESPACE}" \
  --replicas=0 2>/dev/null || true

# The upstream manifests declare no resources, so the LimitRange default
# (200m CPU) applies — too little for the application controller: the
# ServerSideApply dry-run over the large Strimzi CRDs starves and the first
# sync never completes (seen live). Give it an explicit, quota-sized slice.
kubectl -n "${NAMESPACE}" patch statefulset argocd-application-controller --type='json' -p='[
  {"op":"add","path":"/spec/template/spec/containers/0/resources","value":
    {"requests":{"cpu":"200m","memory":"256Mi"},"limits":{"cpu":"500m","memory":"512Mi"}}}
]'

echo "Waiting for the application controller and repo server..."
kubectl rollout status statefulset/argocd-application-controller \
  -n "${NAMESPACE}" --timeout=300s
kubectl rollout status deployment/argocd-repo-server \
  -n "${NAMESPACE}" --timeout=300s

echo ""
echo "ArgoCD core ${ARGOCD_VERSION} ready in namespace '${NAMESPACE}'."
echo "Register the app:  kubectl apply -f infra/argocd/project.yaml -f infra/argocd/apps/lakehouse-local.yaml"
echo "Check sync state:  kubectl get applications -n argocd"
