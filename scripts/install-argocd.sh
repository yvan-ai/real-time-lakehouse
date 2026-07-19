#!/usr/bin/env bash
# Install ArgoCD *core* — controller + repo-server + ApplicationSet controller,
# no API server / UI / Dex (roadmap pillar 6: the WSL2 RAM budget has no room
# for them). The cluster reconciles the lakehouse-root Application (app-of-apps
# → lakehouse-envs ApplicationSet → dev/staging/prod — ADR-0012) from git
# automatically; inspect state with `kubectl get applications -n argocd` or
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

# The ApplicationSet controller generates the per-environment applications
# (lakehouse-envs ApplicationSet — ADR-0012). Like the application controller
# below, it needs explicit, quota-sized resources: the LimitRange default
# would overshoot the namespace quota.
kubectl -n "${NAMESPACE}" patch deployment argocd-applicationset-controller --type='json' -p='[
  {"op":"add","path":"/spec/template/spec/containers/0/resources","value":
    {"requests":{"cpu":"50m","memory":"64Mi"},"limits":{"cpu":"300m","memory":"256Mi"}}}
]'
kubectl scale deployment argocd-applicationset-controller -n "${NAMESPACE}" --replicas=1

# The upstream manifests declare no resources, so the LimitRange default
# (200m CPU) applies — too little for the application controller: the
# ServerSideApply dry-run over the large Strimzi CRDs starves and the first
# sync never completes (seen live). Give it an explicit, quota-sized slice.
kubectl -n "${NAMESPACE}" patch statefulset argocd-application-controller --type='json' -p='[
  {"op":"add","path":"/spec/template/spec/containers/0/resources","value":
    {"requests":{"cpu":"200m","memory":"256Mi"},"limits":{"cpu":"500m","memory":"512Mi"}}}
]'

echo "Waiting for the controllers and repo server..."
kubectl rollout status statefulset/argocd-application-controller \
  -n "${NAMESPACE}" --timeout=300s
kubectl rollout status deployment/argocd-repo-server \
  -n "${NAMESPACE}" --timeout=300s
kubectl rollout status deployment/argocd-applicationset-controller \
  -n "${NAMESPACE}" --timeout=300s

echo ""
echo "ArgoCD core ${ARGOCD_VERSION} ready in namespace '${NAMESPACE}'."
echo "Register the platform:  kubectl apply -f infra/argocd/bootstrap/root.yaml"
echo "Check sync state:       kubectl get applicationsets,applications -n argocd"
