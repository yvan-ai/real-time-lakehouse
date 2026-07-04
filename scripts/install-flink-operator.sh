#!/usr/bin/env bash
# Install the Flink Kubernetes Operator via Helm.
# The operator watches FlinkDeployment CRs cluster-wide and manages JM/TM pods.
# Run once; re-run to upgrade.
set -euo pipefail

OPERATOR_VERSION="1.10.0"
NAMESPACE="flink-operator"

echo "Adding Apache Flink Helm repo..."
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-${OPERATOR_VERSION}/
helm repo update

echo "Installing Flink Kubernetes Operator ${OPERATOR_VERSION} into namespace '${NAMESPACE}'..."
helm upgrade --install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --version "${OPERATOR_VERSION}" \
  --set webhook.create=false \
  --set "defaultConfiguration.create=true" \
  --set resources.requests.memory=256Mi \
  --set resources.limits.memory=512Mi \
  --set resources.requests.cpu=100m \
  --set resources.limits.cpu=500m \
  --wait --timeout 5m

echo ""
echo "Flink Kubernetes Operator installed in namespace '${NAMESPACE}'."
echo "Verify: kubectl get pods -n ${NAMESPACE}"
echo ""
echo "Next steps:"
echo "  1. Build and import the job image:  ./scripts/build-flink-job-image.sh"
echo "  2. Create secrets:                  see infra/kubernetes/base/flink/flink-deployment.yaml"
echo "  3. Apply Flink manifests:           kubectl apply -k infra/kubernetes/overlays/local/"
