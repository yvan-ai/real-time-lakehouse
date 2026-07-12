#!/usr/bin/env bash
# Build the prebaked Spark batch image and import it into k3s containerd.
# No registry required — works for local dev on single-node k3s, and it is
# the RELIABLE path on this machine: the WSL egress resets large registry
# pulls mid-stream, so the node cannot fetch the 750 MB jar layer from GHCR.
#
# The image is imported under the GHCR name with BOTH the :latest tag (used
# by the Airflow DAG) and the sha tag currently pinned in the spark
# kustomization (used by run-batch.sh) — kubelet's IfNotPresent then never
# needs the network.
set -euo pipefail

IMAGE="ghcr.io/yvan-ai/real-time-lakehouse/spark-batch"
DOCKERFILE="pipelines/batch/Dockerfile"
CONTEXT="pipelines/batch"

# `|| true`: a missing pin must not abort the build (set -e + pipefail).
PINNED_TAG="$(grep -A2 'name: spark-batch' infra/kubernetes/base/spark/kustomization.yaml \
  | grep newTag | awk '{print $2}' || true)"

echo "Building ${IMAGE}:latest..."
docker build -t "${IMAGE}:latest" -f "${DOCKERFILE}" "${CONTEXT}"

if [[ -n "${PINNED_TAG}" && "${PINNED_TAG}" != "latest" ]]; then
  echo "Tagging also as ${IMAGE}:${PINNED_TAG} (spark kustomization pin)..."
  docker tag "${IMAGE}:latest" "${IMAGE}:${PINNED_TAG}"
fi

echo "Importing into k3s containerd..."
docker save "${IMAGE}:latest" ${PINNED_TAG:+"${IMAGE}:${PINNED_TAG}"} | sudo k3s ctr images import -

echo ""
echo "Done. Image '${IMAGE}' (latest${PINNED_TAG:+, ${PINNED_TAG}}) is available in k3s."
echo "Run the batch: ./scripts/run-batch.sh"
