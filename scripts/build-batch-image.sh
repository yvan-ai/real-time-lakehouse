#!/usr/bin/env bash
# Build the prebaked Spark batch image and import it into k3s containerd.
# No registry required — works for local dev on single-node k3s.
#
# CI pushes the same image to GHCR on main; the CD workflow bumps the tag in
# infra/kubernetes/base/spark/kustomization.yaml. To run a locally built image
# instead, point the kustomization at it:
#   cd infra/kubernetes/base/spark && kustomize edit set image spark-batch=spark-batch:local
set -euo pipefail

IMAGE_TAG="spark-batch:local"
DOCKERFILE="pipelines/batch/Dockerfile"
CONTEXT="pipelines/batch"

echo "Building ${IMAGE_TAG}..."
docker build -t "${IMAGE_TAG}" -f "${DOCKERFILE}" "${CONTEXT}"

echo "Importing into k3s containerd..."
docker save "${IMAGE_TAG}" | sudo k3s ctr images import -

echo ""
echo "Done. Image '${IMAGE_TAG}' is available in k3s."
echo "Run the batch: ./scripts/run-batch.sh"
