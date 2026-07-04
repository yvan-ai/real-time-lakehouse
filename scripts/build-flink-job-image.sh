#!/usr/bin/env bash
# Build the order-revenue Flink job image and import it into k3s containerd.
# No image registry required — works for local dev on single-node k3s.
set -euo pipefail

IMAGE_TAG="order-revenue-job:1.0.0"
DOCKERFILE="pipelines/streaming/flink-jobs/order-revenue/Dockerfile"
CONTEXT="pipelines/streaming/flink-jobs/order-revenue"

echo "Building ${IMAGE_TAG}..."
docker build -t "${IMAGE_TAG}" -f "${DOCKERFILE}" "${CONTEXT}"

echo "Importing into k3s containerd..."
docker save "${IMAGE_TAG}" | sudo k3s ctr images import -

echo ""
echo "Done. Image '${IMAGE_TAG}' is available in k3s."
echo "Deploy: kubectl apply -k infra/kubernetes/overlays/local/"
