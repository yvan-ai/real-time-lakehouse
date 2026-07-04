#!/usr/bin/env bash
# Build the Debezium Connect image and import it directly into k3s containerd.
# No image registry required — works for local dev on single-node k3s.

set -euo pipefail

IMAGE_TAG="debezium-connect:3.1.0-kafka-4.2.0"
DOCKERFILE="pipelines/streaming/kafka-connect/Dockerfile"

echo "Building ${IMAGE_TAG}..."
docker build -t "${IMAGE_TAG}" -f "${DOCKERFILE}" .

echo "Importing into k3s containerd..."
docker save "${IMAGE_TAG}" | sudo k3s ctr images import -

echo ""
echo "Done. Image '${IMAGE_TAG}' is available in k3s."
echo "Apply the KafkaConnect manifest:"
echo "  kubectl apply -k infra/kubernetes/overlays/local/"
