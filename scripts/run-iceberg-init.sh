#!/usr/bin/env bash
# Run init_iceberg.py via Docker — no local Spark install needed.
# The container uses --network host so it reaches MinIO (localhost:9000) and
# Nessie (localhost:19120) through the kubectl port-forwards below.
#
# Requirements: docker, kubectl

set -euo pipefail

SPARK_IMAGE="apache/spark:3.5.3-python3"
PACKAGES="\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262"

# Read MinIO credentials from the Kubernetes Secret
MINIO_ACCESS_KEY=$(kubectl get secret minio-credentials -n lakehouse \
  -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
MINIO_SECRET_KEY=$(kubectl get secret minio-credentials -n lakehouse \
  -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)

# Start MinIO + Nessie port-forwards in background; kill them on exit
echo "Port-forwarding MinIO on localhost:9000..."
kubectl port-forward svc/minio 9000:9000 -n lakehouse &>/dev/null &
PF_MINIO_PID=$!
echo "Port-forwarding Nessie on localhost:19120..."
kubectl port-forward svc/nessie 19120:19120 -n lakehouse &>/dev/null &
PF_NESSIE_PID=$!
trap 'kill "${PF_MINIO_PID}" "${PF_NESSIE_PID}" 2>/dev/null || true' EXIT INT TERM
sleep 3  # wait for the tunnels to be ready

echo "Running init_iceberg.py via Docker (Spark ${SPARK_IMAGE})..."
docker run --rm \
  --network host \
  -e MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" \
  -e MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
  -e MINIO_ENDPOINT="http://localhost:9000" \
  -e NESSIE_URI="http://localhost:19120/api/v1" \
  -v "$(pwd)/scripts:/opt/app/scripts:ro" \
  -v "$(pwd)/data:/opt/app/data:ro" \
  -w /opt/app \
  "${SPARK_IMAGE}" \
  /opt/spark/bin/spark-submit \
    --master "local[2]" \
    --driver-memory 512m \
    --packages "${PACKAGES}" \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy -Divy.home=/tmp/.ivy" \
    /opt/app/scripts/init_iceberg.py

echo ""
echo "Iceberg tables initialised. Verify via MinIO console:"
echo "  kubectl port-forward svc/minio 9001:9001 -n lakehouse"
echo "  open http://localhost:9001  (bucket: lakehouse)"
