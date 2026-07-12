# Prebaked Spark batch image — every runtime jar bundled at build time.
# Kills the per-run `--packages` Maven downloads: pod egress on the local
# cluster drops large transfers mid-stream (see roadmap "Platform hardening").
#
# Build + import into k3s: ./scripts/build-batch-image.sh
# CI builds and pushes ghcr.io/yvan-ai/real-time-lakehouse/spark-batch.
FROM apache/spark:3.5.3-python3

# Jar closure — keep versions in sync with docs/decisions/0008 and the
# Spark/Iceberg versions used across the repo:
#   - iceberg-spark-runtime  : Iceberg + bundled Nessie catalog
#   - hadoop-aws + aws-bundle: S3A filesystem for MinIO
#   - spark-sql-kafka (+ token-provider, kafka-clients, commons-pool2):
#     bounded Kafka reads in 01_bronze_ingest.py
#   - openlineage-spark      : lineage events to Marquez
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    cd /opt/spark/jars; \
    for artefact in \
      org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      io/openlineage/openlineage-spark_2.12/1.24.2/openlineage-spark_2.12-1.24.2.jar \
    ; do \
      # Explicit resume loop: the WSL egress resets large transfers
      # mid-stream (seen live on the 280 MB aws-sdk-bundle) and curl's
      # --retry does not resume partial files on this curl version.
      # Each attempt continues (-C -) where the previous one died.
      ok=""; \
      for attempt in $(seq 1 30); do \
        if curl -fSL --connect-timeout 15 -C - -O "${MAVEN}/${artefact}"; then ok=1; break; fi; \
        echo "retry ${attempt}/30 for ${artefact}"; sleep 3; \
      done; \
      [ -n "${ok}" ] || exit 1; \
    done
USER spark

# Job scripts baked in for the Airflow DAG (KubernetesPodOperator has no
# ConfigMap refresh step). The k8s Job path keeps mounting the ConfigMap at
# /scripts for fast local iteration — see scripts/run-batch.sh.
COPY spark-jobs/ /opt/spark/jobs/

