FROM flink:1.20.1-scala_2.12-java17

ARG FLINK_VERSION=1.20.1
ARG PYFLINK_VERSION=1.20.0
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20

USER root

# Install Python (slim — no dev headers needed at runtime).
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink must match the Flink image version.
RUN pip3 install --no-cache-dir apache-flink==${PYFLINK_VERSION}

# Kafka SQL connector (includes debezium-json format support via flink-json).
RUN curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

# Enable S3A filesystem plugin for MinIO checkpoints.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar \
       /opt/flink/plugins/s3-fs-hadoop/

COPY job.py /opt/flink/usrlib/job.py

USER flink
