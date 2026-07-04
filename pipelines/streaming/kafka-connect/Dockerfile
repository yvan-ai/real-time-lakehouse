FROM quay.io/strimzi/kafka:0.51.0-kafka-4.2.0

ARG DBZ_VERSION=3.1.0.Final

USER root

RUN mkdir -p /opt/kafka/plugins/debezium-postgres && \
    curl -fsSL \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DBZ_VERSION}/debezium-connector-postgres-${DBZ_VERSION}-plugin.tar.gz" \
    | tar -xz -C /opt/kafka/plugins/debezium-postgres --strip-components=1

USER 1001
