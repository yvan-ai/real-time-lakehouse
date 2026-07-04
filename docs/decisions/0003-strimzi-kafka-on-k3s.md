# ADR-0003 — Strimzi-operated Kafka in KRaft mode

**Status**: Accepted · **Date**: 2026-07-04

## Context

The event bus must run inside a 16 GB WSL2 machine alongside Flink, Spark, Trino and the
rest of the stack. Options: plain Kafka StatefulSets, Bitnami Helm chart, Redpanda, or
the Strimzi operator. A ZooKeeper ensemble alone would cost ~1 GB of RAM.

## Decision

Deploy Kafka with the **Strimzi operator** in **KRaft mode** (no ZooKeeper), one broker,
with JMX metrics exported for Prometheus. Topics are declared as `KafkaTopic` custom
resources under `pipelines/streaming/kafka-topics/` — reviewable, GitOps-friendly
configuration instead of imperative `kafka-topics.sh` calls.

## Consequences

- Kafka upgrades, certificates and rolling restarts are handled by the operator.
- KRaft saves the entire ZooKeeper footprint; a single broker is an accepted
  availability trade-off for a local environment (replication factor 1).
- Kafka Connect (Debezium) is also managed by Strimzi (`KafkaConnect` +
  `KafkaConnector` CRs), keeping the whole streaming layer declarative.
- CRDs must be installed before the overlay applies — handled by
  `scripts/install-strimzi.sh` in the bootstrap sequence.
