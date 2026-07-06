# Operators for the local lakehouse — replaces scripts/install-strimzi.sh and
# scripts/install-flink-operator.sh on fresh clusters (the scripts remain as
# a no-Terraform fallback; bootstrap.sh picks whichever is available).
#
# Prerequisite: infra/kubernetes/base/namespaces.yaml applied (setup-k3s.sh
# does it) — the streaming namespace and its quota are owned there, not here.
# On a cluster where the operators were installed by the scripts, either
# delete them first or `terraform import` the releases.

# The flink-operator namespace exists only for the operator, so Terraform
# owns it (unlike streaming, which carries app workloads and quotas).
resource "kubernetes_namespace" "flink_operator" {
  metadata {
    name = "flink-operator"
    labels = {
      "app.kubernetes.io/part-of" = "real-time-lakehouse"
      "layer"                     = "processing"
    }
  }
}

resource "helm_release" "strimzi" {
  name       = "strimzi-cluster-operator"
  repository = "https://strimzi.io/charts/"
  chart      = "strimzi-kafka-operator"
  version    = var.strimzi_version
  namespace  = "streaming"

  # Namespace-scoped, like the script install: the operator only watches
  # its own namespace, where all Kafka CRs live.
  values = [yamlencode({
    watchAnyNamespace = false
    resources = {
      requests = { memory = "256Mi", cpu = "100m" }
      limits   = { memory = "384Mi", cpu = "500m" }
    }
  })]

  timeout = 300
  wait    = true
}

resource "helm_release" "flink_operator" {
  name       = "flink-kubernetes-operator"
  repository = "https://downloads.apache.org/flink/flink-kubernetes-operator-${var.flink_operator_version}/"
  chart      = "flink-kubernetes-operator"
  version    = var.flink_operator_version
  namespace  = kubernetes_namespace.flink_operator.metadata[0].name

  # Same flags as the script install: no admission webhook (saves a pod),
  # default Flink configuration ConfigMap created, WSL2-sized resources.
  values = [yamlencode({
    webhook              = { create = false }
    defaultConfiguration = { create = true }
    resources = {
      requests = { memory = "256Mi", cpu = "100m" }
      limits   = { memory = "512Mi", cpu = "500m" }
    }
  })]

  timeout = 300
  wait    = true
}
