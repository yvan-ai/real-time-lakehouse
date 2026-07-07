output "strimzi_version" {
  description = "Deployed Strimzi operator chart version."
  value       = helm_release.strimzi.version
}

output "flink_operator_version" {
  description = "Deployed Flink Kubernetes operator chart version."
  value       = helm_release.flink_operator.version
}

output "flink_operator_namespace" {
  description = "Namespace owning the Flink Kubernetes operator."
  value       = kubernetes_namespace.flink_operator.metadata[0].name
}
