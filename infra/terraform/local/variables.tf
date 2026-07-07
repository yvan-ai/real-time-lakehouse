variable "kubeconfig_path" {
  description = "Path to the kubeconfig for the local k3s cluster."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "kubeconfig context name (k3s default context is 'default')."
  type        = string
  default     = "default"
}

variable "strimzi_version" {
  description = "Strimzi Kafka operator chart version (matches scripts/install-strimzi.sh)."
  type        = string
  default     = "0.51.0"
}

variable "flink_operator_version" {
  description = <<-EOT
    Flink Kubernetes operator chart version. Matches the operator RUNNING in
    the cluster (migrated to 1.14.0 on 2026-07-06) — downgrading a live
    operator is unsupported, so never lower this on an existing cluster.
  EOT
  type        = string
  default     = "1.14.0"
}
