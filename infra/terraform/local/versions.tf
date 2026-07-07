# Terraform root for the LOCAL k3s cluster (ADR-0010).
# Owns: operator Helm releases (Strimzi, Flink) + the flink-operator
# namespace. Everything else stays with kustomize/ArgoCD and the scripts.
terraform {
  required_version = ">= 1.7"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.16"
    }
  }
}
