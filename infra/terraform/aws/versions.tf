# Terraform root for the CLOUD path — EKS + MSK + S3 (ADR-0010).
# PLAN-ONLY skeleton: validated in CI, never applied from this repo.
# No credentials are committed anywhere; running `plan` requires an AWS
# profile with real credentials, and `apply` would incur real costs
# (MSK + EKS ≈ several hundred USD/month).
terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}
