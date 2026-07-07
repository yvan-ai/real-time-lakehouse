variable "region" {
  description = "AWS region for the lakehouse stack."
  type        = string
  default     = "eu-west-3"
}

variable "project" {
  description = "Project slug used for resource names and tags."
  type        = string
  default     = "real-time-lakehouse"
}

variable "vpc_cidr" {
  description = "CIDR of the dedicated VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "azs" {
  description = "Availability zones (MSK needs at least two)."
  type        = list(string)
  default     = ["eu-west-3a", "eu-west-3b"]
}

variable "kafka_version" {
  description = "MSK Kafka version — mirrors the Strimzi cluster locally."
  type        = string
  default     = "3.7.x"
}

variable "eks_version" {
  description = "EKS control-plane version."
  type        = string
  default     = "1.31"
}
