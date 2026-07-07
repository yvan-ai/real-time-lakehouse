output "eks_cluster_endpoint" {
  description = "EKS API endpoint."
  value       = module.eks.cluster_endpoint
}

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap broker string (plaintext listener)."
  value       = aws_msk_cluster.lakehouse.bootstrap_brokers
}

output "warehouse_bucket" {
  description = "S3 bucket backing the Iceberg warehouse."
  value       = aws_s3_bucket.lakehouse.bucket
}
