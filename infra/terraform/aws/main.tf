# Cloud skeleton — the managed equivalents of the local stack:
#   k3s   → EKS          (module: terraform-aws-modules/eks)
#   Kafka → MSK          (Strimzi CRs become MSK config)
#   MinIO → S3           (same s3a:// warehouse layout)
# Nessie, Trino, Flink, Spark, Airflow, Marquez keep running as Kubernetes
# workloads — the kustomize overlays and ArgoCD apps stay the deployment path
# (infra/argocd/apps/lakehouse-cloud.yaml is the placeholder for it).

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.16"

  name = var.project
  cidr = var.vpc_cidr

  azs             = var.azs
  private_subnets = [for i, _ in var.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, _ in var.azs : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  # Single NAT gateway: cost over availability for a demo path.
  enable_nat_gateway = true
  single_nat_gateway = true
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.31"

  cluster_name    = var.project
  cluster_version = var.eks_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access = true

  eks_managed_node_groups = {
    default = {
      instance_types = ["t3.large"]
      min_size       = 2
      max_size       = 3
      desired_size   = 2
    }
  }
}

resource "aws_security_group" "msk" {
  name_prefix = "${var.project}-msk-"
  vpc_id      = module.vpc.vpc_id

  # Brokers reachable from the EKS nodes only.
  ingress {
    from_port       = 9092
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_cluster" "lakehouse" {
  cluster_name           = var.project
  kafka_version          = var.kafka_version
  number_of_broker_nodes = length(var.azs)

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 50
      }
    }
  }
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project}-warehouse"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
