# Terraform — infrastructure as code

Boundaries are defined in [ADR-0010](../../docs/decisions/0010-iac-boundaries.md):
Terraform owns cluster **add-ons and cloud infrastructure**; kustomize +
ArgoCD own the application manifests; shell scripts keep the imperative glue
(k3s provisioning, secret generation, immutable Jobs).

## `local/` — operators on the k3s cluster

Replaces `scripts/install-strimzi.sh` and `scripts/install-flink-operator.sh`
on fresh clusters (`bootstrap.sh` uses Terraform when the CLI is available and
falls back to the scripts otherwise):

```bash
cd infra/terraform/local
terraform init
terraform apply        # Strimzi + Flink Kubernetes operator, WSL2-sized
```

Prerequisite: `setup-k3s.sh` ran (namespaces + quotas applied). On a cluster
where the operators were installed by the scripts, `terraform import` the
releases first — or leave Terraform for the next rebuild.

## `aws/` — cloud path (plan-only)

EKS + MSK + S3 skeleton mirroring the local stack. **Never applied from this
repo**: CI only runs `fmt`/`validate`/`tflint`; a real `plan` needs AWS
credentials (none are committed) and an `apply` would cost several hundred
USD/month (MSK + EKS + NAT). The Kubernetes workloads (Trino, Flink, Spark,
Airflow, Nessie, Marquez) would keep deploying through ArgoCD
(`infra/argocd/apps/lakehouse-cloud.yaml`).

State stays local (`*.tfstate` is gitignored); add an S3/DynamoDB backend
before any team usage of the aws root.
