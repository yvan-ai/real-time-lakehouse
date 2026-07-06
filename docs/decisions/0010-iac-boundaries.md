# ADR-0010 — IaC boundaries: Terraform vs kustomize vs ArgoCD vs scripts

**Status**: Accepted · **Date**: 2026-07-07

## Context

Four mechanisms now touch the cluster: shell scripts (`scripts/*.sh`),
kustomize overlays (`infra/kubernetes/`), ArgoCD (`infra/argocd/`, pillar 6)
and Terraform (`infra/terraform/`, pillar 7). Without explicit ownership
rules they fight: two owners of the same resource means drift, failed
applies and, with ArgoCD auto-sync, silent tug-of-war.

## Decision

One owner per resource class:

| Layer | Owner | Examples |
|---|---|---|
| Cluster provisioning | **scripts** | `setup-k3s.sh` (k3s itself, `namespaces.yaml` first apply) |
| Cluster add-ons / operators | **Terraform** (`local/`) | Strimzi, Flink Kubernetes operator (Helm releases), `flink-operator` namespace |
| Cloud infrastructure | **Terraform** (`aws/`, plan-only) | EKS, MSK, S3, VPC |
| Application manifests | **kustomize, reconciled by ArgoCD** | everything in `overlays/local`: deployments, services, topics, quotas, DAG/dbt ConfigMaps |
| Secrets | **scripts + kustomize `secrets/`** | gitignored env files rendered by `secretGenerator`, applied by `bootstrap.sh`/`deploy.sh`, invisible to ArgoCD |
| Immutable / one-shot resources | **scripts** | `*-db-init` Jobs, `batch-pipeline` Job, script-refreshed ConfigMaps |

Rationale for the cuts:

- **Operators are infrastructure**: they are versioned, cluster-scoped-ish,
  upgraded rarely and deliberately — Terraform's plan/apply model fits;
  ArgoCD auto-sync does not (an operator upgrade should never ride along an
  app commit).
- **Namespaces + quotas stay in `namespaces.yaml`** (kustomize/kubectl),
  because they encode the WSL2 RAM budget that reviews alongside the app
  manifests. Terraform only owns `flink-operator`, a namespace that exists
  purely for an operator it installs. This is a deliberate narrowing of the
  roadmap's "namespaces via the kubernetes provider": two owners for the
  `streaming` namespace would guarantee conflicts.
- **ArgoCD reconciles, it does not install itself** — `install-argocd.sh`
  (script, not Terraform) so that a fresh bootstrap has no dependency cycle
  (Terraform → needs cluster → needs bootstrap → needs ArgoCD…).
- **`aws/` is plan-only**: CI runs `fmt`/`validate`/`tflint`; nothing in the
  repo can reach an AWS account (no credentials, no state backend).

`install-strimzi.sh` / `install-flink-operator.sh` remain as the
no-Terraform fallback; `bootstrap.sh` prefers Terraform when the CLI is
installed.

## Consequences

- A fresh cluster reaches operator-ready state with `terraform apply`
  (or the fallback scripts) — one command per layer, no overlap.
- Existing clusters where operators were script-installed must
  `terraform import` (or wait for the next rebuild) before the Terraform
  path manages them — flagged in `infra/terraform/README.md`.
- CI gains a `lint-terraform` job (`fmt -check`, `validate`, `tflint`);
  no plan/apply runs in CI by design.
- The `.tfstate` stays local and gitignored — acceptable for a
  single-operator repo, revisit (S3 backend) if anyone else applies.
