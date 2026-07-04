# ADR-0006 — GitOps deployment with ArgoCD and image-tag bumping

**Status**: Accepted · **Date**: 2026-07-04

## Context

Deployments must be reproducible and auditable. Two families of approaches:

- **Push-based**: CI runs `kubectl apply` with cluster credentials stored in GitHub.
- **Pull-based (GitOps)**: an in-cluster agent reconciles the cluster to the git state;
  no cluster credentials ever leave the cluster.

## Decision

Use **ArgoCD** with the repo as the single source of truth:

1. CI builds and pushes images to GHCR tagged `sha-<short>`.
2. The CD workflow runs `kustomize edit set image` and commits the new tags
   (`[skip ci]`) — the desired state lives in git, not in a CI variable.
3. ArgoCD (when configured via the `ARGOCD_SERVER` secret) syncs the `lakehouse-local`
   application; otherwise the sync steps skip gracefully so the pipeline stays green.

## Consequences

- `git log` on kustomization files is the deployment history; rollback = `git revert`.
- Cluster credentials never appear in CI; only an ArgoCD API token is needed for
  the optional explicit-sync step.
- The image-bump commit is authored by the CI bot — a deliberate, conventional
  exception in an otherwise human-authored history.
