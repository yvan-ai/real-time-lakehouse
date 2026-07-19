# ADR-0012 — Multi-environment promotion (dev → staging → prod) with ArgoCD ApplicationSets

**Status**: Accepted · **Date**: 2026-07-19 · Extends ADR-0006, refines ADR-0010

## Context

ADR-0006 established GitOps CD with a single application (`lakehouse-local`)
auto-syncing one environment. A real delivery chain needs *promotion*: a
release proves itself in dev, then staging, before a gated prod rollout —
without multiplying `Application` manifests by hand.

Hard constraint: everything runs on one WSL2 k3s node (~3.25 CPU / ~9 GB
allocatable, 82 % memory-requested). Three full copies of the platform
(Kafka + Flink + Trino + MinIO + … ≈ 5 GB requests each) are physically
impossible here.

## Decision

**One ApplicationSet, one parameter file per environment, asymmetric
materialisation.**

1. **App-of-apps bootstrap** — `infra/argocd/bootstrap/root.yaml` is the only
   hand-applied ArgoCD object. It reconciles `infra/argocd/control-plane/`
   (the `lakehouse` AppProject + the `lakehouse-envs` ApplicationSet).
2. **Git file generator** — the ApplicationSet reads
   `infra/argocd/envs/{dev,staging,prod}.yaml` and generates
   `lakehouse-dev/-staging/-prod`. Adding an environment (or pointing one at
   another cluster via a `server` field) is a one-file change. Environments
   are **folders on `main`**, not branches — no long-lived branch drift.
3. **Asymmetric materialisation (the RAM answer)** —
   - *dev* = the full platform (`overlays/dev` wraps `overlays/local`; the
     `local` name stays because scripts and the secrets kustomization apply
     it directly).
   - *staging/prod* = a **verification slice**: their own namespace + quota
     (steady state: zero pods) and a **PostSync smoke Job** that runs on the
     *promoted* spark-batch image, read-only against the shared data plane
     (Gold tables through Trino). The pod's image tag is the proof the
     promotion rolled out; a red smoke turns the sync red.
   On a multi-cluster setup the staging/prod overlays would include the full
   base list — the promotion mechanics would not change.
4. **Promotion = a git commit** (`scripts/promote.sh`, or the `Promote`
   workflow) — extends ADR-0006's "git log is the deployment history":
   - dev tags live in the **bases**, bumped by CD on every main build;
   - `promote.sh staging` copies the dev tag into `overlays/staging`
     (auto-sync applies it, smoke verifies it);
   - `promote.sh prod` copies **staging's** tag into `overlays/prod` — the
     chain is strict, prod never receives an unstaged release.
5. **Prod gate = no automated sync.** The promotion commit leaves
   `lakehouse-prod` OutOfSync until a human opens the gate. ArgoCD core has
   no API server, so `promote.sh prod --sync` triggers the operation by
   patching the `Application` object — no `argocd` CLI, no exposed API.
6. **Environment namespaces are GitOps-owned** (refines ADR-0010): the shared
   platform namespaces stay in `base/namespaces.yaml` (scripts apply them
   before ArgoCD exists), but `lakehouse-staging`/`lakehouse-prod` +
   quotas/limits live in the env overlays — they are part of the
   environment's desired state. `preserveResourcesOnDeletion` guards against
   an env-file deletion cascading into a namespace wipe.

## Alternatives rejected

- **Three full stacks** — does not fit in 16 GB (see above); a scaled-down
  copy of Kafka/Flink/Trino would still cost ~5 GB of requests per env.
- **Environment-per-branch** — permanent merge drift between branches;
  folder-per-env keeps one revision history and lets one commit touch one env.
- **Per-env data isolation (Nessie branches, per-env topics)** — the natural
  next step (`spark.sql.catalog.iceberg.ref` per environment) but a data-
  architecture project of its own; out of scope for the CD mechanics.
- **ApplicationSet progressive syncs (RollingSync)** — still alpha behind a
  controller flag in v2.13; the strict promote chain already orders rollouts.

## Consequences

- Rollback = `git revert` of a promotion commit (staging picks it up
  automatically; prod after the gate).
- The smoke slice depends on the shared Trino/warehouse: a broken dev data
  plane fails staging/prod smokes — acceptable coupling on one node, and
  exactly what the slice is meant to detect before prod.
- Node gotcha inherited from the runbook: only tags whose spark-batch image
  is already in the node's containerd store can be promoted (GHCR pulls of
  the 750 MB layer truncate on this machine) — `promote.sh` warns about it.
- The ApplicationSet controller (scaled to 0 since pillar 6) is back on; the
  `argocd` namespace quota grows to 640 Mi requests / 1536 Mi limits.
