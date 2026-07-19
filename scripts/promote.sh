#!/usr/bin/env bash
# promote.sh — move the release (image tags) one environment forward (ADR-0012).
#
#   ./scripts/promote.sh staging             # dev (bases) → overlays/staging
#   ./scripts/promote.sh prod                # overlays/staging → overlays/prod
#   ./scripts/promote.sh prod --sync         # + trigger the manual prod gate
#   ./scripts/promote.sh staging --dry-run   # show the change, revert it
#
# A promotion IS a git commit: the target overlay's `images:` block is set to
# the source environment's current tag (kustomize edit set image), committed
# and pushed. ArgoCD does the rest:
#   staging  auto-syncs (~3 min poll) and runs its PostSync smoke Job on the
#            promoted image — a red smoke turns the sync red.
#   prod     has NO automated sync policy: the commit leaves lakehouse-prod
#            OutOfSync until a human triggers the sync — the gate. ArgoCD
#            core has no API server, so the trigger is a patch of the
#            Application object (--sync does it, needs cluster access).
#
# The chain is strict: prod only ever receives the tag staging currently
# runs; staging only receives what dev (the bases) currently runs.
#
# Node gotcha (WSL2): GHCR pulls of the 750 MB spark-batch layer truncate on
# this machine — only promote tags whose image is already in the node's
# containerd store (any tag dev has run). Import runbook:
# docs/notes/2026-07-07-verification-live-roadmap-v2.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"
IMAGE_NAME="spark-batch"

usage() {
  grep '^#' "${BASH_SOURCE[0]}" | sed -n '2,12p' | sed 's/^# \{0,1\}//'
}

ENV_NAME="${1:-}"
if [[ $# -gt 0 ]]; then shift; fi
SYNC=false
DRY_RUN=false
for arg in "$@"; do
  case "${arg}" in
    --sync)    SYNC=true ;;
    --dry-run) DRY_RUN=true ;;
    *) echo "Unknown option: ${arg}" >&2; usage; exit 2 ;;
  esac
done

case "${ENV_NAME}" in
  staging)
    SOURCE_KUSTOMIZATION="${REPO_ROOT}/infra/kubernetes/base/spark/kustomization.yaml"
    SOURCE_LABEL="dev (base/spark)"
    ;;
  prod)
    SOURCE_KUSTOMIZATION="${REPO_ROOT}/infra/kubernetes/overlays/staging/kustomization.yaml"
    SOURCE_LABEL="staging"
    ;;
  *) usage; exit 2 ;;
esac
TARGET_DIR="${REPO_ROOT}/infra/kubernetes/overlays/${ENV_NAME}"
TARGET_KUSTOMIZATION="${TARGET_DIR}/kustomization.yaml"

command -v kustomize >/dev/null 2>&1 || {
  echo "ERROR: kustomize not found on PATH (required for 'edit set image')." >&2
  exit 1
}

# Read one field of the spark-batch entry in a kustomization `images:` block.
# The files are machine-written (kustomize edit), so the field order is stable.
read_image_field() {
  awk -v field="$2" '
    /name: '"${IMAGE_NAME}"'$/ { in_entry = 1; next }
    in_entry && $1 == field ":" { print $2; exit }
    in_entry && /name:/        { exit }
  ' "$1"
}

NEW_NAME="$(read_image_field "${SOURCE_KUSTOMIZATION}" newName)"
NEW_TAG="$(read_image_field "${SOURCE_KUSTOMIZATION}" newTag)"
if [[ -z "${NEW_NAME}" || -z "${NEW_TAG}" ]]; then
  echo "ERROR: could not read the ${IMAGE_NAME} image from ${SOURCE_KUSTOMIZATION}" >&2
  exit 1
fi
CURRENT_TAG="$(read_image_field "${TARGET_KUSTOMIZATION}" newTag)"

echo "Promoting ${ENV_NAME}: ${CURRENT_TAG:-<none>} → ${NEW_TAG}  (source: ${SOURCE_LABEL})"

if [[ "${CURRENT_TAG}" == "${NEW_TAG}" ]]; then
  echo "${ENV_NAME} already runs ${NEW_TAG} — nothing to promote."
else
  # Refuse to sweep unrelated staged changes into the promotion commit.
  if ! git -C "${REPO_ROOT}" diff --cached --quiet; then
    echo "ERROR: the git index already has staged changes — commit or unstage them first." >&2
    exit 1
  fi

  (cd "${TARGET_DIR}" && kustomize edit set image "${IMAGE_NAME}=${NEW_NAME}:${NEW_TAG}")
  git -C "${REPO_ROOT}" --no-pager diff -- "${TARGET_KUSTOMIZATION}"

  if [[ "${DRY_RUN}" == true ]]; then
    git -C "${REPO_ROOT}" checkout -- "${TARGET_KUSTOMIZATION}"
    echo "Dry run — change reverted, nothing committed."
    exit 0
  fi

  echo "NOTE: make sure ${NEW_NAME}:${NEW_TAG} is already in the node's containerd store"
  echo "      (any tag dev has run) — GHCR pulls of the big layer truncate on this machine."

  git -C "${REPO_ROOT}" add "${TARGET_KUSTOMIZATION}"
  git -C "${REPO_ROOT}" commit -m "chore(promote): ${ENV_NAME} → ${NEW_TAG} [skip ci]"
  git -C "${REPO_ROOT}" push origin main
fi

if [[ "${ENV_NAME}" == "staging" ]]; then
  if [[ "${SYNC}" == true ]]; then
    echo "(--sync ignored: staging auto-syncs from main, ~3 min poll)"
  fi
  echo "Watch it land:  kubectl get application lakehouse-staging -n argocd -w"
  exit 0
fi

# ── prod gate ────────────────────────────────────────────────────────────────
if [[ "${SYNC}" != true ]]; then
  cat <<EOF

prod is gated: lakehouse-prod has no automated sync policy. When ready:
  ./scripts/promote.sh prod --sync        # trigger the sync (cluster access)
  kubectl get application lakehouse-prod -n argocd -w
EOF
  exit 0
fi

command -v kubectl >/dev/null 2>&1 || {
  echo "ERROR: --sync needs kubectl + cluster access." >&2
  exit 1
}

echo "Refreshing lakehouse-prod and waiting for it to see the pushed revision..."
kubectl annotate application lakehouse-prod -n argocd \
  argocd.argoproj.io/refresh=normal --overwrite >/dev/null

# Wait for OutOfSync (a no-op promotion may legitimately stay Synced).
STATUS=""
for _ in $(seq 1 24); do
  STATUS="$(kubectl get application lakehouse-prod -n argocd \
    -o jsonpath='{.status.sync.status}' 2>/dev/null || true)"
  [[ "${STATUS}" == "OutOfSync" ]] && break
  sleep 5
done
if [[ "${STATUS}" != "OutOfSync" ]]; then
  echo "lakehouse-prod reports '${STATUS:-unknown}' — triggering the sync anyway (idempotent)."
fi

echo "Opening the gate: starting the sync operation on lakehouse-prod..."
kubectl patch application lakehouse-prod -n argocd --type merge -p \
  '{"operation":{"initiatedBy":{"username":"promote.sh"},"sync":{"prune":false}}}'

echo "Waiting for the sync operation (PostSync smoke included)..."
PHASE=""
for _ in $(seq 1 60); do
  PHASE="$(kubectl get application lakehouse-prod -n argocd \
    -o jsonpath='{.status.operationState.phase}' 2>/dev/null || true)"
  case "${PHASE}" in Succeeded|Failed|Error) break ;; esac
  sleep 10
done

HEALTH="$(kubectl get application lakehouse-prod -n argocd \
  -o jsonpath='{.status.health.status}' 2>/dev/null || true)"
echo "lakehouse-prod: operation=${PHASE:-timeout} health=${HEALTH}"
if [[ "${PHASE}" != "Succeeded" ]]; then
  echo "Inspect the smoke Job:  kubectl logs job/env-smoke -n lakehouse-prod" >&2
  exit 1
fi
echo "prod promotion complete — smoke verification green."
