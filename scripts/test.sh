#!/usr/bin/env bash
# test.sh — Run the same checks as CI, locally.
#
#   ./scripts/test.sh          # lint + typecheck + unit tests
#   ./scripts/test.sh --fast   # skip the (slower) PySpark unit tests
#
# Tools missing from the environment are skipped with a warning instead of
# failing, so the script stays useful on partial setups.

set -uo pipefail

FAST=false
[[ "${1:-}" == "--fast" ]] && FAST=true

FAILURES=0

run_step() {
  local name="$1"; shift
  if ! command -v "$1" &>/dev/null; then
    echo "[SKIP] ${name} — '$1' not installed"
    return 0
  fi
  echo ""
  echo "── ${name} ──────────────────────────────────────────────"
  if "$@"; then
    echo "[PASS] ${name}"
  else
    echo "[FAIL] ${name}"
    FAILURES=$((FAILURES + 1))
  fi
}

run_step "ruff lint"         ruff check .
run_step "ruff format check" ruff format --check .
run_step "mypy typecheck"    mypy
run_step "yamllint"          yamllint -c .yamllint .

if [[ "${FAST}" == "false" ]]; then
  if python3 -c "import pyspark" &>/dev/null; then
    run_step "pytest" python3 -m pytest quality/tests -v
  else
    echo "[SKIP] pytest — pyspark not installed (pip install -r requirements-test.txt)"
  fi
fi

echo ""
if [[ "${FAILURES}" -gt 0 ]]; then
  echo "✗ ${FAILURES} check(s) failed"
  exit 1
fi
echo "✓ All checks passed"
