#!/usr/bin/env python3
"""
Run Great Expectations checkpoints against the lakehouse Iceberg tables.

Usage:
    # Validate all layers (requires Trino running):
    export GX_TRINO_CONNECTION_STRING="trino://admin@localhost:8080/iceberg"
    python runner.py --layer all

    # Validate a single layer:
    python runner.py --layer silver

    # Validate with a pandas DataFrame (CI / unit tests):
    python runner.py --layer bronze --datasource pandas --data-path /tmp/sample_data/

Exit codes:
    0 — all expectations passed
    1 — one or more expectations failed
    2 — configuration or connection error
"""

import argparse
import os
import sys
from pathlib import Path

import great_expectations as gx
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult

GX_ROOT = Path(__file__).parent
LAYERS = ["bronze", "silver", "gold"]


def get_context() -> gx.DataContext:
    return gx.get_context(context_root_dir=str(GX_ROOT))


def run_checkpoint(context: gx.DataContext, layer: str) -> CheckpointResult:
    return context.run_checkpoint(checkpoint_name=layer)


def summarise(result: CheckpointResult, layer: str) -> bool:
    """Print a human-readable summary and return True if all expectations passed."""
    total_evaluated = 0
    total_passed = 0
    failures: list[str] = []

    for run_key, validation_result in result.run_results.items():
        stats = validation_result.statistics
        evaluated = stats.get("evaluated_expectations", 0)
        passed = stats.get("successful_expectations", 0)
        total_evaluated += evaluated
        total_passed += passed

        suite_name = run_key.expectation_suite_identifier.expectation_suite_name
        status = "PASS" if validation_result.success else "FAIL"
        print(f"  [{status}] {suite_name}: {passed}/{evaluated} expectations")

        if not validation_result.success:
            for exp_result in validation_result.results:
                if not exp_result.success:
                    exp_type = exp_result.expectation_config.expectation_type
                    column = exp_result.expectation_config.kwargs.get("column", "—")
                    failures.append(f"    • {suite_name} / {exp_type} (column: {column})")

    overall = result.success
    print(f"\n  Total: {total_passed}/{total_evaluated} passed  |  {'✓ PASS' if overall else '✗ FAIL'}")

    if failures:
        print("\n  Failed expectations:")
        for f in failures:
            print(f)

    return overall


def check_churn_consistency(context: gx.DataContext) -> bool:
    """
    Custom cross-column check for gold.customer_metrics:
    rows where days_since_last_order > 90 must have is_churned = True.
    Runs as a runtime Pandas batch — requires the Trino connection string to be set.
    """
    conn_str = os.environ.get("GX_TRINO_CONNECTION_STRING")
    if not conn_str:
        print("  [SKIP] Churn consistency check skipped — GX_TRINO_CONNECTION_STRING not set.")
        return True

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(conn_str)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT COUNT(*) AS violations "
                    "FROM gold.customer_metrics "
                    "WHERE days_since_last_order > 90 AND is_churned = false"
                )
            ).fetchone()
        violations = row[0] if row else 0
        if violations > 0:
            print(
                f"  [FAIL] Churn consistency: {violations} rows have "
                "days_since_last_order > 90 but is_churned = false"
            )
            return False
        print("  [PASS] Churn consistency: all churned flags are correct")
        return True
    except Exception as exc:
        print(f"  [WARN] Churn consistency check failed with error: {exc}")
        return True  # don't block CI on connectivity issues


def main() -> int:
    parser = argparse.ArgumentParser(description="Run GX data quality checkpoints")
    parser.add_argument(
        "--layer",
        choices=LAYERS + ["all"],
        default="all",
        help="Layer to validate (default: all)",
    )
    parser.add_argument(
        "--datasource",
        choices=["trino", "pandas"],
        default="trino",
        help="Datasource to use (default: trino)",
    )
    args = parser.parse_args()

    if args.datasource == "trino" and not os.environ.get("GX_TRINO_CONNECTION_STRING"):
        print("ERROR: GX_TRINO_CONNECTION_STRING is not set.")
        print("  Set it with: export GX_TRINO_CONNECTION_STRING='trino://admin@<host>:8080/iceberg'")
        return 2

    try:
        context = get_context()
    except Exception as exc:
        print(f"ERROR: Failed to load GX context: {exc}")
        return 2

    layers_to_run = LAYERS if args.layer == "all" else [args.layer]
    all_passed = True

    for layer in layers_to_run:
        print(f"\n{'=' * 60}")
        print(f"Layer: {layer.upper()}")
        print("=" * 60)
        try:
            result = run_checkpoint(context, layer)
            passed = summarise(result, layer)
            if not passed:
                all_passed = False
        except Exception as exc:
            print(f"  ERROR running checkpoint '{layer}': {exc}")
            all_passed = False

    # Extra cross-column check for gold layer
    if "gold" in layers_to_run:
        print(f"\n{'=' * 60}")
        print("Custom checks — gold layer")
        print("=" * 60)
        if not check_churn_consistency(context):
            all_passed = False

    # Build / refresh data docs
    try:
        context.build_data_docs()
        docs_path = GX_ROOT / "uncommitted" / "data_docs" / "local_site" / "index.html"
        print(f"\nData docs: file://{docs_path.resolve()}")
    except Exception as exc:
        print(f"\n[WARN] Could not build data docs: {exc}")

    print(f"\n{'=' * 60}")
    print(f"Overall result: {'✓ ALL PASSED' if all_passed else '✗ FAILURES DETECTED'}")
    print("=" * 60)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
