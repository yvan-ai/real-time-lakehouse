"""Post-promotion smoke check — read-only verification of a promoted environment.

Runs inside the *promoted* spark-batch image as an ArgoCD PostSync hook
(smoke-job.yaml): a failing check turns the sync red, which is the signal not
to promote further. staging/prod share the dev data plane on this single-node
cluster (ADR-0012), so every check is read-only against Trino.

Python 3.8 stdlib only: the spark image ships 3.8, and pod egress truncates
large downloads on this cluster, so no pip at runtime.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

TRINO_URL = os.environ.get("TRINO_URL", "http://trino.lakehouse.svc.cluster.local:8080")
ENV_NAMESPACE = os.environ.get("LAKEHOUSE_ENV_NAMESPACE", "unknown")

# (name, statement, predicate on the single scalar result)
CHECKS = [
    (
        "gold.daily_revenue is non-empty",
        "SELECT count(*) FROM iceberg.gold.daily_revenue",
        lambda value: value > 0,
    ),
    (
        "gold.customer_metrics is non-empty",
        "SELECT count(*) FROM iceberg.gold.customer_metrics",
        lambda value: value > 0,
    ),
]


def run_query(sql):
    """Execute one statement over Trino's REST protocol and return the rows."""
    request = urllib.request.Request(
        TRINO_URL + "/v1/statement",
        data=sql.encode(),
        headers={"X-Trino-User": "env-smoke", "X-Trino-Source": ENV_NAMESPACE},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)

    rows = []
    while True:
        if payload.get("error"):
            raise RuntimeError(payload["error"].get("message", "unknown Trino error"))
        rows.extend(payload.get("data") or [])
        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows
        # Trino answers 503 while a result chunk is being produced — retry.
        for attempt in range(10):
            try:
                with urllib.request.urlopen(next_uri, timeout=30) as response:
                    payload = json.load(response)
                break
            except urllib.error.HTTPError as exc:
                if exc.code != 503 or attempt == 9:
                    raise
                time.sleep(1)


def wait_for_trino():
    """Trino warms up slowly on its 500m-CPU pod — poll before checking."""
    for attempt in range(12):
        try:
            run_query("SELECT 1")
            return True
        except Exception as exc:
            if attempt == 11:
                print(f"FATAL: Trino unreachable after 12 attempts: {exc}")
                return False
            print(f"Trino not ready ({exc}) — retrying in 10s [{attempt + 1}/12]")
            time.sleep(10)
    return False


def main():
    print(f"env-smoke [{ENV_NAMESPACE}] against {TRINO_URL}")
    if not wait_for_trino():
        return 1

    failed = 0
    for name, sql, predicate in CHECKS:
        try:
            value = run_query(sql)[0][0]
        except Exception as exc:
            print(f"FAIL {name} — {exc}")
            failed += 1
            continue
        if predicate(value):
            print(f"PASS {name} (value={value})")
        else:
            print(f"FAIL {name} (value={value})")
            failed += 1

    if failed:
        print(f"Smoke verification FAILED — {failed}/{len(CHECKS)} checks red.")
        return 1
    print(f"Smoke verification PASSED — {len(CHECKS)}/{len(CHECKS)} checks green.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
