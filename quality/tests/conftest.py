"""Shared pytest fixtures for the lakehouse test suite.

Makes the Spark job modules importable (the spark-jobs directory is not a
package — jobs are mounted as flat files in the batch Job ConfigMap) and
provides a session-scoped local SparkSession.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SPARK_JOBS_DIR = REPO_ROOT / "pipelines" / "batch" / "spark-jobs"

sys.path.insert(0, str(SPARK_JOBS_DIR))
sys.path.insert(0, str(REPO_ROOT / "demo"))


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession — no Iceberg, Kafka or S3 dependencies needed."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
