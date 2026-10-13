import os
import sys

import pytest
from pyspark.sql import SparkSession

# Classic PySpark starts Python worker processes with the `python` command.
# On Windows that resolves to the Microsoft Store alias, not the venv.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """One local Spark session shared by the whole test run.

    Session scope: starting the JVM takes seconds, so it happens once.
    local[1]: a single thread keeps results deterministic.
    shuffle.partitions=1: the default (200) makes tiny test DataFrames slow.
    ui.enabled=false: no port conflicts, no wasted startup time.
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("omnichannel-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
