"""Healthcheck entry point.

Verifies wheel packaging, runtime versions and parameter passing.
Runs unchanged on Databricks serverless and on local classic Spark.
"""

import argparse
import sys
from importlib.metadata import PackageNotFoundError, version

from pyspark.sql import SparkSession

# Change this value (without bumping the package version) to test
# whether serverless reinstalls the wheel on redeploy.
BUILD_MARKER = "healthcheck-v1"


def _pkg_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not installed"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    print(f"build_marker={BUILD_MARKER}")
    print(f"package_version={_pkg_version('omnichannel-cloud-lakehouse')}")
    print(f"python={sys.version.split()[0]}")
    print(f"databricks_connect={_pkg_version('databricks-connect')}")
    print(f"pyspark={_pkg_version('pyspark')}")
    print(f"spark_server={spark.version}")
    print(f"catalog_param={args.catalog}")
    print(f"spark_range_count={spark.range(10).count()}")
