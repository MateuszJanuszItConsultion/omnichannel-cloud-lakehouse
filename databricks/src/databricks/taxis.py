from pyspark.sql import DataFrame

from databricks.sdk.runtime import spark


def find_all_taxis() -> DataFrame:
    """Find all taxi data."""
    return spark.read.table("samples.nyctaxi.trips")
