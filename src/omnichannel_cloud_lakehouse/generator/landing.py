"""Thin I/O layer for files landed in the inbound volume.

One directory per delivery. Overwrite mode makes re-running the same
simulated day idempotent: the generator is deterministic, so the files are identical.
"""

from datetime import date

from pyspark.sql import DataFrame


def inbound(catalog: str) -> str:
    return f"/Volumes/{catalog}/landing/inbound"


def write_loyalty_snapshot(df: DataFrame, catalog: str, day: date) -> str:
    path = f"{inbound(catalog)}/loyalty/members_{day:%Y%m%d}"
    df.write.mode("overwrite").option("header", True).option("sep", ";").csv(path)
    return path


def write_webshop_customers(df: DataFrame, catalog: str, day: date) -> str:
    path = f"{inbound(catalog)}/webshop/customers/customers_{day:%Y%m%d}"
    df.write.mode("overwrite").json(path)
    return path
