"""Thin I/O layer for the truth schema. All generation logic stays pure elsewhere."""

from datetime import date

from pyspark.sql import DataFrame, SparkSession


def table(catalog: str, name: str) -> str:
    return f"{catalog}.truth.{name}"


def read_clock(spark: SparkSession, catalog: str) -> date | None:
    """Current simulated day, or None if the simulation was never bootstrapped."""
    name = table(catalog, "sim_clock")
    if not spark.catalog.tableExists(name):
        return None
    row = spark.table(name).select("sim_day").first()
    return row.sim_day if row else None


def ensure_bootstrap_allowed(sim_day: date | None, day_zero: date) -> None:
    """Bootstrap may run on an empty or freshly bootstrapped truth, never after day zero.

    Re-running on day zero is safe: the generator is deterministic, so the
    overwrite produces identical data. Running it after the simulation advanced
    would silently erase simulated history.
    """
    if sim_day is not None and sim_day > day_zero:
        raise RuntimeError(
            f"Simulation already advanced to {sim_day}; bootstrap would overwrite its history. "
            "Reset the truth schema explicitly before bootstrapping again."
        )


def write_bootstrap(spark: SparkSession, catalog: str, population: DataFrame, day_zero: date) -> None:
    # Person first, clock last: the clock works as a commit marker. If the person
    # write fails, the clock is unchanged and the job can simply be re-run.
    population.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table(catalog, "person"))
    spark.createDataFrame([(day_zero,)], "sim_day date").write.mode("overwrite").saveAsTable(
        table(catalog, "sim_clock")
    )
