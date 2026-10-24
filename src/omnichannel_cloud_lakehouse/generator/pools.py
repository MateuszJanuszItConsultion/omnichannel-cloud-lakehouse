"""Value pools: Faker generates them once on the driver, Spark samples from them.

Faker is pure Python and works row by row. Calling it per row (UDF) would be
the bottleneck at millions of rows. Instead, a few thousand values are drawn
once, turned into a small DataFrame, and rows pick from it by a keyed random
index through a broadcast join.
"""

from collections.abc import Callable
from dataclasses import dataclass

from faker import Faker
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.keyed_random import uniform

# Polish surnames are gendered (Kowalski / Kowalska), so male and female
# pools are kept separate. Mixing them would create unrealistic people.
_DRAWS: dict[str, Callable[[Faker], str]] = {
    "first_name_male": lambda f: f.first_name_male(),
    "first_name_female": lambda f: f.first_name_female(),
    "last_name_male": lambda f: f.last_name_male(),
    "last_name_female": lambda f: f.last_name_female(),
    "street": lambda f: f.street_name(),
    "city": lambda f: f.city(),
}


@dataclass(frozen=True)
class Pool:
    df: DataFrame  # columns: idx int, value string
    size: int  # known on the driver, so sampling needs no Spark action


def _literal_frame(spark: SparkSession, values: list[str]) -> DataFrame:
    """Small reference data as a plan of literals: no Python worker ever reads it.

    spark.createDataFrame(list) would be read through a Python worker on every
    action in classic PySpark (slow on Windows, where workers are not reused).
    """
    if not values:
        raise ValueError("a pool needs at least one value")
    rows = [F.struct(F.lit(i).cast("int").alias("idx"), F.lit(v).alias("value")) for i, v in enumerate(values)]
    return spark.range(1).select(F.inline(F.array(*rows)))


def build_pool(spark: SparkSession, kind: str, seed: int, draws: int) -> Pool:
    fake = Faker("pl_PL")
    # Seeding per kind makes each pool independent of build order.
    fake.seed_instance(f"{seed}:{kind}")
    # dict.fromkeys removes duplicates and keeps first-seen order (deterministic).
    values = list(dict.fromkeys(_DRAWS[kind](fake) for _ in range(draws)))
    return Pool(df=_literal_frame(spark, values), size=len(values))


def pick(
    df: DataFrame,
    pool: Pool,
    *key_cols: Column | str,
    seed: int,
    stream: str,
    alias: str,
) -> DataFrame:
    """Add column `alias` with a value drawn from `pool`, determined by the key."""
    idx = F.floor(uniform(*key_cols, seed=seed, stream=stream) * pool.size).cast("int")
    lookup = pool.df.withColumnRenamed("idx", "_pool_idx").withColumnRenamed("value", alias)
    return df.withColumn("_pool_idx", idx).join(F.broadcast(lookup), "_pool_idx").drop("_pool_idx")


def build_all_pools(spark: SparkSession, seed: int, draws: int) -> dict[str, Pool]:
    return {kind: build_pool(spark, kind, seed, draws) for kind in _DRAWS}
