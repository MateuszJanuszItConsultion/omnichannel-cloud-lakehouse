from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.pools import build_pool, pick


def test_pool_is_deterministic(spark):
    first = build_pool(spark, "last_name_female", seed=42, draws=500)
    second = build_pool(spark, "last_name_female", seed=42, draws=500)

    assert first.df.orderBy("idx").collect() == second.df.orderBy("idx").collect()


def test_pool_has_no_duplicates(spark):
    pool = build_pool(spark, "city", seed=42, draws=500)

    assert pool.df.select("value").distinct().count() == pool.size


def test_pick_keeps_every_row(spark):
    pool = build_pool(spark, "first_name_male", seed=42, draws=500)
    people = spark.range(1_000).withColumnRenamed("id", "person_id")

    picked = pick(people, pool, "person_id", seed=42, stream="first_name", alias="first_name")

    assert picked.count() == 1_000
    assert picked.where(F.col("first_name").isNull()).count() == 0


def test_pick_is_deterministic(spark):
    pool = build_pool(spark, "first_name_male", seed=42, draws=500)
    people = spark.range(100).withColumnRenamed("id", "person_id")

    a = pick(people.repartition(1), pool, "person_id", seed=42, stream="fn", alias="v")
    b = pick(people.repartition(5), pool, "person_id", seed=42, stream="fn", alias="v")

    assert a.orderBy("person_id").collect() == b.orderBy("person_id").collect()
