from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.keyed_random import uniform


def test_values_are_in_unit_interval(spark):
    df = spark.range(10_000).withColumn("u", uniform("id", seed=42, stream="t"))

    stats = df.agg(F.min("u").alias("lo"), F.max("u").alias("hi")).first()

    assert stats.lo >= 0.0
    assert stats.hi < 1.0


def test_values_do_not_depend_on_partitioning(spark):
    base = spark.range(1_000)
    one = base.repartition(1).withColumn("u1", uniform("id", seed=42, stream="t"))
    many = base.repartition(7).withColumn("u7", uniform("id", seed=42, stream="t"))

    mismatches = one.join(many, "id").where(F.col("u1") != F.col("u7")).count()

    assert mismatches == 0


def test_streams_are_independent(spark):
    df = spark.range(1_000).select(
        uniform("id", seed=42, stream="a").alias("a"),
        uniform("id", seed=42, stream="b").alias("b"),
    )

    assert df.where(F.col("a") == F.col("b")).count() == 0


def test_distribution_is_roughly_uniform(spark):
    # Deterministic: the same seed always gives the same mean, so no flakiness.
    df = spark.range(100_000).select(uniform("id", seed=42, stream="t").alias("u"))

    mean = df.agg(F.avg("u")).first()[0]

    assert 0.49 < mean < 0.51
