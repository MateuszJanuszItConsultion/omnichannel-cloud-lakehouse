import pytest
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator import noise_content as nc


def _typo(spark, value, kind, u):
    return (
        spark.createDataFrame([(value, kind, u)], "v string, k int, u double")
        .select(nc.typo(F.col("v"), F.col("k"), F.col("u")))
        .first()[0]
    )


@pytest.mark.parametrize(
    ("kind", "u", "expected"),
    [
        pytest.param(nc.TYPO_DELETE, 0.0, "Kwalski", id="delete-first-allowed"),
        pytest.param(nc.TYPO_DELETE, 0.999, "Kowalsk", id="delete-last"),
        pytest.param(nc.TYPO_DUPLICATE, 0.0, "Koowalski", id="duplicate"),
        pytest.param(nc.TYPO_TRANSPOSE, 0.0, "Kwoalski", id="transpose-first-allowed"),
        pytest.param(nc.TYPO_TRANSPOSE, 0.999, "Kowalsik", id="transpose-last"),
    ],
)
def test_typo_kinds(spark, kind, u, expected):
    assert _typo(spark, "Kowalski", kind, u) == expected


def test_typo_never_changes_first_character(spark):
    df = spark.range(300).select(
        (F.col("id") % nc.TYPO_KIND_COUNT).cast("int").alias("k"),
        (F.col("id") / 300.0).alias("u"),
    )
    out = df.select(nc.typo(F.lit("Kowalski"), F.col("k"), F.col("u")).alias("t"))
    assert out.where(~F.col("t").startswith("K")).count() == 0
    assert out.where(F.col("t") == "Kowalski").count() == 0  # every row really changed


def test_typo_leaves_short_values(spark):
    assert _typo(spark, "Ed", nc.TYPO_DELETE, 0.5) == "Ed"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("Anna Maria", "Anna"), ("Anna-Maria", "Anna"), ("Katarzyna", "Katarzyna")],
)
def test_truncate_double_first_name(spark, raw, expected):
    row = spark.createDataFrame([(raw,)], "v string").select(nc.truncate_double_first_name(F.col("v"))).first()
    assert row[0] == expected


def test_diminutive_known_name(spark):
    df = spark.range(100).select((F.col("id") / 100.0).alias("u"))
    out = {r[0] for r in df.select(nc.diminutive(F.lit("Katarzyna"), F.col("u"))).collect()}
    assert out == {"Kasia", "Kaśka"}  # both forms used, nothing else


def test_diminutive_unknown_name_unchanged(spark):
    row = spark.range(1).select(nc.diminutive(F.lit("Natasza"), F.lit(0.5))).first()
    assert row[0] == "Natasza"
