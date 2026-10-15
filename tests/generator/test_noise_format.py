import pytest
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator import noise_format as nf


def _apply(spark, value, fn):
    return spark.createDataFrame([(value,)], "v string").select(fn(F.col("v"))).first()[0]


@pytest.mark.parametrize(
    ("fn", "raw", "expected"),
    [
        pytest.param(nf.upper, "Jan Kowalski", "JAN KOWALSKI", id="upper"),
        pytest.param(nf.lower, "Jan Kowalski", "jan kowalski", id="lower"),
        pytest.param(nf.pad_whitespace, "Jan Kowalski", " Jan  Kowalski ", id="whitespace"),
        pytest.param(nf.strip_diacritics, "Wójcik Łódź", "Wojcik Lodz", id="diacritics"),
        pytest.param(nf.postal_code_without_dash, "00-950", "00950", id="postal-code"),
    ],
)
def test_simple_format_noise(spark, fn, raw, expected):
    assert _apply(spark, raw, fn) == expected


@pytest.mark.parametrize(
    ("variant", "expected"),
    [
        (0, "600123456"),
        (1, "+48600123456"),
        (2, "48600123456"),
        (3, "0048600123456"),
        (4, "(0) 600123456"),
        (5, "600 123 456"),
        (6, "600-123-456"),
        (7, "+48 600 123 456"),
    ],
)
def test_phone_variants(spark, variant, expected):
    row = (
        spark.createDataFrame([("+48600123456", variant)], "p string, v int")
        .select(nf.phone_variant(F.col("p"), F.col("v")))
        .first()
    )
    assert row[0] == expected


def test_every_phone_variant_reduces_to_the_same_digits(spark):
    # Property Silver standardization will rely on: keep digits, take the last 9.
    df = spark.range(nf.PHONE_VARIANT_COUNT).select(
        nf.phone_variant(F.lit("+48600123456"), F.col("id").cast("int")).alias("p")
    )
    national = df.select(F.substring(F.regexp_replace("p", r"\D", ""), -9, 9).alias("n")).distinct()
    assert [r.n for r in national.collect()] == ["600123456"]


@pytest.mark.parametrize(
    ("variant", "expected"),
    [(0, "ulica Ogrodowa"), (1, "ul. Ogrodowa"), (2, "ul Ogrodowa"), (3, "Ogrodowa")],
)
def test_street_prefix(spark, variant, expected):
    row = (
        spark.createDataFrame([("Ogrodowa", variant)], "s string, v int")
        .select(nf.street_with_prefix(F.col("s"), F.col("v")))
        .first()
    )
    assert row[0] == expected


def test_phone_variants_keep_null(spark):
    df = spark.range(nf.PHONE_VARIANT_COUNT).select(
        nf.phone_variant(F.lit(None).cast("string"), F.col("id").cast("int")).alias("p")
    )
    assert df.where(F.col("p").isNotNull()).count() == 0
