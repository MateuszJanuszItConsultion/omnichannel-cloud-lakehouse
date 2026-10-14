from dataclasses import fields

import pytest
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.noise import PROFILES, NoiseProfile, apply_noise

ROW = ("r", "Katarzyna", "Wójcik", "Łąkowa", "Łódź", "00-950", "+48600123456")
SCHEMA = "id bigint, first_name string, last_name string, street string, city string, postal_code string, phone string"
NOISY = ["first_name", "last_name", "street", "city", "postal_code", "phone"]


@pytest.fixture
def records(spark):
    rows = [(i, *ROW[1:]) for i in range(5_000)]
    return spark.createDataFrame(rows, SCHEMA).withColumn("source_record_id", F.concat(F.lit("r"), "id"))


def test_zero_profile_changes_nothing(records):
    out = apply_noise(records, NoiseProfile(), seed=42)
    assert out.where(F.size("noise") > 0).count() == 0
    assert out.select(*NOISY).distinct().count() == 1


def test_every_flag_matches_a_real_change(records):
    # No cache(): it blocks predicate pushdown and once hid a 2**N plan explosion.
    out = apply_noise(records, PROFILES["pos"], seed=42)
    # Rows without flags must be untouched; rows with flags must differ from the truth.
    clean = out.where(F.size("noise") == 0).select(*NOISY).distinct().collect()
    assert [tuple(r) for r in clean] in ([], [ROW[1:]])
    truth = F.lit("|".join(ROW[1:]))
    assert out.where((F.size("noise") > 0) & (F.concat_ws("|", *NOISY) == truth)).count() == 0


def test_last_name_typo_rate_follows_profile(records):
    profile = NoiseProfile(typo_name=0.08)
    out = apply_noise(records, profile, seed=42)
    rate = out.where(F.array_contains("noise", "last_name:typo")).count() / records.count()
    assert abs(rate - 0.08) < 0.015


def test_unknown_name_gets_no_diminutive_flag(spark):
    df = spark.createDataFrame(
        [("x", "Natasza", "Nowak", "Lipowa", "Gdańsk", "80-001", "+48600123456")],
        "source_record_id string, first_name string, last_name string, street string, "
        "city string, postal_code string, phone string",
    )
    out = apply_noise(df, NoiseProfile(diminutive=1.0), seed=42).first()
    assert out.first_name == "Natasza"
    assert out.noise == []


def test_noise_is_deterministic(records):
    a = apply_noise(records.repartition(1), PROFILES["webshop"], seed=42)
    b = apply_noise(records.repartition(4), PROFILES["webshop"], seed=42)
    assert a.orderBy("id").collect() == b.orderBy("id").collect()


@pytest.mark.parametrize("channel", sorted(PROFILES))
def test_profiles_are_valid_probabilities(channel):
    profile = PROFILES[channel]
    assert all(0.0 <= getattr(profile, f.name) <= 1.0 for f in fields(profile))
    assert profile.upper_case + profile.lower_case <= 1.0
    # Content noise on first_name uses disjoint ranges of one draw.
    assert profile.truncate_double_name + profile.diminutive + profile.typo_name <= 1.0


def test_filter_on_noise_keeps_plan_small(records):
    """Regression guard: filtering on `noise` used to explode the optimized plan (2**N)."""
    filtered = apply_noise(records, PROFILES["pos"], seed=42).where(F.size("noise") > 0)
    plan_chars = len(filtered._jdf.queryExecution().optimizedPlan().toString())
    print(f"plan_chars={plan_chars}")
    assert plan_chars < 2_000_000
