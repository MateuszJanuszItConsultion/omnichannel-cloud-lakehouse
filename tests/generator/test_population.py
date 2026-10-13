import pytest
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.config import GeneratorConfig
from omnichannel_cloud_lakehouse.generator.pools import build_all_pools
from omnichannel_cloud_lakehouse.generator.population import (
    build_population,
    email_token,
    feminine_surname,
    fold_polish,
)

CFG = GeneratorConfig(n_persons=2_000, pool_draws=300)


@pytest.fixture(scope="module")
def population(spark):
    pools = build_all_pools(spark, CFG.seed, CFG.pool_draws)
    # cache() is test-only: it is not supported on serverless.
    return build_population(spark, CFG, pools).cache()


def test_fold_polish(spark):
    row = spark.createDataFrame([("Łukasz Wójcik Źdźbło",)], "s string").select(fold_polish(F.col("s"))).first()
    assert row[0] == "Lukasz Wojcik Zdzblo"


@pytest.mark.parametrize(
    ("male", "female"),
    [("Kowalski", "Kowalska"), ("Zawadzki", "Zawadzka"), ("Nowak", "Nowak")],
)
def test_feminine_surname(spark, male, female):
    row = spark.createDataFrame([(male,)], "s string").select(feminine_surname(F.col("s"))).first()
    assert row[0] == female


def test_person_count_close_to_target(population):
    assert abs(population.count() - CFG.n_persons) / CFG.n_persons < 0.05


def test_person_id_unique(population):
    assert population.select("person_id").distinct().count() == population.count()


def test_household_shares_address_and_surname(population):
    per_household = population.groupBy("household_id").agg(
        F.countDistinct("street", "house_number", "postal_code", "city").alias("addresses"),
        F.countDistinct(F.regexp_replace("last_name", "ka$", "ki")).alias("surnames"),
    )
    assert per_household.where((F.col("addresses") != 1) | (F.col("surnames") != 1)).count() == 0


def test_emails_unique_and_ascii(population):
    assert population.select("email").distinct().count() == population.count()
    assert population.where(~F.col("email").rlike(r"^[a-z0-9.]+@[a-z0-9.]+$")).count() == 0


@pytest.mark.parametrize(
    ("raw", "token"),
    [("Anna Maria", "annamaria"), ("Łukasz", "lukasz"), ("Nowak-Kowalska", "nowak-kowalska")],
)
def test_email_token(spark, raw, token):
    row = spark.createDataFrame([(raw,)], "s string").select(email_token(F.col("s"))).first()
    assert row[0] == token


def test_phones_unique_and_polish_mobile(population):
    assert population.select("phone").distinct().count() == population.count()
    assert population.where(~F.col("phone").rlike(r"^\+48[5-8]\d{8}$")).count() == 0


def test_loyalty_share_and_cards(population):
    members = population.where("is_loyalty_member")
    share = members.count() / population.count()
    assert abs(share - CFG.loyalty_member_share) < 0.04
    assert members.where(F.col("loyalty_card_no").isNull()).count() == 0
    assert population.where(~F.col("is_loyalty_member") & F.col("loyalty_card_no").isNotNull()).count() == 0
    assert members.select("loyalty_card_no").distinct().count() == members.count()


def test_population_is_deterministic(spark, population):
    pools = build_all_pools(spark, CFG.seed, CFG.pool_draws)
    again = build_population(spark, CFG, pools)
    assert population.orderBy("person_id").collect() == again.orderBy("person_id").collect()
