import pytest
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.config import GeneratorConfig
from omnichannel_cloud_lakehouse.generator.pools import build_all_pools
from omnichannel_cloud_lakehouse.generator.population import build_population
from omnichannel_cloud_lakehouse.generator.source_records import CAPTURE, build_source_records

CFG = GeneratorConfig(n_persons=3_000, pool_draws=300)


@pytest.fixture(scope="module")
def population(spark):
    return build_population(spark, CFG, build_all_pools(spark, CFG.seed, CFG.pool_draws))


@pytest.fixture(scope="module")
def records(population):
    return build_source_records(population, CFG)


def test_one_loyalty_record_per_member(population, records):
    members = population.where("is_loyalty_member").count()
    assert records.where(F.col("channel") == "loyalty").count() == members


def test_webshop_share_follows_config(population, records):
    share = records.where(F.col("channel") == "webshop").count() / population.count()
    assert abs(share - CFG.webshop_account_share) < 0.04


def test_source_record_id_unique(records):
    assert records.select("source_record_id").distinct().count() == records.count()


def test_every_record_points_to_a_person(population, records):
    orphans = records.join(population.select("person_id"), "person_id", "left_anti")
    assert orphans.count() == 0


def test_webshop_always_has_email_and_address(records):
    webshop = records.where(F.col("channel") == "webshop")
    missing = webshop.where(F.col("email").isNull() | F.col("street").isNull() | F.col("city").isNull())
    assert missing.count() == 0


@pytest.mark.parametrize(
    ("channel", "field"),
    [(ch, f) for ch, fields in CAPTURE.items() for f in fields],
)
def test_capture_rate_follows_config(records, channel, field):
    subset = records.where(F.col("channel") == channel)
    rate = subset.where(F.col(field).isNotNull()).count() / subset.count()
    assert abs(rate - CAPTURE[channel][field]) < 0.06


def test_created_on_not_after_day_zero(records):
    assert records.where(F.col("created_on") > F.lit(CFG.day_zero)).count() == 0


def test_source_records_are_deterministic(population, records):
    again = build_source_records(population, CFG)
    assert records.orderBy("source_record_id").collect() == again.orderBy("source_record_id").collect()
