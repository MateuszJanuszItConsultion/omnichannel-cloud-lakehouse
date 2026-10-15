import pytest
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator import render
from omnichannel_cloud_lakehouse.generator.config import GeneratorConfig
from omnichannel_cloud_lakehouse.generator.pools import build_all_pools
from omnichannel_cloud_lakehouse.generator.population import build_population
from omnichannel_cloud_lakehouse.generator.source_records import build_source_records

CFG = GeneratorConfig(n_persons=2_000, pool_draws=300)
CHANNELS = ("loyalty", "webshop")


@pytest.fixture(scope="module")
def records(spark):
    population = build_population(spark, CFG, build_all_pools(spark, CFG.seed, CFG.pool_draws))
    return build_source_records(population, CFG)


@pytest.fixture(scope="module")
def noisy(records):
    return render.apply_channel_noise(records, CHANNELS, CFG.seed)


def test_noise_keeps_keys_and_row_count(records, noisy):
    keys = ["source_record_id", "channel", "channel_key", "person_id"]
    assert noisy.count() == records.count()
    assert noisy.select(*keys).exceptAll(records.select(*keys)).count() == 0


def test_both_channels_received_noise(noisy):
    with_noise = noisy.where(F.size("noise") > 0).groupBy("channel").count()
    assert {r.channel for r in with_noise.collect()} == set(CHANNELS)


def test_truth_keeps_mapping_and_flags_only(noisy):
    assert tuple(render.truth_source_records(noisy).columns) == render.TRUTH_COLUMNS


def test_loyalty_snapshot_looks_like_legacy_crm(noisy):
    df = render.render_loyalty_snapshot(noisy)
    assert df.columns[:3] == ["nr_karty", "imie", "nazwisko"]
    bad_dates = df.where(~F.col("data_przystapienia").rlike(r"^\d{2}\.\d{2}\.\d{4}$"))
    assert bad_dates.count() == 0


def test_webshop_customers_look_like_an_api_export(noisy):
    df = render.render_webshop_customers(noisy)
    assert "address" in df.columns and "firstName" in df.columns
    assert df.where(~F.col("createdAt").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")).count() == 0
    assert set(df.select("address.*").columns) == {"street", "houseNumber", "postalCode", "city"}


def test_rendered_rows_match_channel_counts(records, noisy):
    for channel, fn in (("loyalty", render.render_loyalty_snapshot), ("webshop", render.render_webshop_customers)):
        assert fn(noisy).count() == records.where(F.col("channel") == channel).count()
