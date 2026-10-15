"""Source records: which person appears in which channel, under which key,
and which attributes the channel actually captured.

Still clean (truth values). Noise is applied afterwards with the channel's
profile (noise.apply_noise); rendering to files comes after that.

Data model: one person -> many source records, also within one channel
(e.g. two webshop accounts). In v1 within-channel duplicates are 0%
(ADR-0014), but nothing here assumes one record per person per channel.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.config import GeneratorConfig
from omnichannel_cloud_lakehouse.generator.keyed_random import uniform
from omnichannel_cloud_lakehouse.generator.population import bijective_digits

ATTRIBUTES = (
    "first_name",
    "last_name",
    "email",
    "phone",
    "birth_date",
    "street",
    "house_number",
    "postal_code",
    "city",
)

# Probability that a channel captured a given attribute (missing otherwise).
# Assumptions, not measurements. Attributes not listed are always captured.
CAPTURE: dict[str, dict[str, float]] = {
    # Enrollment form filled in by a store advisor.
    "loyalty": {"email": 0.70, "phone": 0.90, "birth_date": 0.80},
    # Customer account: email is the login, address is needed for delivery.
    "webshop": {"phone": 0.80, "birth_date": 0.30},
}

_WEBSHOP_MULTIPLIER = 48_271_009  # odd, not divisible by 5: bijective modulo 10**8


def _with_capture(df: DataFrame, channel: str, seed: int) -> DataFrame:
    """Null out attributes the channel did not capture, decided per source record."""
    captured: dict[str, Column] = {}
    for field, p in CAPTURE.get(channel, {}).items():
        keep = uniform("source_record_id", seed=seed, stream=f"capture_{field}") < F.lit(p)
        captured[field] = F.when(keep, F.col(field))
    return df.withColumns(captured) if captured else df


def _loyalty(population: DataFrame) -> DataFrame:
    return population.where("is_loyalty_member").select(
        F.lit("loyalty").alias("channel"),
        F.col("loyalty_card_no").alias("channel_key"),
        "person_id",
        F.col("loyalty_joined_on").alias("created_on"),
        *ATTRIBUTES,
    )


def _webshop(population: DataFrame, cfg: GeneratorConfig) -> DataFrame:
    has_account = uniform("person_id", seed=cfg.seed, stream="webshop_account") < F.lit(cfg.webshop_account_share)
    days_before = F.floor(uniform("person_id", seed=cfg.seed, stream="webshop_created") * cfg.history_days)
    return population.where(has_account).select(
        F.lit("webshop").alias("channel"),
        F.concat(
            F.lit("W"),
            F.lpad(bijective_digits(F.col("person_id"), _WEBSHOP_MULTIPLIER, 10**8).cast("string"), 8, "0"),
        ).alias("channel_key"),
        "person_id",
        F.date_sub(F.lit(cfg.day_zero), days_before.cast("int")).alias("created_on"),
        *ATTRIBUTES,
    )


def build_source_records(population: DataFrame, cfg: GeneratorConfig) -> DataFrame:
    """Customer records of loyalty and webshop as of day zero (clean values)."""
    parts = {"loyalty": _loyalty(population), "webshop": _webshop(population, cfg)}
    records = None
    for channel, df in parts.items():
        df = df.withColumn("source_record_id", F.concat_ws(":", "channel", "channel_key"))
        df = _with_capture(df, channel, cfg.seed)
        records = df if records is None else records.unionByName(df)
    return records.select("source_record_id", "channel", "channel_key", "person_id", "created_on", *ATTRIBUTES)
