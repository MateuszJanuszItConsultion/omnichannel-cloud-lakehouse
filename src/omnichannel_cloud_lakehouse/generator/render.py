"""Noise per channel and rendering of channel customer files (day zero).

Each channel looks like a different system, as in reality:
- loyalty: legacy CRM export, Polish column names, dates as dd.MM.yyyy,
  semicolon-separated CSV, full snapshot;
- webshop: modern API export, camelCase JSON with a nested address,
  ISO dates; missing values become missing keys.
Silver has to map both to one model; that mapping is part of the exercise.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.noise import PROFILES, apply_noise

TRUTH_COLUMNS = ("source_record_id", "channel", "channel_key", "person_id", "created_on", "noise")


def apply_channel_noise(records: DataFrame, channels: tuple[str, ...], seed: int) -> DataFrame:
    """Apply each channel's noise profile to its own records."""
    noisy = None
    for channel in channels:
        part = apply_noise(records.where(F.col("channel") == channel), PROFILES[channel], seed)
        noisy = part if noisy is None else noisy.unionByName(part)
    return noisy


def truth_source_records(noisy: DataFrame) -> DataFrame:
    """What the truth keeps: the mapping and the applied distortions, not the noisy values."""
    return noisy.select(*TRUTH_COLUMNS)


def render_loyalty_snapshot(noisy: DataFrame) -> DataFrame:
    return noisy.where(F.col("channel") == "loyalty").select(
        F.col("channel_key").alias("nr_karty"),
        F.col("first_name").alias("imie"),
        F.col("last_name").alias("nazwisko"),
        F.col("email").alias("email"),
        F.col("phone").alias("telefon"),
        F.date_format("birth_date", "dd.MM.yyyy").alias("data_urodzenia"),
        F.col("street").alias("ulica"),
        F.col("house_number").alias("nr_domu"),
        F.col("postal_code").alias("kod_pocztowy"),
        F.col("city").alias("miejscowosc"),
        F.date_format("created_on", "dd.MM.yyyy").alias("data_przystapienia"),
    )


def render_webshop_customers(noisy: DataFrame) -> DataFrame:
    return noisy.where(F.col("channel") == "webshop").select(
        F.col("channel_key").alias("customerId"),
        F.col("email"),
        F.col("first_name").alias("firstName"),
        F.col("last_name").alias("lastName"),
        F.col("phone"),
        F.date_format("birth_date", "yyyy-MM-dd").alias("birthDate"),
        F.struct(
            F.col("street").alias("street"),
            F.col("house_number").alias("houseNumber"),
            F.col("postal_code").alias("postalCode"),
            F.col("city").alias("city"),
        ).alias("address"),
        F.date_format(F.col("created_on").cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("createdAt"),
    )
