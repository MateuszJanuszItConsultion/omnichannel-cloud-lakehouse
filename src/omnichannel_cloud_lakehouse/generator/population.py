"""Day-zero population: households and persons with their TRUE attributes.

Truth is always clean. Typos, missing diacritics and format differences are
added later, when source files are rendered, never here.

v1 simplifications (deliberate, see ADR-0014):
- every member of a household shares the household surname and address,
- all persons are adult customers (18-85),
- no attribute history before day zero yet (valid_from = start of history).
"""

import math
from collections.abc import Mapping

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.config import GeneratorConfig
from omnichannel_cloud_lakehouse.generator.keyed_random import uniform
from omnichannel_cloud_lakehouse.generator.pools import Pool, pick

_PL_CHARS = "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ"
_ASCII_CHARS = "acelnoszzACELNOSZZ"

# Odd and not divisible by 5, i.e. coprime with any power of 10. Multiplying
# by such a number modulo 10**k is a bijection, so distinct ids give distinct,
# random-looking numbers without any uniqueness check (no shuffle needed).
_PHONE_MULTIPLIER = 73_939_133
_CARD_MULTIPLIER = 7_340_036_789


def fold_polish(col: Column) -> Column:
    """Replace Polish diacritics with ASCII letters (Łukasz -> Lukasz)."""
    return F.translate(col, _PL_CHARS, _ASCII_CHARS)


def email_token(col: Column) -> Column:
    """Lowercase ASCII token for an email local part.

    fold_polish maps Polish letters (Ł -> L) instead of dropping them; the regex
    is a safety net for anything else Faker may return, e.g. the space in
    the double first name "Anna Maria".
    """
    return F.regexp_replace(F.lower(fold_polish(col)), "[^a-z0-9-]", "")


def feminine_surname(col: Column) -> Column:
    """Kowalski -> Kowalska, Zawadzki -> Zawadzka; Nowak stays Nowak."""
    return F.regexp_replace(col, "ki$", "ka")


def bijective_digits(id_col: Column, multiplier: int, modulus: int) -> Column:
    """Map ids below `modulus` to distinct pseudo-random numbers in [0, modulus)."""
    return F.pmod(id_col * F.lit(multiplier), F.lit(modulus))


def _household_size(cfg: GeneratorConfig) -> Column:
    u = uniform("household_id", seed=cfg.seed, stream="household_size")
    expr = None
    cumulative = 0.0
    for index, p in enumerate(cfg.household_size_probs):
        cumulative += p
        expr = F.when(u < cumulative, index + 1) if expr is None else expr.when(u < cumulative, index + 1)
    return expr.otherwise(len(cfg.household_size_probs))


def _households(spark: SparkSession, cfg: GeneratorConfig, pools: Mapping[str, Pool]) -> DataFrame:
    n_households = math.ceil(cfg.n_persons / cfg.expected_household_size)
    df = spark.range(n_households).withColumnRenamed("id", "household_id")
    df = df.withColumn("household_size", _household_size(cfg))
    df = pick(df, pools["street"], "household_id", seed=cfg.seed, stream="street", alias="street")
    df = pick(df, pools["city"], "household_id", seed=cfg.seed, stream="city", alias="city")
    df = pick(df, pools["last_name_male"], "household_id", seed=cfg.seed, stream="surname", alias="household_surname")
    return df.select(
        "household_id",
        "household_size",
        "street",
        (F.floor(uniform("household_id", seed=cfg.seed, stream="house_no") * 150) + 1)
        .cast("string")
        .alias("house_number"),
        F.format_string(
            "%02d-%03d",
            F.floor(uniform("household_id", seed=cfg.seed, stream="postal_a") * 100),
            F.floor(uniform("household_id", seed=cfg.seed, stream="postal_b") * 1000),
        ).alias("postal_code"),
        "city",
        "household_surname",
    )


def _email(cfg: GeneratorConfig) -> tuple[Column, Column]:
    first = email_token(F.col("first_name"))
    last = email_token(F.col("last_name"))
    yy = F.date_format("birth_date", "yy")
    p = uniform("person_id", seed=cfg.seed, stream="email_pattern")
    local = (
        F.when(p < 0.4, F.concat_ws(".", first, last))
        .when(p < 0.7, F.concat(F.substring(first, 1, 1), last, yy))
        .otherwise(F.concat(first, last, yy))
    )
    domains = F.array(*[F.lit(d) for d in cfg.email_domains])
    domain_idx = F.floor(uniform("person_id", seed=cfg.seed, stream="email_domain") * len(cfg.email_domains)) + 1
    return local, F.element_at(domains, domain_idx.cast("int"))


def build_population(spark: SparkSession, cfg: GeneratorConfig, pools: Mapping[str, Pool]) -> DataFrame:
    households = _households(spark, cfg, pools)

    persons = (
        households.withColumn("member_no", F.explode(F.sequence(F.lit(1), F.col("household_size"))))
        # Composite id instead of row_number() over the whole dataset: a global
        # window would pull every row into a single partition.
        .withColumn("person_id", F.col("household_id") * 10 + F.col("member_no"))
        .withColumn(
            "gender",
            F.when(uniform("person_id", seed=cfg.seed, stream="gender") < 0.5, "F").otherwise("M"),
        )
        .withColumn(
            "last_name",
            F.when(F.col("gender") == "F", feminine_surname(F.col("household_surname"))).otherwise(
                F.col("household_surname")
            ),
        )
    )
    persons = pick(persons, pools["first_name_female"], "person_id", seed=cfg.seed, stream="fn", alias="_fn_f")
    persons = pick(persons, pools["first_name_male"], "person_id", seed=cfg.seed, stream="fn", alias="_fn_m")
    persons = persons.withColumn(
        "first_name", F.when(F.col("gender") == "F", F.col("_fn_f")).otherwise(F.col("_fn_m"))
    ).withColumn(
        "birth_date",
        F.date_sub(
            F.lit(cfg.day_zero),
            (F.lit(18 * 365) + F.floor(uniform("person_id", seed=cfg.seed, stream="age") * (67 * 365))).cast("int"),
        ),
    )

    # Email: name-based patterns collide for namesakes; duplicates get the
    # unique person_id appended, which guarantees uniqueness in the truth.
    local, domain = _email(cfg)
    persons = persons.withColumn("_local", local).withColumn("_domain", domain)
    by_email = Window.partitionBy("_local", "_domain").orderBy("person_id")
    with_id_suffix = F.concat(
        F.col("_local"), F.lit("."), F.col("person_id").cast("string"), F.lit("@"), F.col("_domain")
    )
    persons = persons.withColumn(
        "email",
        F.when(
            F.row_number().over(by_email) == 1,
            F.concat(F.col("_local"), F.lit("@"), F.col("_domain")),
        ).otherwise(with_id_suffix),
    )

    is_member = uniform("person_id", seed=cfg.seed, stream="loyalty_member") < cfg.loyalty_member_share
    persons = (
        persons.withColumn(
            "phone",
            F.concat(
                F.lit("+48"),
                (F.lit(5) + F.floor(uniform("person_id", seed=cfg.seed, stream="phone_prefix") * 4)).cast("string"),
                F.lpad(bijective_digits(F.col("person_id"), _PHONE_MULTIPLIER, 10**8).cast("string"), 8, "0"),
            ),
        )
        .withColumn("is_loyalty_member", is_member)
        .withColumn(
            "loyalty_card_no",
            F.when(
                is_member,
                F.concat(
                    F.lit("LC"),
                    F.lpad(bijective_digits(F.col("person_id"), _CARD_MULTIPLIER, 10**10).cast("string"), 10, "0"),
                ),
            ),
        )
        .withColumn(
            "loyalty_joined_on",
            F.when(
                is_member,
                F.date_sub(
                    F.lit(cfg.day_zero),
                    F.floor(uniform("person_id", seed=cfg.seed, stream="joined") * cfg.history_days).cast("int"),
                ),
            ),
        )
        .withColumn("valid_from", F.date_sub(F.lit(cfg.day_zero), cfg.history_days))
        .withColumn("valid_to", F.lit(None).cast("date"))
    )

    return persons.select(
        "person_id",
        "household_id",
        "gender",
        "first_name",
        "last_name",
        "birth_date",
        "email",
        "phone",
        "street",
        "house_number",
        "postal_code",
        "city",
        "is_loyalty_member",
        "loyalty_card_no",
        "loyalty_joined_on",
        "valid_from",
        "valid_to",
    )
