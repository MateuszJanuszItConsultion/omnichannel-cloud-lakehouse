"""Applying noise to source records according to a channel profile.

Decisions are drawn from the SOURCE RECORD key, not the person: the same person
in three channels gets three independent sets of distortions.

Every applied distortion is recorded in the `noise` array column, but only if
it actually changed the value (e.g. a diminutive for a name outside the
dictionary, or removing diacritics from 'Nowak', records nothing). Evaluation
can then measure matching quality per distortion type.

Order matters: content noise first (on clean values), format noise last,
as in reality: a cashier mistypes a name, the system then stores it uppercase.

Plan-size rule (learned the hard way, see ADR-0014): no column may refer to
its own previous version more than once. A filter on such a column is pushed
down through every projection by substituting its definition, which doubles
the expression per step (2**25 copies for 25 steps -> driver OOM). Therefore:
- content noise is ONE choice per field, computed on the clean value,
- format noise is a short, fixed chain per field,
- every flag is an independent column; `noise` is assembled once at the end.
"""

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator import noise_content as nc
from omnichannel_cloud_lakehouse.generator import noise_format as nf
from omnichannel_cloud_lakehouse.generator.keyed_random import uniform

KEY = "source_record_id"
NOISE = "noise"
NAME_AND_ADDRESS = ("first_name", "last_name", "street", "city")


@dataclass(frozen=True)
class NoiseProfile:
    """Probabilities per source record. Assumptions, not measurements: tune them."""

    truncate_double_name: float = 0.0
    diminutive: float = 0.0
    typo_name: float = 0.0
    typo_address: float = 0.0
    strip_diacritics: float = 0.0
    upper_case: float = 0.0
    lower_case: float = 0.0
    pad_whitespace: float = 0.0
    phone_variant: float = 0.0
    street_prefix: float = 0.0
    postal_without_dash: float = 0.0


PROFILES: dict[str, NoiseProfile] = {
    # Entered by a store advisor at enrollment.
    "loyalty": NoiseProfile(
        truncate_double_name=0.3, diminutive=0.03, typo_name=0.05, typo_address=0.05,
        strip_diacritics=0.10, upper_case=0.20, pad_whitespace=0.05,
        phone_variant=0.50, street_prefix=0.60, postal_without_dash=0.10,
    ),
    # Entered by a cashier at the till.
    "pos": NoiseProfile(
        truncate_double_name=0.3, diminutive=0.10, typo_name=0.08, typo_address=0.08,
        strip_diacritics=0.20, upper_case=0.40, pad_whitespace=0.10,
        phone_variant=0.50, street_prefix=0.60, postal_without_dash=0.20,
    ),
    # Entered by the customer.
    "webshop": NoiseProfile(
        truncate_double_name=0.5, diminutive=0.10, typo_name=0.01, typo_address=0.02,
        strip_diacritics=0.30, upper_case=0.05, lower_case=0.10, pad_whitespace=0.05,
        phone_variant=0.60, street_prefix=0.70, postal_without_dash=0.15,
    ),
}  # fmt: skip


def _content_noise(
    profile: NoiseProfile, u: Callable[[str], Column], typo_kind: Callable[[str], Column]
) -> tuple[dict[str, Column], list[Column]]:
    """One content distortion per field, chosen from disjoint probability ranges."""
    values: dict[str, Column] = {}
    flags: list[Column] = []
    for field in NAME_AND_ADDRESS:
        x = F.col(field)
        options: list[tuple[str, float, Callable[[Column], Column]]] = []
        if field == "first_name":
            options.append(("truncate", profile.truncate_double_name, nc.truncate_double_first_name))
            options.append(("diminutive", profile.diminutive, lambda c: nc.diminutive(c, u("diminutive_pick"))))
        typo_rate = profile.typo_name if field in ("first_name", "last_name") else profile.typo_address
        options.append(
            ("typo", typo_rate, lambda c, f=field: nc.typo(c, typo_kind(f"typo_kind_{f}"), u(f"typo_pos_{f}")))
        )

        r = u(f"content_{field}")
        low = 0.0
        case = None
        for label, prob, transform in options:
            chosen = (r >= F.lit(low)) & (r < F.lit(low + prob))
            low += prob
            y = transform(x)
            flags.append(F.when(chosen & (y != x), F.lit(f"{field}:{label}")))
            case = F.when(chosen, y) if case is None else case.when(chosen, y)
        values[field] = case.otherwise(x)
    return values, flags


def _format_noise(
    profile: NoiseProfile, u: Callable[[str], Column], hit: Callable[[str, float], Column]
) -> tuple[dict[str, Column], list[Column]]:
    """Short fixed chain per field: diacritics -> case -> whitespace. Record-level decisions."""
    values: dict[str, Column] = {}
    flags: list[Column] = []

    no_diacritics = hit("diacritics", profile.strip_diacritics)
    case_u = u("case")
    is_upper = case_u < F.lit(profile.upper_case)
    is_lower = (case_u >= F.lit(profile.upper_case)) & (case_u < F.lit(profile.upper_case + profile.lower_case))
    padded = hit("whitespace", profile.pad_whitespace)

    for field in NAME_AND_ADDRESS:
        y = F.col(field)
        stripped = nf.strip_diacritics(y)
        d = F.when(no_diacritics, stripped).otherwise(y)
        flags.append(F.when(no_diacritics & (stripped != y), F.lit(f"{field}:no_diacritics")))

        up, low = nf.upper(d), nf.lower(d)
        cased = F.when(is_upper, up).when(is_lower, low).otherwise(d)
        flags.append(F.when(is_upper & (up != d), F.lit(f"{field}:upper")))
        flags.append(F.when(is_lower & (low != d), F.lit(f"{field}:lower")))

        if field in ("first_name", "last_name"):
            values[field] = F.when(padded, nf.pad_whitespace(cased)).otherwise(cased)
            flags.append(F.when(padded, F.lit(f"{field}:whitespace")))
        else:
            values[field] = cased

    phone = F.col("phone")
    phone_new = nf.phone_variant(phone, F.floor(u("phone_pick") * nf.PHONE_VARIANT_COUNT).cast("int"))
    phone_hit = hit("phone", profile.phone_variant)
    values["phone"] = F.when(phone_hit, phone_new).otherwise(phone)
    flags.append(F.when(phone_hit & (phone_new != phone), F.lit("phone:format")))

    # Prefix goes on top of the street value from this stage (index 3 = empty prefix is never drawn).
    prefix_idx = F.floor(u("prefix_pick") * (len(nf.STREET_PREFIXES) - 1)).cast("int")
    prefix_hit = hit("prefix", profile.street_prefix)
    values["street"] = F.when(prefix_hit, nf.street_with_prefix(values["street"], prefix_idx)).otherwise(
        values["street"]
    )
    flags.append(F.when(prefix_hit, F.lit("street:prefix")))

    postal = F.col("postal_code")
    postal_new = nf.postal_code_without_dash(postal)
    postal_hit = hit("postal", profile.postal_without_dash)
    values["postal_code"] = F.when(postal_hit, postal_new).otherwise(postal)
    flags.append(F.when(postal_hit & (postal_new != postal), F.lit("postal_code:no_dash")))
    return values, flags


def apply_noise(df: DataFrame, profile: NoiseProfile, seed: int) -> DataFrame:
    """Distort name, address and phone columns of `df`; add the `noise` array column."""

    def u(stream: str) -> Column:
        return uniform(KEY, seed=seed, stream=stream)

    def hit(stream: str, rate: float) -> Column:
        return u(stream) < F.lit(rate)

    def typo_kind(stream: str) -> Column:
        return F.floor(u(stream) * nc.TYPO_KIND_COUNT).cast("int")

    # Stage 1: content noise on clean values. withColumns evaluates every
    # expression against the input row, so flags still see the clean value.
    content, content_flags = _content_noise(profile, u, typo_kind)
    flag_cols = {f"_nf_c{i}": flag for i, flag in enumerate(content_flags)}
    out = df.withColumns({**content, **flag_cols})

    # Stage 2: format noise on the stage-1 values.
    fmt, format_flags = _format_noise(profile, u, hit)
    fmt_flag_cols = {f"_nf_f{i}": flag for i, flag in enumerate(format_flags)}
    out = out.withColumns({**fmt, **fmt_flag_cols})

    # Stage 3: assemble flags once; nulls (distortion not applied) are dropped.
    all_flags = [*flag_cols, *fmt_flag_cols]
    return out.withColumn(NOISE, F.array_compact(F.array(*[F.col(c) for c in all_flags]))).drop(*all_flags)
