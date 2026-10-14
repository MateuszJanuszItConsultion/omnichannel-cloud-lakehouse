"""Format noise: variants that standardization in Silver must fully undo.

Every function is a pure Column -> Column transformation. Whether a variant is
applied, and which one, is decided elsewhere (change 4c) from the source
record key, so these functions stay simple and individually testable.

After standardization, a value with format noise must be identical to the
truth. If it is not, the bug is in Silver, not in the matching.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

from omnichannel_cloud_lakehouse.generator.population import fold_polish


def upper(col: Column) -> Column:
    return F.upper(col)


def lower(col: Column) -> Column:
    return F.lower(col)


def pad_whitespace(col: Column) -> Column:
    """'Jan Kowalski' -> ' Jan  Kowalski ' (leading, trailing and doubled inner spaces)."""
    return F.concat(F.lit(" "), F.regexp_replace(col, " ", "  "), F.lit(" "))


def strip_diacritics(col: Column) -> Column:
    """'Wójcik' -> 'Wojcik'. Same mapping as email tokens, applied to displayed values."""
    return fold_polish(col)


def postal_code_without_dash(col: Column) -> Column:
    """'00-950' -> '00950'."""
    return F.regexp_replace(col, "-", "")


def _phone_variants(col: Column) -> list[Column]:
    # Truth phones are '+48' followed by 9 national digits.
    n = F.substring(col, 4, 9)
    a, b, c = F.substring(n, 1, 3), F.substring(n, 4, 3), F.substring(n, 7, 3)
    return [
        n,  # 600123456
        F.concat(F.lit("+48"), n),  # +48600123456
        F.concat(F.lit("48"), n),  # 48600123456
        F.concat(F.lit("0048"), n),  # 0048600123456
        F.concat(F.lit("(0) "), n),  # (0) 600123456
        F.concat_ws(" ", a, b, c),  # 600 123 456
        F.concat_ws("-", a, b, c),  # 600-123-456
        F.concat_ws(" ", F.lit("+48"), a, b, c),  # +48 600 123 456
    ]


PHONE_VARIANT_COUNT = 8


def phone_variant(col: Column, variant: Column) -> Column:
    """Render a truth phone in one of PHONE_VARIANT_COUNT formats; `variant` is 0-based."""
    return F.element_at(F.array(*_phone_variants(col)), variant + 1)


STREET_PREFIXES = ("ulica ", "ul. ", "ul ", "")


def street_with_prefix(col: Column, variant: Column) -> Column:
    """'Ogrodowa' -> 'ulica Ogrodowa' / 'ul. Ogrodowa' / 'ul Ogrodowa' / 'Ogrodowa'."""
    prefixes = F.array(*[F.lit(p) for p in STREET_PREFIXES])
    return F.concat(F.element_at(prefixes, variant + 1), col)
