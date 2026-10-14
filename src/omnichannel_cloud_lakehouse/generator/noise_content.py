"""Content noise: changes that standardization cannot undo.

- typos: matching must tolerate them (or deliberately reject the pair),
- truncated double first names: partially recoverable by token comparison,
- diminutives: recoverable only with a reference dictionary.

Like format noise, these are pure Column -> Column functions. Randomness
(which kind, which position, which diminutive) arrives as Column arguments
derived from the source record key in change 4c.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

TYPO_DELETE = 0  # Kowalski -> Kwalski
TYPO_DUPLICATE = 1  # Kowalski -> Koowalski
TYPO_TRANSPOSE = 2  # Kowalski -> Kwoalski
TYPO_KIND_COUNT = 3


def _sub(col: Column, start: Column, length: Column) -> Column:
    # Column.substr accepts Column positions; F.substring needs Python ints.
    return col.substr(start, length)


def typo(col: Column, kind: Column, u: Column) -> Column:
    """Apply one typo of `kind` at a position derived from `u` in [0, 1).

    The first character is never touched: first-letter typos are rarer in real
    data and break blocking keys built on the initial. They deserve their own,
    explicitly rare noise (v2), not a side effect of this one.
    Values shorter than 3 characters are returned unchanged.
    """
    n = F.length(col)
    rest = n  # substr past the end is safe, so the remaining length is enough
    # Deletion and duplication: position 2..n. Transposition: 2..n-1 (needs a right neighbour).
    p = (F.lit(2) + F.floor(u * (n - 1))).cast("int")
    pt = (F.lit(2) + F.floor(u * (n - 2))).cast("int")
    one = F.lit(1)

    deleted = F.concat(_sub(col, one, p - 1), _sub(col, p + 1, rest))
    duplicated = F.concat(_sub(col, one, p), _sub(col, p, rest))
    transposed = F.concat(
        _sub(col, one, pt - 1),
        _sub(col, pt + 1, one),
        _sub(col, pt, one),
        _sub(col, pt + 2, rest),
    )

    changed = (
        F.when(kind == TYPO_DELETE, deleted)
        .when(kind == TYPO_DUPLICATE, duplicated)
        .when(kind == TYPO_TRANSPOSE, transposed)
    )
    return F.when(n < 3, col).otherwise(changed)


def truncate_double_first_name(col: Column) -> Column:
    """'Anna Maria' -> 'Anna', 'Anna-Maria' -> 'Anna'; single names unchanged."""
    return F.regexp_replace(col, r"[ -].*$", "")


# Generator "reality": how people actually shorten names. Deliberately fuller
# than the reference dictionary Silver will use, so that missing entries in
# Silver show up as measurable missed matches (ADR-0014).
DIMINUTIVES: dict[str, tuple[str, ...]] = {
    # female
    "Anna": ("Ania", "Anka"),
    "Katarzyna": ("Kasia", "Kaśka"),
    "Małgorzata": ("Gosia", "Małgosia", "Gośka"),
    "Agnieszka": ("Aga",),
    "Joanna": ("Asia", "Joasia"),
    "Magdalena": ("Magda", "Madzia"),
    "Aleksandra": ("Ola", "Olka"),
    "Barbara": ("Basia", "Baśka"),
    "Elżbieta": ("Ela", "Elka"),
    "Ewa": ("Ewka",),
    "Krystyna": ("Krysia",),
    "Monika": ("Monia",),
    "Dorota": ("Dorotka",),
    "Teresa": ("Tereska",),
    "Zofia": ("Zosia",),
    "Natalia": ("Natka",),
    "Jadwiga": ("Jadzia",),
    "Weronika": ("Wera", "Weronka"),
    # male
    "Krzysztof": ("Krzysiek", "Krzyś"),
    "Aleksander": ("Olek", "Alek"),
    "Tomasz": ("Tomek",),
    "Piotr": ("Piotrek",),
    "Paweł": ("Pawełek",),
    "Andrzej": ("Jędrek",),
    "Wojciech": ("Wojtek",),
    "Stanisław": ("Staszek", "Stasiek"),
    "Józef": ("Józek",),
    "Jan": ("Janek", "Jasiek"),
    "Jakub": ("Kuba",),
    "Mateusz": ("Mati",),
    "Grzegorz": ("Grzesiek", "Grześ"),
    "Kazimierz": ("Kazik",),
    "Zbigniew": ("Zbyszek",),
    "Władysław": ("Władek",),
    "Bartłomiej": ("Bartek",),
    "Maciej": ("Maciek",),
    "Dariusz": ("Darek",),
    "Mirosław": ("Mirek",),
    "Ryszard": ("Rysiek",),
    "Tadeusz": ("Tadek",),
    "Henryk": ("Heniek",),
}


def _diminutive_map() -> Column:
    pairs = []
    for name, forms in DIMINUTIVES.items():
        pairs.extend([F.lit(name), F.array(*[F.lit(f) for f in forms])])
    return F.create_map(*pairs)


def diminutive(col: Column, u: Column) -> Column:
    """Replace a formal first name with one of its diminutives; names not in the
    dictionary are returned unchanged. The map is a literal: no join, no shuffle."""
    forms = F.element_at(_diminutive_map(), col)  # null for unknown names
    idx = (F.floor(u * F.size(forms)) + 1).cast("int")
    return F.coalesce(F.element_at(forms, idx), col)
