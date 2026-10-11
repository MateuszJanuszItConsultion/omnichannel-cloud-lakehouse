"""Deterministic pseudo-randomness derived from row keys.

`F.rand(seed)` depends on how rows are partitioned: the same data split
differently yields different numbers. Values derived from a hash of the row
key do not depend on partitioning, cluster size or row order, so a simulated
day can be regenerated bit-for-bit, locally and on Databricks.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

# Doubles represent every integer below 2**53 exactly, so the division
# below loses no precision.
_SCALE = 2**53


def uniform(*key_cols: Column | str, seed: int, stream: str) -> Column:
    """Return a value in [0, 1) that depends only on the key, seed and stream.

    `stream` names an independent random decision (e.g. "loyalty_member",
    "typo_last_name"). The same key with different streams gives unrelated
    values, so separate decisions about one person are not correlated.
    """
    hashed = F.xxhash64(*key_cols, F.lit(f"{seed}:{stream}"))
    return F.pmod(hashed, F.lit(_SCALE)) / F.lit(float(_SCALE))
