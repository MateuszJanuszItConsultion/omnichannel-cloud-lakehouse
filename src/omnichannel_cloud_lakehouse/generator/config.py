"""Generator parameters.

Only parameters needed by the current stage live here. Noise rates and
channel-specific settings are added when the code that uses them is added.
"""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    n_persons: int = 50_000
    day_zero: date = date(2026, 1, 1)
    history_days: int = 365
    loyalty_member_share: float = 0.35
    webshop_account_share: float = 0.55
    pool_draws: int = 5_000
    # Probability of a household of size 1, 2, 3, 4 (index + 1).
    household_size_probs: tuple[float, ...] = (0.35, 0.30, 0.20, 0.15)
    email_domains: tuple[str, ...] = ("gmail.com", "wp.pl", "onet.pl", "o2.pl", "interia.pl")

    def __post_init__(self) -> None:
        if self.n_persons <= 0:
            raise ValueError("n_persons must be positive")
        if self.history_days < 0:
            raise ValueError("history_days must not be negative")
        if not 0.0 <= self.loyalty_member_share <= 1.0:
            raise ValueError("loyalty_member_share must be within [0, 1]")
        if not 0.0 <= self.webshop_account_share <= 1.0:
            raise ValueError("webshop_account_share must be within [0, 1]")
        if abs(sum(self.household_size_probs) - 1.0) > 1e-9:
            raise ValueError("household_size_probs must sum to 1")

    @property
    def expected_household_size(self) -> float:
        return sum((size + 1) * p for size, p in enumerate(self.household_size_probs))
