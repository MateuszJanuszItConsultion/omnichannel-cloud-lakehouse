from datetime import date

import pytest

from omnichannel_cloud_lakehouse.generator.truth_tables import ensure_bootstrap_allowed

DAY_ZERO = date(2026, 1, 1)


def test_bootstrap_allowed_on_empty_truth():
    ensure_bootstrap_allowed(None, DAY_ZERO)


def test_bootstrap_allowed_again_on_day_zero():
    ensure_bootstrap_allowed(DAY_ZERO, DAY_ZERO)


def test_bootstrap_refused_after_simulation_advanced():
    with pytest.raises(RuntimeError, match="already advanced"):
        ensure_bootstrap_allowed(date(2026, 1, 2), DAY_ZERO)
