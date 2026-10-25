from datetime import date

import pytest
from pyspark.sql.types import DateType

from omnichannel_cloud_lakehouse.generator.truth_tables import clock_frame, ensure_bootstrap_allowed

DAY_ZERO = date(2026, 1, 1)


def test_bootstrap_allowed_on_empty_truth():
    ensure_bootstrap_allowed(None, DAY_ZERO)


def test_bootstrap_allowed_again_on_day_zero():
    ensure_bootstrap_allowed(DAY_ZERO, DAY_ZERO)


def test_bootstrap_refused_after_simulation_advanced():
    with pytest.raises(RuntimeError, match="already advanced"):
        ensure_bootstrap_allowed(date(2026, 1, 2), DAY_ZERO)


def test_clock_frame_is_one_date_row_built_from_literals(spark):
    df = clock_frame(spark, DAY_ZERO)
    assert df.schema["sim_day"].dataType == DateType()
    assert [r.sim_day for r in df.collect()] == [DAY_ZERO]
    assert "LogicalRDD" not in df._jdf.queryExecution().optimizedPlan().toString()
