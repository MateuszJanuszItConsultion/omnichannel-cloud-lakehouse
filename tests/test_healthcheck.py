import sys

from omnichannel_cloud_lakehouse.jobs import healthcheck


def test_pkg_version_reports_missing_package():
    assert healthcheck._pkg_version("package-that-does-not-exist") == "not installed"


def test_main_receives_catalog_parameter(spark, monkeypatch, capsys):
    # Same argument format that python_wheel_task passes from named_parameters.
    # Test CI and local classic Spark both use this code path.
    monkeypatch.setattr(sys, "argv", ["healthcheck", "--catalog=omnichannel_test"])

    healthcheck.main()

    output = capsys.readouterr().out
    assert "catalog_param=omnichannel_test" in output
    assert "spark_range_count=10" in output
