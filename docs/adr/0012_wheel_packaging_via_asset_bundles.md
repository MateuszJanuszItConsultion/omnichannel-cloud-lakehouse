# ADR-0012: Wheel packaging via Asset Bundles

**Status:** Accepted
**Date:** 2026-10-10

## Context
Silver code is shared between Databricks serverless and local classic Spark
(ADR-0009), so it lives in a Python package under `src/`. Running job code as
plain files from Git requires manipulating `sys.path` for imports between
modules, the same family of problems as the missing `__file__` in Python script
jobs known from project 1.

## Decision
- The package is built as a wheel by the bundle (`artifacts` section,
  `uv build --wheel`) and installed into each job's serverless environment
  (`environments[].spec.dependencies`).
- Jobs run as `python_wheel_task`, pointing to entry points declared in
  `[project.scripts]`. Parameters arrive as named parameters
  (`--catalog=...`) and are parsed with `argparse`.
- The serverless environment version is pinned explicitly in every job
  (`environment_version: "5"`).
- `[project] dependencies` never contains `pyspark`: the platform provides it,
  and installing it with the wheel would overwrite the platform version.
- Runtime dependencies that affect generated data are pinned to an exact
  version (`faker==40.43.0`), because serverless installs from the wheel
  metadata, not from `uv.lock`.
- Dependency paths in resource files are relative to the resource file
  (`../dist/*.whl`), not to `databricks.yml`.

## Consequences
- Imports work identically locally, in tests and on serverless; no `sys.path`.
- Local `databricks-connect` must match the client of the pinned environment
  version, not the Spark server version.
- No dynamic wheel versioning for now (see evidence); the healthcheck procedure
  (change a marker, deploy, run) remains the diagnostic if stale code is suspected.

## Alternatives considered
- Files from Git with `sys.path` manipulation: rejected (fragile imports).
- Notebooks as job tasks: rejected; harder to test and to share with local Spark.

## Evidence
- Deploy with CLI v1.14.1 uses the direct engine (state in `resources.json`,
  no `terraform/` directory in `.databricks/bundle/dev/`).
- Environment version 5: `python=3.12.3`, `databricks_connect=18.1.9`,
  `pyspark=4.1.0+databricks.connect.18.1.9`, `spark_server=4.2.0`.
  The Python API available to the code is the 4.1 client, not the 4.2 server.
- A changed `BUILD_MARKER` with an unchanged package version (`0.1.0`) was
  picked up after redeploy: the wheel was reinstalled. One observation, not a
  guarantee under all conditions (e.g. warm environments).
- `faker==40.43.0` from the wheel metadata was installed from PyPI on serverless.
- The task traceback shows how `python_wheel_task` works: Databricks reads the
  installed distribution's entry points via `importlib.metadata` and calls the
  matching function.
