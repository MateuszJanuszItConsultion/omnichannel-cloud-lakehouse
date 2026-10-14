# ADR-0011: Dev/prod environment separation from day one

**Status:** Accepted
**Date:** 2026-10-10

## Context
The project is new and has no code that depends on environment names yet.
Adding environment separation later would require reviewing every path, table
name and job parameter in PySpark code, dbt profiles and bundle definitions.
Free Edition offers one workspace and one metastore, so environments cannot be
separated by workspace.

## Decision
- Two Unity Catalog catalogs in the single workspace: `omnichannel_dev` and
  `omnichannel_prd`. Both have the same schemas: `landing`, `bronze`, `silver`,
  `gold`, `eval`, `truth`.
- Two Asset Bundle targets:
  - `dev`: `mode: development` (resources prefixed with the user name,
    schedules paused), default target;
  - `prod`: `mode: production`, with an explicit `workspace.root_path`.
- The catalog name exists in exactly one place: the bundle variable `catalog`,
  set per target and passed to every job as a named parameter. PySpark code
  never contains a hardcoded catalog name; dbt receives it through its target.
- The workspace host is not stored in `databricks.yml`. It is resolved from the
  Databricks CLI profile locally and from `DATABRICKS_HOST` / `DATABRICKS_TOKEN`
  in CI. The bundle can be deployed to any workspace without editing files.

## Consequences
- Dev and prod cannot be confused even in one workspace: resource names,
  schedules and catalogs differ by construction.
- Every new job must accept a `--catalog` parameter. This is verified by the
  healthcheck job and its entry-point test.
- Both environments share one metastore and one SQL warehouse: isolation is
  logical, not physical. Free Edition does not allow more.

## Alternatives considered
- Separation added later: rejected; retrofitting parameters across the codebase
  is exactly the kind of migration the project avoids.
- Schema prefixes in one catalog (`dev_bronze`, `prd_bronze`): rejected; catalog
  level separation maps directly to grants and to bundle targets.

## Evidence
- Healthcheck job on target `dev` printed `catalog_param=omnichannel_dev`.
