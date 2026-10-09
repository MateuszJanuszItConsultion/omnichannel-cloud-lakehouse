## ADR-0001: Platform constraints of Databricks Free Edition

**Status:** Accepted

### Context
The project runs on Databricks Free Edition. Before designing the architecture, every planned platform feature was verified with a hands-on probe in a disposable sandbox catalog (`retail_dev`, dropped after the probe).

### Decision
Design within the following verified constraints:

- Serverless compute only. Structured Streaming supports only `availableNow` / `once` triggers; the trigger must always be set explicitly.
- Max 5 concurrent job tasks per account; one active Lakeflow pipeline per pipeline type; one SQL warehouse (2X-Small); one Lakebase project.
- One workspace, one metastore, no account console, no account-level APIs.
- All data lives on Databricks default storage; custom storage locations are not supported.
- On serverless: no Spark UI (query profile only), no `cache()` / `persist()` / `checkpoint()`, only a handful of Spark confs are settable (e.g. `spark.sql.shuffle.partitions`, `spark.sql.files.maxPartitionBytes`). Only Spark Connect APIs.
- Column masks use `is_member()` (workspace groups). `is_account_group_member()` returns `false` for the workspace admin, so masks built on it would hide data from everyone.
- The serverless environment version is pinned explicitly for every job, and local `databricks-connect` is pinned to the same major version.
- Free Edition is non-commercial: it is used only for this portfolio, never for client work.

### Consequences
- The single 2X-Small warehouse is shared by dbt, metric views, dashboards and ad-hoc queries. It is the expected bottleneck, and cost/performance trade-offs will be measured on it.
- Job fan-out is designed to stay at or below 4 parallel tasks.
- Spark performance work on Databricks is limited to execution plans, query profile, data layout (liquid clustering, file sizes) and code-level fixes (salting, join hints). Configuration-level tuning is covered by ADR-0009.
- Inactive Free Edition accounts can be deleted, so everything must be reproducible from the repository (ADR-0004).

### Evidence (probe results)

| Feature | Result |
|---|---|
| Runtime (notebook) | Spark 4.2.0 (server), Python 3.12.3, `databricks-connect` 17.3.13 (client) |
| VARIANT (`parse_json`, path extraction) | Works |
| Delta CDF with MERGE | Works (insert observed) |
| CDF on a table with a column mask | Works; the mask is applied to `table_changes()` output |
| Liquid clustering DDL | Works (effect not yet measured) |
| `system.billing.usage`, `system.lakeflow.jobs` | Readable |
| Column mask | Works; `is_account_group_member('admins')` = false, `is_member('admins')` = true |
| Managed Iceberg table | Can be created; external read blocked (ADR-0010) |
| Metric view (`WITH METRICS`, YAML, `MEASURE()`) | Works from a notebook |
| `ai_similarity` | Works |
| Custom catalog | Can be created |

### Open verification
- Serverless environment version used by jobs vs. notebooks (client 17.3 suggests an environment older than version 5).
- CDF `update_preimage` / `update_postimage` after a second MERGE.
- `system.access.table_lineage` returns rows.
- Metric view query from the SQL warehouse (not only from a notebook).
- One LDP pipeline and one Auto Loader stream with `availableNow`.

---
