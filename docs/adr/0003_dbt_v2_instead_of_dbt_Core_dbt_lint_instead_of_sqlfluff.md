## ADR-0003: dbt v2 instead of dbt Core; `dbt lint` instead of sqlfluff

**Status:** Accepted — pending verification

### Context
The original plan considered dbt Fusion (Databricks adapter in preview) versus dbt Core. In September 2026 dbt v2 (the Rust engine formerly known as Fusion) reached general availability for Databricks, for both local and platform use. dbt v2 is now the default install.

### Decision
- Use dbt v2 for the Gold layer.
- Replace sqlfluff in pre-commit with the built-in `dbt lint`, which is SQLFluff-compatible (keeps `.sqlfluff` config and rule codes).

### Consequences
- Stricter parsing: unknown configs, missing macros, missing variables and missing generic tests fail at parse time.
- Two distributions exist: `dbt` (dbt Labs license) and `dbt-oss` (Apache 2.0). Licensing differences are documented as part of contractor knowledge.
- Strict static analysis (column-level lineage) requires `dbt login`; without it the engine falls back to baseline analysis.
- dbt v2 ships as a binary using ADBC drivers, so it is not managed by the uv lockfile in the same way as dbt Core was.

### Open verification
- `dbt debug` against the Free Edition SQL warehouse.
- `dbt lint` produces equivalent results to sqlfluff on the project's rule set.
- Whether the dbt task in Lakeflow Jobs supports dbt v2; fallback: run dbt from a Python task.
- How metric views are deployed: through dbt or through Asset Bundles.

### Alternatives considered
- dbt Core 1.x: still supported, but new capabilities land in v2 only.

---