## ADR-0010: Out of scope — Lakebase serving layer, Iceberg interoperability

**Status:** Accepted

### Context
Two planned "small additions" were evaluated: Lakebase with synced tables as a serving layer (instead of FastAPI), and a managed Iceberg Gold table read locally from DuckDB.

### Decision
Both are out of scope.

**Iceberg / DuckDB.** External engines read Unity Catalog tables through the Iceberg REST catalog with credential vending. On Free Edition all schemas live on default storage, which does not support credential vending. The probe failed before any client was involved:

```
GRANT EXTERNAL USE SCHEMA ON SCHEMA retail_dev.bronze TO `<user>`;
-> INVALID_PARAMETER_VALUE.PRIVILEGE_NOT_APPLICABLE_TO_ENTITY
   Privilege EXTERNAL USE SCHEMA is not applicable to this entity
   [retail_dev.bronze:SCHEMA/SCHEMA_DB_STORAGE]
```

The original hypothesis predicted that metadata would be readable and the data read would fail. The failure happened earlier, at the privilege layer. The outcome (no external read) holds; the mechanism differs from the prediction. Compatibility Mode (requires an external location) and open Delta Sharing (requires account console) are also unavailable.

**Lakebase.** Available (one project, scale-to-zero), but with fully automatic matching (ADR-0006) there is no consumer for a serving layer. A serving layer without a consumer would be a checkbox, not a system.

### Revisit when
- Iceberg / DuckDB: on an account with custom storage locations.
- Lakebase: if a real consumer appears, e.g. a steward review UI or a Customer 360 lookup app.

### Open verification
- Optional second piece of evidence for the Iceberg decision: a direct call to the Iceberg REST table endpoint with the `vended-credentials` header.
