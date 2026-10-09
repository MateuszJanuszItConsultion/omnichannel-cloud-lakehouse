## ADR-0002: Responsibility split — PySpark in Silver, dbt in Gold

**Status:** Accepted

### Context
The medallion layers were fixed up front (Bronze raw, Silver integrated, Gold consumption). What needed a rule was the boundary between hand-written PySpark and dbt.

### Decision
The boundary follows the kind of logic, not the layer name:

- **Silver (PySpark):** anything that needs memory of previous runs or a non-relational algorithm: incremental matching, stable ID assignment, blocking, graph clustering, MERGE driven by CDF, sessionization.
- **Gold (dbt v2):** set-to-set transformations expressed in SQL for consumers, with contracts, tests and documentation.

Rule of thumb: if a model must know what it computed yesterday, it belongs in Silver. If it recomputes its result from current state, it belongs in Gold.

Layer content:

- **Bronze:** one append-only table per source entity, technical columns (ingestion time, source file, batch id), rescued-data column, CDF enabled on tables that feed Silver. Transactional channels via Auto Loader (`availableNow`); clickstream via Lakeflow Declarative Pipelines, stored as VARIANT.
- **Silver:** (1) cleaned and standardized source records, (2) customer identity domain (match attributes, candidate pairs, match decisions, connected components, `customer_xref`, merge/split event log, golden record), (3) unified transactions and web sessions keyed by source customer keys.
- **Gold:** dimensions (customer as SCD2 via dbt snapshot of the golden record, product, channel, store, date), sales fact at line grain across channels, Customer 360 mart, contracts on all public models.
- **Semantic layer:** Unity Catalog metric views on top of Gold.

### Consequences
- Survivorship (golden record) stays in Silver so that the whole identity domain lives in one place and in PySpark. It could be expressed in dbt; this is a deliberate boundary choice and may be revisited.
- LDP is used for exactly one flow (clickstream) to compare a declarative pipeline with hand-written Auto Loader on the same ingestion pattern. The identity path is not a good fit for LDP because it requires custom stateful logic.
- Graph clustering cannot use GraphFrames on serverless (requires a JAR), so connected components are implemented as iterative label propagation on DataFrames.

### Alternatives considered
- dbt for Silver as well: rejected; stateful incremental matching does not fit dbt's model.
- LDP for the whole pipeline: rejected; one pipeline per type in Free Edition, and identity resolution needs custom logic.

---