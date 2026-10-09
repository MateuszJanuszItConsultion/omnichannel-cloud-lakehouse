## ADR-0005: Stable golden customer identifier

**Status:** Accepted

### Context
The same customer appears across channels under different identifiers (email, loyalty card, marketplace login, payment token). The golden ID can either be recomputed from scratch on every run or be stable over time, as in an MDM hub.

### Decision
The golden customer ID is stable over time:

- `customer_xref` (source system, source id → golden id, with validity period) is the source of truth for identity.
- Matching is incremental: only new and changed source records (CDF from Bronze/Silver) are matched against existing clusters, and results are applied with MERGE.
- Golden IDs are generated independently of run order (no `row_number()`); re-running the same input yields the same IDs.
- Cluster merges and splits are recorded in an event log. On merge, the surviving ID is deterministic (rule to be fixed at implementation, default: oldest ID).
- Transactions in Silver store only the source customer key. The golden ID is attached in Gold at build time by joining the current `customer_xref`, so a merge re-points the full purchase history on the next `dbt build` without rewriting fact rows.

### Consequences
- Considerably more design and implementation work than full recomputation.
- Justifies incremental processing (CDF, MERGE) in Silver with real business reasons.
- The optional Data Vault slice (Raw Vault for the customer entity, post-MVP, hard 2–3 week time box) becomes a natural extension, because a same-as link with history records what Silver alone does not. It remains optional.

### Alternatives considered
- Full recomputation of clusters per run: simpler, but IDs can change between runs and the core business problem is avoided.

---
