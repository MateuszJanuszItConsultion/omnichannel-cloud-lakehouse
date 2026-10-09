## ADR-0009: Local Spark as a second lens on the same jobs

**Status:** Accepted

### Context
Serverless hides the Spark UI and most configuration (ADR-0001). Diagnosing jobs from stages, tasks, spill and skew is a transferable skill that cannot be practiced on serverless alone.

### Decision
- The same Silver jobs run in two places: on Databricks serverless with the logical volume, and locally in Docker (classic Spark with Spark UI) with the performance volume.
- Silver code is a Python package in `src/`; jobs receive a Spark session from outside, so the code is identical in both environments.
- Local setup starts with a single container in `local[*]` mode. Workers are added only when a specific hypothesis requires them (executor memory, task distribution).
- Each optimization is diagnosed in both lenses: Spark UI locally, execution plans and query profile on Databricks. The comparison is a documented outcome.
- Gold (dbt) runs only on Databricks and is not replicated locally.
- This is a post-MVP module with a hard time box and pre-written hypotheses, same status as the Data Vault slice.

### Consequences
- The project stays one project with one codebase; the local path is a lens, not a separate world.
- Containers on one machine share CPU, memory and disk: shuffle does not cross a real network and spill hits the same disk. Results show mechanics, not production cost ratios, and are documented as such.
- Primary hotspots for diagnosis: blocking (skew on common surnames) and iterative connected components.

### Alternatives considered
- Performance testing only on serverless: rejected as the only path; too little visibility for learning diagnosis.
- Multi-worker cluster from the start: rejected; adds setup time without serving a specific hypothesis.

---
