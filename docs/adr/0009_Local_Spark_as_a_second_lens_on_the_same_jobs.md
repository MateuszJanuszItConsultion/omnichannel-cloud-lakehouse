## ADR-0009: Local Spark as a second lens on the same jobs

**Status:** Accepted
**Updated:** 2026-10-14 (known differences between the lenses, rules for shared code)

### Context
Serverless hides the Spark UI and most configuration (ADR-0001). Diagnosing jobs from stages, tasks, spill and skew is a transferable skill that cannot be practiced on serverless alone.

### Decision
- The same Silver jobs run in two places: on Databricks serverless with the logical volume, and locally in Docker (classic Spark with Spark UI) with the performance volume.
- Silver code is a Python package in `src/`; jobs receive a Spark session from outside, so the code is identical in both environments.
- Local setup starts with a single container in `local[*]` mode. Workers are added only when a specific hypothesis requires them (executor memory, task distribution).
- Each optimization is diagnosed in both lenses: Spark UI locally, execution plans and query profile on Databricks. The comparison is a documented outcome.
- Gold (dbt) runs only on Databricks and is not replicated locally.
- This is a post-MVP module with a hard time box and pre-written hypotheses, same status as the Data Vault slice.

### Known differences between the lenses
The two lenses run the same code, but not the same runtime. Differences found so far, each observed empirically:

| Area | Local (classic PySpark) | Databricks serverless (Spark Connect) |
|---|---|---|
| Spark version | PySpark 4.1.3, driver and executors in one JVM | Client 4.1.0 (`databricks-connect` 18.1.9), server 4.2.0. The Python API available to the code is the client's |
| Data from Python (`createDataFrame` on a local list) | Rows are serialized in Python and read by a **Python worker process** started by the JVM | Data is sent to the server as **Arrow**; no local Python workers |
| Python worker interpreter | Started with the `python` command unless `PYSPARK_PYTHON` is set. On Windows `python` resolves to the Microsoft Store alias, so workers never start (`Python worker failed to connect back`) | Not applicable |
| `cache()` / `persist()` | Available. It also creates a plan boundary that blocks predicate pushdown | Not available (raises an error) |
| Analysis errors | Raised when the DataFrame is defined | Deferred until an action (Spark Connect resolves names on the server) |
| Delta Lake | Not built in; needs `delta-spark` matching Spark 4.1 | Built in |
| Writing files on Windows | Requires `winutils` / `HADOOP_HOME`; reading and in-memory work only log a warning | Not applicable |
| Java | Java 17 required, selected through `JAVA_HOME` in `.env` | Not applicable |

### Rules for code shared by both lenses
- No `cache()`, `persist()`, `checkpoint()`, RDD APIs or `sparkContext` in `src/`. They either fail on serverless or change the plan locally in a way serverless never sees.
- No `cache()` in tests either, unless the test is explicitly about caching: a cached DataFrame once hid a 2**N plan explosion that would have crashed on serverless (ADR-0014, plan-size rule).
- Only Spark configuration settable on serverless may be used in `src/`.
- Pure transformations are tested locally in memory; reading and writing stay in a thin I/O layer verified on Databricks, until local writes run in Docker.
- Local environment variables live in `.env`, with values containing spaces or backslashes in single quotes; `tests/conftest.py` sets `PYSPARK_PYTHON` to `sys.executable` so tests do not depend on it.

### Consequences
- The project stays one project with one codebase; the local path is a lens, not a separate world.
- Containers on one machine share CPU, memory and disk: shuffle does not cross a real network and spill hits the same disk. Results show mechanics, not production cost ratios, and are documented as such.
- Primary hotspots for diagnosis: blocking (skew on common surnames) and iterative connected components.
- A test passing locally is necessary but not sufficient: behaviour that depends on the differences above (deferred analysis, cache-dependent plans, Python workers) must also be checked on Databricks.
- Local Delta writes on Windows are impractical; they move to the Docker setup of this module.

### Alternatives considered
- Performance testing only on serverless: rejected as the only path; too little visibility for learning diagnosis.
- Multi-worker cluster from the start: rejected; adds setup time without serving a specific hypothesis.

### Evidence
- The healthcheck entry point ran unchanged locally (`spark_server=4.1.3`, `databricks_connect=not installed`) and on serverless (`spark_server=4.2.0`, `databricks_connect=18.1.9`).
- `keyed_random` tests passed locally while `pools` tests failed: the former use only JVM functions, the latter call `createDataFrame` on a Python list and need Python workers. Setting `PYSPARK_PYTHON` fixed all of them.
- With `cache()` in a test, a filter on the `noise` column planned normally; without it, the same filter produced a driver OOM (ADR-0014).

---
