# ADR-0014: Source data generator as a time-based simulation

**Status:** Accepted
**Date:** 2026-10-12 (updated 2026-10-14)

## Context
Matching quality must be measured exactly, which needs ground truth (ADR-0008).
Stable golden IDs (ADR-0005) and incremental matching can only be tested on a
population that persists and changes over time. Real projects start with an
existing customer base (initial load), followed by daily increments.

## Decision

### Simulation and rendering are separate
- **Simulation (truth):** the population and its changes, stored in Delta tables
  in the `truth` schema: `person` (true attributes, SCD2 validity), `sim_clock`
  (current simulated day), `source_record` (which source record belongs to which
  person, which distortions were applied).
- **Rendering (sources):** channel files with channel-specific formats and noise.
  Noise only ever touches rendered files, never the truth.

### Time
- Day 0 is a bootstrap of an existing base (households, loyalty members with past
  enrollment dates, short order history), then daily steps.
- `sim_clock` is written after `person`: it acts as a commit marker across tables.
- Bootstrap is allowed on an empty truth or on day 0 (deterministic overwrite);
  it is refused once the clock has advanced.

### Determinism
- Randomness is derived from row keys: `pmod(xxhash64(key, "seed:stream"), 2**53) / 2**53`.
  `F.rand(seed)` is not used because its values depend on partitioning.
- Independent decisions use independent named streams.
- Faker generates value pools once on the driver; Spark samples from them with a
  keyed index and a broadcast join. Faker is pinned to an exact version
  because its output changes between versions.

### Population (v1)
- Households share address and surname; female surnames derive from the male
  form (`ki$` -> `ka`). Composite `person_id = household_id * 10 + member_no`
  avoids a global `row_number()` window.
- Phones and loyalty card numbers: modular multiplication by a number coprime
  with 10**k (a bijection), unique without any shuffle.
- Emails from name patterns; namesake collisions resolved by a window and the
  person id suffix. Email tokens: Polish letters folded, then everything outside
  `[a-z0-9-]` removed (double first names such as "Anna Maria").
- v1 simplifications: all household members share the surname; all persons are
  adults; no attribute history before day 0; value pools are uniform (no name
  frequency distribution yet).

### Noise
- Three categories, recorded per source record:
  - **format** (case, whitespace, diacritics, phone formats, postal code,
    street prefix): Silver standardization must fully undo it;
  - **content** (typos, truncated double first names): matching must tolerate it;
  - **dictionary-recoverable** (diminutives): fixable only with reference data.
    The generator dictionary is deliberately fuller than the one Silver will use.
- Typos never touch the first character (first-letter typos break blocking keys
  and will be a separate, explicitly rare distortion).
- At most one content distortion per field per record (disjoint probability ranges).
- A distortion is recorded only if it actually changed the value.
- Channel profiles (assumptions, to be tuned): loyalty and POS data are entered
  by staff (more typos in names and addresses), webshop data by customers
  (more missing diacritics, fewer typos).

### Plan-size rule
No column may refer to its own previous version more than once. A filter on such
a column is pushed down through every projection by substituting its definition,
which doubles the expression per step. Flags are therefore independent columns,
assembled into `noise` once with `array_compact`.

### Scope and time box (updated 2026-10-14)
- Time box counted in working hours: 20-30 h for generator v1. About 10 h spent
  on steps 1-4 (estimate, time logging starts now).
- Checkpoint: if exit criteria are not met after 25 h in total, POS transactions
  move to v1.1 and the project proceeds to Bronze with webshop orders only.
- v1: customer records in 3 channels, daily step (new persons, moves, phone
  changes, loyalty enrollments, daily loyalty snapshot), minimal transactions
  with a small static product catalog.
- Moved to v1.1: 12-month order history on day 0 (v1: parameter, default 30 days);
  within-channel duplicates (data model supports one person -> many source
  records, share is 0% in v1); richer loyalty leave scenarios.

### Exit criteria for v1
1. `bootstrap` creates the truth and the files of 3 channels in `landing.inbound`.
2. `day` advances the clock by one day and produces increments and the loyalty
   snapshot; re-running the same day does not duplicate data.
3. Output is deterministic; tests are green.

## Consequences
- The generator is a small stateful system, not a script.
- Matching and survivorship quality can be measured per distortion type.
- The loyalty program is both the most trusted source (ADR-0007) and a source of
  staff typos: survivorship by source ranking will sometimes pick a typo. This is
  measured, then decided in stage 3.

## Evidence
- `xxhash64`-based values identical on serverless (Spark 4.2.0) and locally
  (PySpark 4.1.3), e.g. id 0 -> `0.9425847490273724`.
- Bootstrap twice: Delta versions 0 and 1 identical (`EXCEPT` in both directions).
- Guard: with the clock at 2026-01-02 the job failed before any write; history
  of `truth.person` still showed only versions 0 and 1.
- Plan size with a filter on `noise` before the fix (optimized plan characters):

  | step | without filter | with filter |
  |---|---|---|
  | no_diacritics | 5,162 | 71,261 |
  | upper | 5,399 | 164,329 |
  | lower | 5,639 | 350,235 |
  | whitespace | 5,953 | 721,923 |

  25 steps led to a driver OOM; tests took 106 s. After the fix: 9 tests in 17.8 s.
- `cache()` in one test blocked predicate pushdown and hid the problem; it is not
  available on serverless and was removed from tests.
