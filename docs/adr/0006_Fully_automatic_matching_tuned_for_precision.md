## ADR-0006: Fully automatic matching, tuned for precision

**Status:** Accepted

### Context
Grey-zone matches can be resolved automatically or through a data steward review queue. There will be no real stewards using an application built in this project.

### Decision
- Matching is fully automatic: deterministic rules first, AI-assisted scoring for the grey zone in a later stage.
- Thresholds are tuned for precision, not recall. A false merge (two different people combined) is far more costly than a missed match.
- Pairs that remain uncertain are not merged; they are stored in a candidates table with score and rationale.
- Every match decision records its source (`rule`, `ai`) and the rules version. The model leaves room for a future `manual` source without redesign.

### Consequences
- No review UI, so no consumer for a serving layer (see ADR-0010).
- Changing rules never silently reshuffles clusters: a rerun with a new rules version can be compared against the previous result (time travel) before it is applied.
- `ai_similarity` is documented as producing a relative score suitable for ranking. Its use as a threshold must be calibrated against ground truth (ADR-0008) before it is trusted.

### Alternatives considered
- Steward review queue: rejected for a portfolio without real users; the decision model keeps it addable later.

---
