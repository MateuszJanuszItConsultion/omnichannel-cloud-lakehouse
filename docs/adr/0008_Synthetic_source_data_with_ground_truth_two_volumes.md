## ADR-0008: Synthetic source data with ground truth, two volumes

**Status:** Accepted

### Context
Realistic multi-channel data with deliberately inconsistent identifiers is not publicly available. Synthetic data, unlike real data, provides exact ground truth.

### Decision
- One generator written in PySpark (works under Spark Connect and classic Spark), parameterized by a scale factor.
- Channels and identity signals:
  - webshop: orders (JSON) with email and delivery address; clickstream events (JSON, stored as VARIANT), often anonymous before login;
  - marketplace: orders (CSV) with marketplace login, partly masked email, different date format;
  - stores: POS transactions (Parquet), loyalty card optional, mostly anonymous;
  - loyalty program: member master with updates over time;
  - payments: payment events with a hashed card token.
- Daily file batches land in a UC volume, including updates and late-arriving records.
- Products: webshop and marketplace catalogs with different SKUs, matched deterministically by EAN only. Product matching is not developed further.
- Ground truth (which source record belongs to which person, and each person's true current attributes) is written to a separate schema. The pipeline identity has no access to it; only the evaluation job does.
- Tunable parameters: share of loyalty members, share of anonymous POS transactions, share of orders shipped to a non-home address, share of relay marketplace emails, rate of attribute changes.
- Two volumes from the same code: a logical volume on Databricks (portfolio "production") and a performance volume locally (ADR-0009), with deliberate skew in clickstream events.

### Consequences
- Precision and recall of matching, and the accuracy of survivorship, are computed exactly by an evaluation job for every rules version, including the comparison between deterministic and AI-assisted matching.
- The ground truth schema doubles as a Unity Catalog permissions demonstration.
- Generator code must avoid RDD APIs and classic-only constructs.

---
