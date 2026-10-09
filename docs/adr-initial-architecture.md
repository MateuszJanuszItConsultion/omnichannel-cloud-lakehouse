# Architecture Decision Records — initial set

Project: omnichannel retail lakehouse on Databricks Free Edition
Date: 2026-10-09
Decider: Mateusz

Format per record: Status, Context, Decision, Consequences, Alternatives considered, Evidence / open verification.
Status values: `Accepted`, `Accepted — pending verification` (the decision stands, but one stated assumption still needs an empirical check).

| ADR | Title | Status |
|---|---|---|
| 0001 | Platform constraints of Databricks Free Edition | Accepted |
| 0002 | Responsibility split: PySpark in Silver, dbt in Gold | Accepted |
| 0003 | dbt v2 instead of dbt Core; `dbt lint` instead of sqlfluff | Accepted — pending verification |
| 0004 | Terraform for platform, Asset Bundles for workloads | Accepted — pending verification |
| 0005 | Stable golden customer identifier | Accepted |
| 0006 | Fully automatic matching, tuned for precision | Accepted |
| 0007 | Loyalty program as most trusted source for personal data, no anchor | Accepted |
| 0008 | Synthetic source data with ground truth, two volumes | Accepted |
| 0009 | Local Spark as a second lens on the same jobs | Accepted |
| 0010 | Out of scope: Lakebase serving layer, Iceberg interoperability | Accepted |

---