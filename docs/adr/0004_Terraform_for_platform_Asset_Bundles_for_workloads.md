## ADR-0004: Terraform for platform, Asset Bundles for workloads

**Status:** Accepted — pending verification

### Context
Infrastructure as code from day one is a project requirement. Asset Bundles have moved to a direct deployment engine that talks to Databricks REST APIs and no longer depends on Terraform (per Databricks community guidance: default for new bundles since CLI 1.3.0; Terraform engine disabled in new CLI releases from September 2026). Free Edition exposes no account-level API, so Terraform can only manage workspace-level objects.

### Decision
- **Terraform (platform layer):** catalogs, schemas, volumes, grants, secret scopes.
- **Asset Bundles, direct engine (workload layer):** jobs, pipelines, dashboards, environment pinning, dev/prod targets.
- Catalogs are created only by Terraform. The sandbox catalog created manually during the probe is dropped, not imported.

### Consequences
- The two tools have a clean, non-overlapping boundary with no shared state.
- The Terraform footprint is intentionally small. If it grows beyond the platform layer, that is a signal to move the resource into the bundle.

### Open verification
- `databricks bundle deploy` of an empty job with the direct engine on the installed CLI version.

### Alternatives considered
- Asset Bundles only: viable, but loses an explicit, reviewable platform layer (grants, catalogs).
- Terraform only: rejected; bundles are the native and supported way to deploy jobs and pipelines.

---
