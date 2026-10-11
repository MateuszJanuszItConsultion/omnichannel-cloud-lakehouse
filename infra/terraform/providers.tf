# Workspace-level provider. Host and credentials are resolved from the
# Databricks CLI profile (~/.databrickscfg) locally, or from
# DATABRICKS_HOST / DATABRICKS_TOKEN in CI. Nothing is hardcoded.
provider "databricks" {}
