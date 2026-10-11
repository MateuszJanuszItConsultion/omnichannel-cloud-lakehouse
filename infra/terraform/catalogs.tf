locals {
  environments = toset(["dev", "prd"])
}

# Catalogs are created by infra/bootstrap/create-catalogs.sql
data "databricks_catalog" "this" {
  for_each = local.environments
  name     = "omnichannel_${each.key}"
}
