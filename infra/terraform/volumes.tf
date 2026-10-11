resource "databricks_volume" "inbound" {
  for_each = local.environments

  catalog_name = data.databricks_catalog.this[each.key].name
  schema_name  = databricks_schema.this["${each.key}_landing"].name
  name         = "inbound"
  volume_type  = "MANAGED"
  comment      = "Raw files landed by the source generator - ${each.key}"
}
