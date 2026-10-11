locals {
  # Start with one schema to verify that the API can create schemas inside
  # SQL-created catalogs. Extend the list once the hypothesis is confirmed.
  schemas = toset(["bronze"])

  catalog_schemas = {
    for pair in setproduct(local.environments, local.schemas) :
    "${pair[0]}_${pair[1]}" => {
      env    = pair[0]
      schema = pair[1]
    }
  }
}

resource "databricks_schema" "this" {
  for_each = local.catalog_schemas

  catalog_name = data.databricks_catalog.this[each.value.env].name
  name         = each.value.schema
  comment      = "${each.value.schema} layer - ${each.value.env}"
}
