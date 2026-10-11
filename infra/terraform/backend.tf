# Partial backend configuration: values come from backend.hcl
# (not committed), passed with `terraform init -backend-config=backend.hcl`.
terraform {
  backend "azurerm" {}
}
