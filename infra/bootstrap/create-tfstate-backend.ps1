# One-time bootstrap of the Azure Storage backend for Terraform state.
# Run once, manually, after `az login`. Not managed by Terraform itself
# (the backend must exist before Terraform can store state in it).

$ErrorActionPreference = "Stop"

$rg       = "rg-omnichannel-tfstate"
$sa       = "stomnichtfstatemjomnichl"   # 3-24 chars, lowercase letters/digits, globally unique
$location = "polandcentral"
$container = "tfstate"

az group create --name $rg --location $location

az storage account create `
  --name $sa `
  --resource-group $rg `
  --location $location `
  --sku Standard_LRS `
  --kind StorageV2 `
  --min-tls-version TLS1_2 `
  --allow-blob-public-access false

# Versioning + soft delete: a corrupted or deleted state can be recovered.
az storage account blob-service-properties update `
  --account-name $sa `
  --resource-group $rg `
  --enable-versioning true `
  --enable-delete-retention true `
  --delete-retention-days 7

# Data-plane access through Entra ID (no storage account keys).
$me    = az ad signed-in-user show --query id -o tsv
$scope = az storage account show --name $sa --resource-group $rg --query id -o tsv
az role assignment create --assignee $me --role "Storage Blob Data Contributor" --scope $scope

# Role assignments can take a few minutes to propagate.
# If this step fails with 403, wait and run it again.
az storage container create --name $container --account-name $sa --auth-mode login
