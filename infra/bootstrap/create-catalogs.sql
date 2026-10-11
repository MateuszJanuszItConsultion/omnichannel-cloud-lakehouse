-- Free Edition: catalogs on default storage cannot be created through the
-- Unity Catalog REST API (Terraform, Asset Bundles). Created here, read by Terraform.
CREATE CATALOG IF NOT EXISTS omnichannel_dev COMMENT 'Omnichannel retail lakehouse - dev';
CREATE CATALOG IF NOT EXISTS omnichannel_prd COMMENT 'Omnichannel retail lakehouse - prd';
