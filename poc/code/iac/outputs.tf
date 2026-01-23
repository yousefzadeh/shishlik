output "workspace_url" {
  description = "URL of the Databricks workspace"
  value       = module.databricks_workspace.workspace_url
}

output "workspace_id" {
  description = "ID of the Databricks workspace"
  value       = module.databricks_workspace.workspace_id
}

output "unity_catalog_bucket" {
  description = "S3 bucket for Unity Catalog root storage"
  value       = aws_s3_bucket.unity_catalog_root.id
}

output "dbfs_bucket" {
  description = "S3 bucket for DBFS root storage"
  value       = aws_s3_bucket.dbfs_root.id
}
