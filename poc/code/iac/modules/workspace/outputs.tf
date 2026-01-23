output "workspace_url" {
  description = "URL of the Databricks workspace"
  value       = databricks_mws_workspaces.this.workspace_url
}

output "workspace_id" {
  description = "ID of the Databricks workspace"
  value       = databricks_mws_workspaces.this.workspace_id
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}
