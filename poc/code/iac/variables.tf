variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "ap-southeast-2" # Sydney - close to you
}

variable "project_name" {
  description = "Project name prefix for resources"
  type        = string
  default     = "shishlik-poc"
}

variable "databricks_account_id" {
  description = "Databricks Account ID (from accounts.cloud.databricks.com)"
  type        = string
  sensitive   = true
}

# Tags applied to all resources
variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project     = "shishlik-poc"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}
