variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = "ib"
}

variable "project_name" {
  description = "Project name prefix for resources"
  type        = string
  default     = "databricks-sandbox"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "databricks_account_id" {
  description = "Databricks account ID (used as external ID for IAM trust)"
  type        = string
}
