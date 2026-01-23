variable "project_name" {
  description = "Project name prefix"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks Account ID"
  type        = string
}

variable "credentials_id" {
  description = "Databricks credentials configuration ID"
  type        = string
}

variable "storage_configuration_id" {
  description = "Databricks storage configuration ID"
  type        = string
}

variable "unity_catalog_bucket_name" {
  description = "S3 bucket name for Unity Catalog"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}
