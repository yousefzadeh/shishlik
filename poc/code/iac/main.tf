# ==============================================================================
# Databricks on AWS - POC Infrastructure
# ==============================================================================
# 
# This creates:
# 1. S3 bucket for Unity Catalog metastore root storage
# 2. IAM cross-account role for Databricks
# 3. Databricks workspace
# 4. Unity Catalog metastore (optional but recommended)
#
# Prerequisites:
# - AWS CLI configured with credentials
# - Databricks account created at accounts.cloud.databricks.com
# - Set env vars: DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET (or DATABRICKS_TOKEN)
# ==============================================================================

data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------------------
# S3 Bucket for Unity Catalog Root Storage
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "unity_catalog_root" {
  bucket = "${var.project_name}-unity-catalog-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name = "${var.project_name}-unity-catalog"
  }
}

resource "aws_s3_bucket_versioning" "unity_catalog_root" {
  bucket = aws_s3_bucket.unity_catalog_root.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "unity_catalog_root" {
  bucket = aws_s3_bucket.unity_catalog_root.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "unity_catalog_root" {
  bucket = aws_s3_bucket.unity_catalog_root.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------------------
# S3 Bucket for DBFS Root (workspace storage)
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "dbfs_root" {
  bucket = "${var.project_name}-dbfs-root-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name = "${var.project_name}-dbfs-root"
  }
}

resource "aws_s3_bucket_versioning" "dbfs_root" {
  bucket = aws_s3_bucket.dbfs_root.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dbfs_root" {
  bucket = aws_s3_bucket.dbfs_root.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "dbfs_root" {
  bucket = aws_s3_bucket.dbfs_root.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------------------
# IAM Role for Databricks Cross-Account Access
# ------------------------------------------------------------------------------
data "databricks_aws_assume_role_policy" "this" {
  provider    = databricks.account
  external_id = var.databricks_account_id
}

resource "aws_iam_role" "databricks_cross_account" {
  name               = "${var.project_name}-databricks-cross-account"
  assume_role_policy = data.databricks_aws_assume_role_policy.this.json

  tags = {
    Name = "${var.project_name}-databricks-cross-account"
  }
}

data "databricks_aws_crossaccount_policy" "this" {
  provider = databricks.account
}

resource "aws_iam_role_policy" "databricks_cross_account" {
  name   = "${var.project_name}-databricks-cross-account-policy"
  role   = aws_iam_role.databricks_cross_account.id
  policy = data.databricks_aws_crossaccount_policy.this.json
}

# Policy for S3 access (Unity Catalog + DBFS)
resource "aws_iam_role_policy" "databricks_s3_access" {
  name = "${var.project_name}-databricks-s3-access"
  role = aws_iam_role.databricks_cross_account.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.unity_catalog_root.arn,
          "${aws_s3_bucket.unity_catalog_root.arn}/*",
          aws_s3_bucket.dbfs_root.arn,
          "${aws_s3_bucket.dbfs_root.arn}/*"
        ]
      }
    ]
  })
}

# ------------------------------------------------------------------------------
# Databricks Credential Configuration (account-level)
# ------------------------------------------------------------------------------
resource "databricks_mws_credentials" "this" {
  provider         = databricks.account
  credentials_name = "${var.project_name}-credentials"
  role_arn         = aws_iam_role.databricks_cross_account.arn

  depends_on = [aws_iam_role_policy.databricks_cross_account]
}

# ------------------------------------------------------------------------------
# Databricks Storage Configuration (account-level)
# ------------------------------------------------------------------------------
resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.account
  storage_configuration_name = "${var.project_name}-storage"
  bucket_name                = aws_s3_bucket.dbfs_root.id
}

# ------------------------------------------------------------------------------
# VPC for Databricks (using module for simplicity)
# ------------------------------------------------------------------------------
module "databricks_workspace" {
  source = "./modules/workspace"

  project_name              = var.project_name
  aws_region                = var.aws_region
  databricks_account_id     = var.databricks_account_id
  credentials_id            = databricks_mws_credentials.this.credentials_id
  storage_configuration_id  = databricks_mws_storage_configurations.this.storage_configuration_id
  unity_catalog_bucket_name = aws_s3_bucket.unity_catalog_root.id

  providers = {
    aws                  = aws
    databricks.account   = databricks.account
  }
}
