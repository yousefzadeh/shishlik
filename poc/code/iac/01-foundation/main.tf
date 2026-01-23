terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

data "aws_caller_identity" "current" {}

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# S3 bucket for landing zone (raw data)
resource "aws_s3_bucket" "landing" {
  bucket = "${var.project_name}-landing-${data.aws_caller_identity.current.account_id}"
  tags   = local.tags
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket = aws_s3_bucket.landing.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 bucket for Unity Catalog managed storage
resource "aws_s3_bucket" "unity_catalog" {
  bucket = "${var.project_name}-unity-catalog-${data.aws_caller_identity.current.account_id}"
  tags   = local.tags
}

resource "aws_s3_bucket_versioning" "unity_catalog" {
  bucket = aws_s3_bucket.unity_catalog.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "unity_catalog" {
  bucket = aws_s3_bucket.unity_catalog.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM role for Databricks Unity Catalog
resource "aws_iam_role" "databricks_unity_catalog" {
  name = "${var.project_name}-unity-catalog-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = [
            "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
            "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-unity-catalog-role"
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      },
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/reza"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "databricks_unity_catalog" {
  name = "${var.project_name}-unity-catalog-policy"
  role = aws_iam_role.databricks_unity_catalog.id

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
          aws_s3_bucket.unity_catalog.arn,
          "${aws_s3_bucket.unity_catalog.arn}/*",
          aws_s3_bucket.landing.arn,
          "${aws_s3_bucket.landing.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Resource = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-unity-catalog-role"
      }
    ]
  })
}
