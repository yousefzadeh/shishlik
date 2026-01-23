output "landing_bucket" {
  description = "S3 bucket for landing zone"
  value       = aws_s3_bucket.landing.id
}

output "landing_bucket_arn" {
  description = "ARN of landing bucket"
  value       = aws_s3_bucket.landing.arn
}

output "unity_catalog_bucket" {
  description = "S3 bucket for Unity Catalog"
  value       = aws_s3_bucket.unity_catalog.id
}

output "unity_catalog_bucket_arn" {
  description = "ARN of Unity Catalog bucket"
  value       = aws_s3_bucket.unity_catalog.arn
}

output "unity_catalog_role_arn" {
  description = "IAM role ARN for Databricks Unity Catalog"
  value       = aws_iam_role.databricks_unity_catalog.arn
}
