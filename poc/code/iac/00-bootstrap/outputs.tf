output "tfstate_bucket" {
  description = "S3 bucket for Terraform state"
  value       = aws_s3_bucket.tfstate.id
}

output "tfstate_bucket_arn" {
  description = "ARN of the Terraform state bucket"
  value       = aws_s3_bucket.tfstate.arn
}

output "aws_account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}
