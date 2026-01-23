terraform {
  backend "s3" {
    bucket  = "databricks-sandbox-tfstate-560449670213"
    key     = "foundation/terraform.tfstate"
    region  = "us-west-2"
    profile = "ib"
  }
}
