# AWS Provider - for creating infrastructure
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

# Databricks provider for account-level operations (creating workspaces, users, etc.)
# Uses DATABRICKS_HOST and DATABRICKS_TOKEN env vars, or configure explicitly
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
  # Auth: set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET env vars
  # Or use DATABRICKS_TOKEN for personal access token
}

# Databricks provider for workspace-level operations (creating clusters, notebooks, etc.)
# Configured after workspace is created
provider "databricks" {
  alias = "workspace"
  host  = module.databricks_workspace.workspace_url
  # Auth will use the same credentials as account provider
}
