# Databricks Sandbox - Infrastructure

AWS + Databricks Serverless infrastructure for POC testing.

## Databricks Details

| Item | Value |
|------|-------|
| Workspace URL | https://dbc-d026ccaf-decd.cloud.databricks.com |
| Account ID | 87e28a04-c41a-4a2f-8166-a47f8233bf2b |
| AWS Account | 560449670213 |
| Region | us-west-2 |
| Metastore | metastore_aws_us_west_2 |
| Catalog | workspace |

## Prerequisites

- AWS CLI with profile `ib` configured
- Terraform 1.5+
- Databricks workspace (already exists via AWS Marketplace)

```bash
aws sts get-caller-identity --profile ib  # verify AWS access
```

## Setup Steps

### Step 1: Bootstrap (Terraform State)

```bash
cd 00-bootstrap
terraform init && terraform apply
```

### Step 2: Foundation (S3 + IAM)

```bash
cd ../01-foundation
terraform init && terraform apply
```

Outputs you'll need:
- `landing_bucket`: databricks-sandbox-landing-560449670213
- `unity_catalog_role_arn`: arn:aws:iam::560449670213:role/databricks-sandbox-unity-catalog-role

### Step 3: Create Storage Credential (Databricks UI)

1. Go to **Catalog** → **External Data** → **Storage Credentials**
2. Click **Create credential**
3. Fill in:
   - Name: `databricks_sandbox_cred`
   - Type: AWS IAM role
   - Role ARN: `arn:aws:iam::560449670213:role/databricks-sandbox-unity-catalog-role`
4. Click **Create**
5. Click **Validate** to confirm it works

### Step 4: Create External Location (Databricks UI)

1. Go to **Catalog** → **External Data** → **External Locations**
2. Click **Create location**
3. Fill in:
   - Name: `landing`
   - URL: `s3://databricks-sandbox-landing-560449670213/`
   - Storage credential: `databricks_sandbox_cred`
4. Click **Create**

### Step 5: Upload Sample Data to S3

Sync each table to its own folder (supports incremental loads):

```bash
# Sync each table folder
aws s3 sync ./poc/dev_sample_data/ s3://databricks-sandbox-landing-560449670213/landing/ --profile ib

# Verify
aws s3 ls s3://databricks-sandbox-landing-560449670213/landing/ --profile ib --recursive
```

Structure:
```
s3://databricks-sandbox-landing-560449670213/landing/
├── Answer/
│   └── *.csv
├── Question/
│   └── *.csv
├── QuestionGroup/
│   └── *.csv
└── QuestionGroupResponse/
    └── *.csv
```

For incremental loads, just add new files to the local folders and re-run sync.

### Step 6: Create Schema + External Tables (Databricks SQL)

```sql
-- Create landing schema
CREATE SCHEMA IF NOT EXISTS workspace.landing;

-- Answer table (points to folder, reads all CSVs inside)
CREATE TABLE IF NOT EXISTS workspace.landing.answer
USING CSV
OPTIONS (header = 'false', inferSchema = 'true')
LOCATION 's3://databricks-sandbox-landing-560449670213/landing/Answer/';

-- Question table
CREATE TABLE IF NOT EXISTS workspace.landing.question
USING CSV
OPTIONS (header = 'false', inferSchema = 'true')
LOCATION 's3://databricks-sandbox-landing-560449670213/landing/Question/';

-- QuestionGroup table
CREATE TABLE IF NOT EXISTS workspace.landing.question_group
USING CSV
OPTIONS (header = 'false', inferSchema = 'true')
LOCATION 's3://databricks-sandbox-landing-560449670213/landing/QuestionGroup/';

-- QuestionGroupResponse table
CREATE TABLE IF NOT EXISTS workspace.landing.question_group_response
USING CSV
OPTIONS (header = 'false', inferSchema = 'true')
LOCATION 's3://databricks-sandbox-landing-560449670213/landing/QuestionGroupResponse/';
```

**Note:** External tables point to folders, so adding new CSV files to a folder automatically includes them in queries (for incremental loads).

### Step 7: Verify

```sql
SELECT count(*) FROM workspace.landing.answer;
SELECT count(*) FROM workspace.landing.question;
SELECT count(*) FROM workspace.landing.question_group;
SELECT count(*) FROM workspace.landing.question_group_response;
```

## Cleanup

To destroy everything:

```bash
# Drop tables in Databricks first (SQL)
DROP TABLE IF EXISTS workspace.landing.answer;
DROP TABLE IF EXISTS workspace.landing.question;
DROP TABLE IF EXISTS workspace.landing.question_group;
DROP TABLE IF EXISTS workspace.landing.question_group_response;

# Delete external location and storage credential via Databricks UI

# Then terraform
cd 01-foundation && terraform destroy
cd ../00-bootstrap && terraform destroy
```

## Next Actions

1. Confirm CSV headings or export query with column names from the source system.
2. Create proper DDL files for all four landing tables (start with `poc/code/answer.sql`).
3. Create Bronze tables as managed Databricks Delta tables built from landing.
