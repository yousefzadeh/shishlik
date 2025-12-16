# 6clicks Engagement — Research & Ideas

## About 6clicks

6clicks is an Australian-founded GRC (Governance, Risk & Compliance) SaaS company. Their platform helps enterprises, government agencies, and advisory firms manage cyber risk, compliance, vendor risk, audits, and incidents. Key differentiators include an AI assistant called "Hailey" that automates control mapping and assessments, a "Hub & Spoke" architecture for federated deployments (useful for MSPs and multi-entity orgs), and a pre-built content library of compliance frameworks (ISO 27001, SOC 2, etc.). They're ISO 27001 and ISO 42001 certified themselves, and serve customers like Telstra, Flybuys, Freightways, and Liberty Steel. The product includes embedded analytics via Yellowfin, which is where the current performance pain originates — analytics queries hitting the production SQL Server database.

---

## Azure Analytics Stack — Translator's Guide

Since you know AWS, BigQuery, and Databricks well, here's how Azure maps:

### Azure Synapse Analytics

**What it is:** Microsoft's enterprise analytics service — think of it as "Redshift + Glue + some Spark, bundled together."

**Key concepts:**
- **Dedicated SQL Pools** — this is the classic MPP data warehouse (formerly called Azure SQL Data Warehouse). Like Redshift: you provision compute capacity, load data in, run SQL. Columnar storage, distribution keys, the works. T-SQL syntax.
- **Serverless SQL Pools** — query data in Azure Data Lake Storage directly using SQL, without loading it first. Similar to Redshift Spectrum or BigQuery's external tables. Pay per TB scanned.
- **Spark Pools** — managed Apache Spark for notebooks and data engineering. Like EMR or Databricks Spark, but integrated into Synapse Studio.
- **Pipelines** — built-in ETL/orchestration, basically Azure Data Factory embedded in Synapse. Like Glue or Airflow.
- **Synapse Studio** — unified web IDE for SQL, Spark, pipelines, and monitoring. Decent but not as polished as Databricks notebooks.

**How it compares:**

| Concept | AWS Equivalent | GCP Equivalent | Databricks Equivalent |
|---------|---------------|----------------|----------------------|
| Dedicated SQL Pool | Redshift | BigQuery (provisioned) | Databricks SQL Warehouse |
| Serverless SQL Pool | Redshift Spectrum / Athena | BigQuery (external tables) | — |
| Spark Pools | EMR / Glue Spark | Dataproc | Databricks clusters |
| Pipelines | Glue / Step Functions | Dataflow / Composer | Databricks Workflows |
| Storage | S3 | GCS | Delta Lake on cloud storage |
| Synapse Studio | — | — | Databricks workspace |

**Pricing model:** Dedicated pools = pay for provisioned capacity (DWUs). Serverless = pay per TB scanned. Spark = pay per node-hour. Can get expensive if you leave dedicated pools running.

**Strengths:**
- Native integration with Azure ecosystem (Azure AD, Key Vault, Power BI, etc.)
- Good if you're already Azure-native and want one vendor
- T-SQL familiarity for SQL Server teams (their team will like this)

**Weaknesses:**
- More complex than BigQuery's simplicity
- Spark experience is okay but not as good as Databricks
- Dedicated pools can be slow to scale up/down

---

### Microsoft Fabric (the newer option)

**What it is:** Microsoft's all-in-one analytics SaaS, launched 2024. Think of it as "Synapse + Power BI + Data Factory + OneLake, unified and simplified."

**Key difference from Synapse:**
- **SaaS vs PaaS** — Fabric is fully managed, Synapse requires more configuration
- **OneLake** — single unified data lake (like a managed Delta Lake for the whole org), no need to manage separate storage accounts
- **Simpler pricing** — capacity-based, one number to manage
- **Better Power BI integration** — if they use Power BI, Fabric is tightly coupled
- **Lower barrier** — more low-code/no-code friendly

**When to recommend Fabric over Synapse:**
- Smaller teams who want less infrastructure to manage
- Orgs already using Power BI heavily
- Simpler use cases, less need for deep Spark customization
- Teams who want to move fast without tuning DWUs

**When Synapse might still fit:**
- Need fine-grained control over compute/storage
- Complex Spark workloads
- Already invested in Synapse and working well

---

### Other Azure pieces you might encounter

| Service | What it does | AWS/GCP equivalent |
|---------|--------------|-------------------|
| Azure Data Factory | ETL/orchestration | Glue, Airflow, Dataflow |
| Azure Data Lake Storage Gen2 | Object storage with hierarchical namespace | S3, GCS |
| Azure Databricks | Databricks, hosted on Azure | ...Databricks |
| Power BI | BI/dashboards | QuickSight, Looker, Tableau |
| Azure SQL Database | Managed SQL Server (OLTP) | RDS, Cloud SQL |

---

## Pricing Breakdown

### Azure Synapse Pricing

Synapse has **multiple meters** — you pay separately for each component you use:

**1. Dedicated SQL Pools (the warehouse)**
- Priced in **DWUs (Data Warehouse Units)** — bundles of CPU, memory, and IO
- Smallest: DW100c (~$1.20/hr) → Largest: DW30000c (~$360/hr)
- **You pay while it's running**, even if idle. Can pause to stop billing.
- Reserved capacity (1-year commit) saves up to 65%
- Rough monthly cost: DW100c ≈ $870/month, DW500c ≈ $4,350/month (if running 24/7)

**2. Serverless SQL Pools (query data lake)**
- Pay per **TB of data scanned**: ~$5/TB
- Minimum 10MB charge per query
- Good for ad-hoc queries, bad for heavy repeated queries (costs add up)

**3. Spark Pools**
- Pay per **node-hour** based on node size
- Small nodes ~$0.20/hr, Large nodes ~$1.60/hr
- Auto-pause available (stops billing when idle)

**4. Pipelines (Data Factory)**
- Pay per **activity run** and **data movement**
- Orchestration: ~$1 per 1,000 activity runs
- Data movement: ~$0.25/DIU-hour

**5. Storage (ADLS Gen2)**
- ~$0.02/GB/month for hot tier
- Separate from compute billing

**Synapse cost gotcha:** If you leave a Dedicated SQL Pool running 24/7, it gets expensive fast. Teams often pause overnight or scale down when not in use.

---

### Microsoft Fabric Pricing

Fabric is **simpler** — one capacity-based model:

**Capacity Units (CUs)**
- You buy a **capacity size** (F2, F4, F8... up to F2048)
- Each capacity has a fixed CU count and a fixed hourly/monthly rate
- **Everything runs on that capacity**: warehouse queries, Spark, pipelines, Power BI

**Example costs (US West 2, pay-as-you-go):**

| Capacity | CUs | $/hour | $/month (730 hrs) |
|----------|-----|--------|-------------------|
| F2 | 2 | $0.36 | ~$263 |
| F8 | 8 | $1.44 | ~$1,051 |
| F32 | 32 | $5.76 | ~$4,205 |
| F64 | 64 | $11.52 | ~$8,410 |

**Reserved pricing (1-year commit):** ~40% savings

**Key differences from Synapse:**
- **Always on by default** — capacity runs 24/7 unless you pause it
- **Burst/smoothing** — Fabric can "borrow" CUs for spiky workloads and smooth over time
- **Power BI included** — but below F64, users still need Pro licenses ($10/user/month)
- **OneLake storage** — charged separately, similar to ADLS (~$0.023/GB/month)

**Fabric cost gotcha:** If you're under F64, you still need Power BI Pro licenses for report consumers. At F64+, unlimited viewers are included.

---

### Quick Comparison: What would 6clicks likely pay?

For a small team doing embedded analytics + data warehouse:

| Scenario | Synapse (Dedicated) | Fabric |
|----------|--------------------|---------| 
| Entry level (light usage) | DW100c: ~$870/mo + storage | F8: ~$1,050/mo |
| Medium (production workload) | DW500c: ~$4,350/mo | F32: ~$4,200/mo |
| With reserved pricing | ~35-65% off | ~40% off |

**Bottom line:** For a 3-person team with moderate workloads, Fabric F8-F32 range or Synapse DW100c-DW500c are likely starting points. Fabric is simpler to reason about; Synapse gives more control but more knobs to manage.

---

## Initial Thinking — Questions for Myself

- Given a 3-person team, Fabric might be the simpler path vs Synapse. Less knobs to turn.
- They're already on SQL Server, so T-SQL familiarity is a plus for Synapse/Fabric.
- Yellowfin can connect to Synapse or Fabric — need to confirm connector compatibility.
- If they have Databricks on Azure already (check!), that's another option — but probably overkill for their use case.
- Key question: do they want a lakehouse pattern (raw → curated → serving) or just a warehouse for analytics?
