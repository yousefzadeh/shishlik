# Architecture Ideas

## Option: Lakehouse Pattern with Synapse Serverless

This approach decouples analytics workloads from production SQL Server using a CDC-based lakehouse pattern.

### Main Architecture Flow

```mermaid
flowchart LR
    subgraph Production["Production (per region)"]
        SQLPROD[(SQL Server<br/>Production DB)]
    end

    subgraph DataLake["Azure Data Lake Storage Gen2"]
        RAW[("Raw Layer<br/>(Parquet)")]
        TRANSFORM[("Transformed Layer<br/>(Parquet)")]
    end

    subgraph Synapse["Azure Synapse Workspace"]
        PIPE[Synapse Pipelines<br/>CDC Orchestration]
        SERVERLESS[Serverless SQL Pool<br/>$5/TB scanned]
    end

    subgraph Analytics["Analytics Serving"]
        SQLANALYTICS[(Azure SQL DB<br/>Analytics)]
        YF[Yellowfin<br/>Embedded BI]
    end

    SQLPROD -->|"1. CDC Extract"| PIPE
    PIPE -->|"2. Write Parquet"| RAW
    RAW -->|"3. dbt reads"| SERVERLESS
    SERVERLESS -->|"4. dbt transforms"| TRANSFORM
    TRANSFORM -->|"5. Load final tables"| SQLANALYTICS
    SQLANALYTICS -->|"6. Query"| YF

    style SQLPROD fill:#e74c3c,color:#fff
    style RAW fill:#3498db,color:#fff
    style TRANSFORM fill:#3498db,color:#fff
    style SERVERLESS fill:#9b59b6,color:#fff
    style SQLANALYTICS fill:#27ae60,color:#fff
    style YF fill:#f39c12,color:#fff
```

**Flow:**
1. **CDC Extract** - Synapse Pipeline reads change data from prod SQL Server
2. **Write Parquet** - Lands in ADLS Gen2 raw layer (partitioned by date/region)
3. **dbt reads** - dbt models query raw Parquet via Serverless SQL Pool
4. **dbt transforms** - Output written to transformed layer (also Parquet)
5. **Load final tables** - Mart tables pushed to Azure SQL DB for serving
6. **Query** - Yellowfin queries the analytics DB (not prod!)

### Per-Region Deployment (GDPR Compliant)

Each region keeps its own data lake and analytics DB, maintaining data sovereignty. Yellowfin connects to multiple data sources.

```mermaid
flowchart TB
    subgraph AU["Australia Region"]
        SQLAU[(SQL Server AU)] --> ADLSAU[ADLS AU] --> SERVAU[Serverless Pool] --> SQLANALAU[(Analytics DB AU)]
    end
    
    subgraph UK["UK Region"]
        SQLUK[(SQL Server UK)] --> ADLSUK[ADLS UK] --> SERVUK[Serverless Pool] --> SQLANALUK[(Analytics DB UK)]
    end
    
    subgraph US["US Region"]
        SQLUS[(SQL Server US)] --> ADLSUS[ADLS US] --> SERVUS[Serverless Pool] --> SQLANALUS[(Analytics DB US)]
    end

    SQLANALAU --> YF[Yellowfin]
    SQLANALUK --> YF
    SQLANALUS --> YF
```

### Key Benefits

- **Production isolation**: Analytics queries never hit prod SQL Server
- **Cost-effective**: Synapse Serverless is ~$5/TB scanned (same as Athena), negligible for their data volumes
- **Leverages existing investment**: They already have Synapse workspace for orchestration
- **dbt compatible**: `dbt-synapse` adapter supports serverless pools
- **GDPR friendly**: Each region can have isolated storage and compute
- **Near real-time option**: CDC can run frequently for fresher data

### Azure Component Mapping

| AWS/GCP | Azure Equivalent |
|---------|------------------|
| S3 / GCS | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Athena / BigQuery | Synapse Serverless SQL Pool |
| Glue / Dataflow | Azure Data Factory / Synapse Pipelines |

### Implementation Notes

- SQL Server has native CDC: `sp_cdc_enable_db`, `sp_cdc_enable_table`
- Synapse Pipelines = ADF under the hood (they're already familiar with this)
- Parquet format reduces storage costs and query scan costs
- Partition by date and/or tenant for optimal query performance
