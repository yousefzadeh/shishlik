# CDC Options for 6clicks

Quick comparison of approaches to capture changes from Azure SQL Database.

---

## Option 1: Native CDC on SQL Server

Enable CDC directly on source tables. Captures all INSERT/UPDATE/DELETE with before/after values.

**Pros:** Full audit trail, captures deletes, no app changes needed  
**Cons:** 10-15% write overhead, log growth issues, cleanup job management

📚 **Docs:**
- [Enable CDC on Azure SQL Database](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server)
- [About CDC in SQL Server](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)

---

## Option 2: Change Tracking (Lighter Alternative)

Tracks which rows changed, but not the actual values. Lower overhead than CDC.

**Pros:** ~2-5% overhead (vs 10-15%), simpler cleanup, captures deletes  
**Cons:** No before/after values, still need to query source for current data

📚 **Docs:**
- [About Change Tracking](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server)
- [ADF Tutorial: Incremental copy using Change Tracking](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-change-tracking-feature-portal)

---

## Option 3: ADF Watermark Pattern (No source overhead)

Use `LastModifiedDate` column. ADF queries `WHERE LastModifiedDate > @watermark`.

**Pros:** Zero overhead on source, simple to implement  
**Cons:** Can't capture hard deletes (only soft delete with `IsDeleted` flag)

📚 **Docs:**
- [ADF Tutorial: Incremental copy using LastModifiedDate](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-lastmodified-copy-data-tool)
- [Incremental data loading patterns](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-overview)

---

## Option 4: ADF CDC Resource (Managed CDC)

ADF-native CDC that reads from SQL Server change tables. Managed polling and watermarks.

**Pros:** Full change capture, ADF manages complexity, writes to Delta Lake  
**Cons:** Still requires CDC enabled on source, relatively new feature

📚 **Docs:**
- [CDC Resource Overview](https://learn.microsoft.com/en-us/azure/data-factory/concepts-change-data-capture-resource)
- [How to create a CDC Resource](https://learn.microsoft.com/en-us/azure/data-factory/how-to-change-data-capture-resource)
- [ADF Tutorial: Incremental copy using CDC](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-change-data-capture-feature-portal)

---

## Target Format Support

| Option | Delta Lake | Parquet | JSON | SQL Database |
|--------|------------|---------|------|--------------|
| **Native CDC** | Via ADF | Via ADF | Via ADF | Via ADF |
| **Change Tracking** | Via ADF | Via ADF | Via ADF | Via ADF |
| **ADF Watermark** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **ADF CDC Resource** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |

**Notes:**
- Native CDC & Change Tracking are source-side features only. You need ADF (or another tool) to read and write to any format.
- ADF Copy Activity supports 30+ formats including Delta, Parquet, JSON, Avro, ORC, CSV
- **Delta Lake is fully supported** by ADF CDC Resource with schema evolution ([docs](https://learn.microsoft.com/en-us/azure/data-factory/how-to-change-data-capture-resource-with-schema-evolution))

**For medallion architecture (Bronze → Silver → Gold):**
- All options can write to Delta Lake in ADLS Gen2
- Delta is recommended for Bronze/Silver layers (ACID transactions, time travel, schema evolution)
- Gold can stay as Delta or push to SQL Server for Yellowfin

---

## Infrastructure as Code Support

| Option | Terraform | Bicep/ARM | Notes |
|--------|-----------|-----------|-------|
| **Native CDC** | ⚠️ Partial | ⚠️ Partial | Enable via T-SQL script in deployment |
| **Change Tracking** | ⚠️ Partial | ⚠️ Partial | Enable via T-SQL script in deployment |
| **ADF Watermark** | ✅ Yes | ✅ Yes | Full pipeline definition as code |
| **ADF CDC Resource** | ✅ Yes | ✅ Yes | Full resource definition as code |

**Details:**

- **CDC/Change Tracking**: Database features enabled via T-SQL, not native IaC resources. Workaround: use `sqlcmd` or Azure CLI in deployment scripts.
  ```sql
  EXEC sys.sp_cdc_enable_db;
  EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='Answer';
  ```

- **ADF Pipelines/CDC Resource**: Fully supported in ARM, Bicep, Terraform (`azurerm_data_factory_*` resources). Can export existing pipelines as JSON/ARM templates.

- **Terraform**: Use `azurerm_data_factory_pipeline` for watermark pattern, or `azurerm_resource_group_template_deployment` for CDC Resource (if not natively supported yet).

- **Bicep**: Use `Microsoft.DataFactory/factories/pipelines` or `Microsoft.DataFactory/factories/adfcdcs`.

**Recommendation**: Combine IaC for ADF resources + SQL deployment scripts for database-level CDC/CT enablement.

---

## Getting CDC Data into Databricks

If using Native CDC on SQL Server, here are the options to get that data into Databricks:

### Option A: ADF → Delta Lake → Databricks (Recommended)

```
SQL Server CDC tables → ADF Pipeline → Delta Lake (ADLS) → Databricks reads Delta
```

- ADF reads from `cdc.<schema>_<table>_CT` change tables
- Writes to Delta Lake in Azure Data Lake Storage
- Databricks reads/processes Delta tables
- **Decoupled**: Databricks doesn't need direct access to SQL Server

### Option B: Databricks Lakeflow Connect (Native)

```
SQL Server CDC → Lakeflow Connect → Delta Tables in Unity Catalog
```

- Databricks-native ingestion feature
- Reads directly from SQL Server CDC tables
- Writes to Delta tables automatically
- Handles watermarks and incremental loads
- 📚 [Docs: Lakeflow Connect SQL Server](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/sql-server-pipeline)

### Option C: Direct JDBC Read (Simple but Limited)

```python
# Databricks notebook - read CDC table directly
jdbc_url = "jdbc:sqlserver://<server>:1433;databaseName=<db>"
cdc_query = "(SELECT * FROM cdc.dbo_Answer_CT WHERE __$start_lsn > ?) AS cdc"
df = spark.read.jdbc(url=jdbc_url, table=cdc_query, properties=connection_props)
```

- Quick for POC
- **Cons**: Databricks needs network access to SQL Server, you manage watermarks manually

### Option D: Debezium + Kafka (Streaming)

```
SQL Server → Debezium → Kafka/Event Hub → Databricks Structured Streaming
```

- True real-time streaming (sub-second latency)
- More infrastructure to manage
- Overkill for 10-min refresh requirement

---

**Recommendation for 6clicks**: 

**Option A (ADF → Delta)** is cleanest:
- ADF handles CDC reading and watermarks
- Databricks stays focused on transformations (dbt)
- Clear separation of concerns
- Already using ADF for orchestration

---

## Cost Implications (10-min refresh)

| Option | Source DB Cost | ADF Cost | Notes |
|--------|---------------|----------|-------|
| **Native CDC** | +10-15% vCore usage | ~$0.001/run | Capture job runs continuously on source |
| **Change Tracking** | +2-5% vCore usage | ~$0.001/run | Lower source impact |
| **ADF Watermark** | None | ~$0.001/run | Cheapest overall |
| **ADF CDC Resource** | +10-15% vCore usage | ~$0.10-0.15/vCore-hr | Runs continuously, most expensive ADF option |

**At 10-min frequency (144 runs/day):**
- Watermark/CT pipeline: ~$0.15/day in ADF activity costs
- ADF CDC Resource: ~$2.50-4/day (runs 24/7)

**Source database impact:**
- If running 4 vCores @ $0.50/vCore-hr, 10% overhead = ~$1.50/day extra
- Change Tracking at 3% overhead = ~$0.45/day extra

**Bottom line:** Watermark pattern is cheapest if you can live with soft deletes only.

---

## Recommendation for 6clicks

Given:
- Tables already have `LastModifiedDate` 
- Using `IsDeleted` soft-delete pattern
- Modest data volumes

**Start with Option 3 (Watermark Pattern)** – zero source overhead, simple to implement.

If hard deletes become a requirement, upgrade to **Option 2 (Change Tracking)** for lower overhead than full CDC.

