# QBA Question Answer: Medallion Implementation

## Overview

Implementation plan for migrating `vwQBA_QuestionAnswer` (766 lines of SQL with heavy JSON parsing) to the CDC → Databricks → Push-back architecture.

---

## Source Tables (Verified from DDL)

All tables exist in `dbo` schema with standard audit columns (`CreationTime`, `LastModificationTime`, `IsDeleted`).

| Table | Key Columns | CDC Priority |
|-------|-------------|--------------|
| `dbo.Answer` | Id, QuestionId, TenantId, ComponentStr (JSON), Score, RiskStatus | High |
| `dbo.Question` | Id, TenantId, ComponentStr (JSON), Type, Weighting, IsMultiSelectType* | High |
| `dbo.QuestionGroup` | Id, TenantId, Name | Medium |
| `dbo.QuestionGroupResponse` | Id, TenantId, QuestionGroupId, Response, Compliance | Medium |

*`IsMultiSelectType` is a computed column: `CASE WHEN Type IN (3,7,8) THEN 1 ELSE 0 END`

---

## Architecture

```mermaid
flowchart TB
    subgraph "SQL Server (Production)"
        A1[dbo.Answer]
        A2[dbo.Question]
        A3[dbo.QuestionGroup]
        A4[dbo.QuestionGroupResponse]
    end
    
    subgraph "CDC Layer"
        CDC[ADF CDC Resource<br/>5-min intervals]
    end
    
    subgraph "Bronze Layer (Delta Lake)"
        B1[bronze.answer]
        B2[bronze.question]
        B3[bronze.question_group]
        B4[bronze.question_group_response]
    end
    
    subgraph "Silver Layer (Parsed & Normalized)"
        S1[silver.stg_answer<br/>JSON parsed]
        S2[silver.stg_question<br/>JSON parsed]
        S3[silver.question_options<br/>Exploded array]
        S4[silver.stg_question_group]
        S5[silver.stg_question_group_response]
    end
    
    subgraph "Gold Layer"
        G1[gold.qba_question_answer]
    end
    
    subgraph "Serving"
        SQL[SQL Server<br/>gold schema]
        YF[Yellowfin]
    end
    
    A1 & A2 & A3 & A4 --> CDC
    CDC --> B1 & B2 & B3 & B4
    B1 --> S1
    B2 --> S2
    B2 --> S3
    B3 --> S4
    B4 --> S5
    S1 & S2 & S3 & S4 & S5 --> G1
    G1 --> SQL
    SQL --> YF
```

---

## Incremental Data Flow

```mermaid
sequenceDiagram
    participant Prod as SQL Server (Prod)
    participant CDC as CDC Tables
    participant ADF as ADF Pipeline
    participant Bronze as Bronze (Delta)
    participant DBT as Databricks dbt
    participant Gold as Gold (Delta)
    participant Serve as SQL Server (Gold)

    Note over Prod,CDC: CDC captures changes continuously
    Prod->>CDC: Transaction log → Change tables
    
    Note over ADF,Bronze: Every 5 minutes
    ADF->>CDC: Read changes since last LSN
    ADF->>Bronze: Append to Delta (with _cdc_operation)
    ADF->>ADF: Store new watermark (LSN)
    
    Note over DBT,Gold: Every 10 minutes
    DBT->>Bronze: Read new records (incremental)
    DBT->>DBT: Parse JSON → Silver
    DBT->>DBT: Apply business logic → Gold
    DBT->>Gold: MERGE into gold table
    
    Note over Gold,Serve: After dbt completes
    ADF->>Gold: Read full gold table
    ADF->>Serve: Copy to staging table
    Serve->>Serve: Swap staging → live
```

---

## Layer Details

### Bronze Layer: Raw CDC Data

Bronze models are simple - just land the CDC data with metadata.

```mermaid
erDiagram
    BRONZE_ANSWER {
        int id PK
        int question_id FK
        int tenant_id
        string component_str
        decimal score
        int risk_status
        int compliance
        bigint responder_id
        bigint assessment_response_id
        datetime last_modification_time
        string _cdc_operation
        bigint _cdc_lsn
        datetime _cdc_timestamp
    }
    
    BRONZE_QUESTION {
        int id PK
        int tenant_id
        int assessment_domain_id
        string component_str
        int type
        float weighting
        bit is_multi_select_type
        bit has_conditional_logic
        bit hidden_in_survey
        int question_group_id FK
        int question_group_response_id FK
        datetime last_modification_time
        string _cdc_operation
        bigint _cdc_lsn
    }
```

**Incremental Strategy**: Append-only. Each CDC batch adds new rows.

```sql
-- bronze/answer.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key=None  -- Append all CDC records
) }}

SELECT 
    *,
    __$operation AS _cdc_operation,
    __$start_lsn AS _cdc_lsn,
    CURRENT_TIMESTAMP() AS _cdc_loaded_at
FROM {{ source('cdc', 'dbo_Answer_CT') }}
{% if is_incremental() %}
WHERE __$start_lsn > (SELECT COALESCE(MAX(_cdc_lsn), 0) FROM {{ this }})
{% endif %}
```

---

### Silver Layer: Parsed & Normalized

JSON parsing happens **once** here. This is where the performance gain comes from.

```mermaid
erDiagram
    STG_ANSWER {
        int answer_id PK
        int question_id FK
        int tenant_id
        string answer_radio
        string answer_radio_custom
        string answer_text_area
        string answer_multi_select_json
        boolean answer_submit
        decimal score
        int risk_status
        int compliance
        bigint responder_id
        datetime effective_at
        boolean is_deleted
    }
    
    STG_QUESTION {
        int question_id PK
        int tenant_id
        int assessment_domain_id
        string question_name
        int question_type
        string question_type_code
        float weighting
        boolean is_multi_select
        string options_json
        boolean has_conditional_logic
        boolean hidden_in_survey
        datetime effective_at
        boolean is_deleted
    }
    
    QUESTION_OPTIONS {
        int question_id FK
        string option_value
        int option_rank
        int option_risk_status
        float weighting
    }
    
    STG_QUESTION ||--o{ QUESTION_OPTIONS : "exploded from JSON"
```

**Incremental Strategy**: Merge on primary key. Apply latest state.

```sql
-- silver/stg_answer.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='answer_id'
) }}

WITH ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id 
            ORDER BY _cdc_lsn DESC
        ) AS rn
    FROM {{ ref('bronze_answer') }}
    {% if is_incremental() %}
    WHERE _cdc_loaded_at > (SELECT MAX(_cdc_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    id AS answer_id,
    question_id,
    tenant_id,
    
    -- Parse JSON once, store as columns
    get_json_object(component_str, '$.Radio') AS answer_radio,
    get_json_object(component_str, '$.RadioCustom') AS answer_radio_custom,
    get_json_object(component_str, '$.TextArea') AS answer_text_area,
    get_json_object(component_str, '$.MultiSelectValues') AS answer_multi_select_json,
    CAST(get_json_object(component_str, '$.Submit') AS BOOLEAN) AS answer_submit,
    
    score,
    max_possible_score,
    risk_status,
    compliance,
    responder_id,
    assessment_response_id,
    
    COALESCE(last_modification_time, creation_time) AS effective_at,
    CASE WHEN _cdc_operation = 1 THEN TRUE ELSE is_deleted END AS is_deleted,
    _cdc_loaded_at

FROM ranked
WHERE rn = 1  -- Latest version per answer
```

```sql
-- silver/question_options.sql (Explode JSON array ONCE)
{{ config(materialized='table') }}

SELECT
    q.question_id,
    q.weighting AS question_weighting,
    opt.value AS option_value,
    CAST(opt.rank AS INT) AS option_rank,
    CAST(opt.riskStatus AS INT) AS option_risk_status
    
FROM {{ ref('stg_question') }} q
LATERAL VIEW OUTER explode(
    from_json(options_json, 'array<struct<value:string,rank:int,riskStatus:int>>')
) AS opt
WHERE q.is_multi_select = TRUE
  AND q.is_deleted = FALSE
```

---

### Gold Layer: Business Logic

The final table combines all pieces. **No JSON parsing** - just joins and CASE statements.

```mermaid
erDiagram
    GOLD_QBA_QUESTION_ANSWER {
        string answer_response_pk PK
        int tenant_id
        int question_id
        int answer_id
        string part
        string question_name
        int question_type
        string question_type_code
        float question_weighting
        int answer_response_key
        string answer_response_value
        string answer_response_value_list
        string answer_text_area
        decimal answer_score
        int answer_risk_status
        string answer_risk_status_code
        decimal answer_risk_status_calc
        int answer_compliance
        string answer_compliance_code
        string question_status
        int assessment_domain_id
        datetime qba_qa_update_time
    }
```

**Incremental Strategy**: Full rebuild OR merge based on modified timestamps.

```sql
-- gold/qba_question_answer.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='answer_response_pk',
    partition_by={'field': 'tenant_id', 'data_type': 'int'}
) }}

-- Detect which questions/answers changed
{% if is_incremental() %}
{% set last_run = "SELECT COALESCE(MAX(qba_qa_update_time), '1900-01-01') FROM " ~ this %}
{% endif %}

WITH questions AS (
    SELECT * FROM {{ ref('stg_question') }}
    WHERE is_deleted = FALSE
    {% if is_incremental() %}
    AND effective_at > ({{ last_run }})
    {% endif %}
),

answers AS (
    SELECT * FROM {{ ref('stg_answer') }}
    WHERE is_deleted = FALSE
    {% if is_incremental() %}
    AND effective_at > ({{ last_run }})
    {% endif %}
),

question_options AS (
    SELECT * FROM {{ ref('question_options') }}
),

-- ... (rest of the business logic from original view)
-- Single answer, multi-select, freetext, no-answer, group variants
-- All UNIONed together

final AS (
    SELECT * FROM qa_single
    UNION ALL SELECT * FROM qa_multi
    UNION ALL SELECT * FROM qa_freetext
    UNION ALL SELECT * FROM qa_no_answer
    UNION ALL SELECT * FROM qa_group_single
    UNION ALL SELECT * FROM qa_group_multi
)

SELECT 
    *,
    CONCAT(answer_id, '_', answer_response_key) AS answer_response_pk
FROM final
```

---

## Incremental Refresh Patterns

### Option A: Full Table Refresh (Recommended for Start)

```mermaid
flowchart LR
    subgraph "Every 10 min"
        A[Bronze<br/>Append CDC] --> B[Silver<br/>Merge latest state]
        B --> C[Gold<br/>Full rebuild]
        C --> D[Copy to SQL<br/>Swap tables]
    end
```

**Why**: Their data volumes (10s of thousands per tenant) make full rebuild fast (~10-30 seconds on Databricks).

**Gold model config**:
```sql
{{ config(materialized='table') }}
```

---

### Option B: True Incremental (If Needed Later)

```mermaid
flowchart LR
    subgraph "Every 10 min"
        A[Bronze<br/>Append CDC] --> B[Silver<br/>Merge deltas]
        B --> C[Gold<br/>Merge changed PKs]
        C --> D[Copy changed rows<br/>Merge into SQL]
    end
```

**When to use**: If gold table grows to millions of rows and full refresh becomes slow.

**Gold model config**:
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='answer_response_pk',
    on_schema_change='append_new_columns'
) }}
```

**Challenge**: Detecting all affected rows when a Question changes (affects all its Answers).

---

## Handling Edge Cases

### Deletes

CDC captures deletes with `__$operation = 1`. Handle in Silver:

```sql
CASE WHEN _cdc_operation = 1 THEN TRUE ELSE is_deleted END AS is_deleted
```

Gold layer filters: `WHERE is_deleted = FALSE`

### Late-Arriving Data

Bronze is append-only, so late data just appends. Silver merge picks up latest LSN per PK.

### Schema Evolution

Bronze stores raw JSON. If source schema changes:
1. New columns in Bronze (append)
2. Update Silver parsing logic
3. dbt handles downstream propagation

---

## dbt Project Structure

```
models/
├── sources.yml                      # CDC source definitions
├── bronze/
│   ├── bronze_answer.sql
│   ├── bronze_question.sql
│   ├── bronze_question_group.sql
│   └── bronze_question_group_response.sql
├── silver/
│   ├── stg_answer.sql              # JSON parsed
│   ├── stg_question.sql            # JSON parsed  
│   ├── question_options.sql        # Exploded options array
│   ├── stg_question_group.sql
│   └── stg_question_group_response.sql
└── gold/
    └── qba_question_answer.sql     # Final denormalized table
```

---

## Performance Comparison

| Aspect | Current (View) | New (Medallion) |
|--------|---------------|-----------------|
| JSON parsing | Every Yellowfin query | Once (Silver layer) |
| `openjson()` explosion | Every query | Once (Silver layer) |
| 6-way UNION | Every query | Pre-materialized |
| Window functions | Every query | Pre-computed |
| Execution location | Production SQL Server | Databricks Serverless |
| Yellowfin query | 766 lines of SQL | Simple `SELECT *` |
| Query time | Minutes | Seconds |

---

## Push-Back to SQL Server

### Table Swap (Blue-Green)

```sql
-- ADF executes this stored procedure after copy
CREATE PROCEDURE gold.SwapQBAQuestionAnswer
AS
BEGIN
    BEGIN TRANSACTION;
    
    -- Swap staging to live
    IF OBJECT_ID('gold.qba_question_answer_old') IS NOT NULL
        DROP TABLE gold.qba_question_answer_old;
    
    IF OBJECT_ID('gold.qba_question_answer') IS NOT NULL
        EXEC sp_rename 'gold.qba_question_answer', 'qba_question_answer_old';
    
    EXEC sp_rename 'gold_staging.qba_question_answer', 'gold.qba_question_answer';
    
    COMMIT;
END
```

### Yellowfin View (Optional - for zero-downtime)

```sql
-- Yellowfin queries this view
CREATE VIEW reporting.vwQBA_QuestionAnswer AS
SELECT * FROM gold.qba_question_answer;
```

---

## Orchestration Timeline

```mermaid
gantt
    title 10-Minute Refresh Cycle
    dateFormat mm:ss
    axisFormat %M:%S
    
    section CDC
    Capture changes     :cdc, 00:00, 5m
    
    section dbt
    Bronze append       :bronze, 05:00, 30s
    Silver merge        :silver, 05:30, 1m
    Gold rebuild        :gold, 06:30, 1m
    
    section Push-back
    Copy to staging     :copy, 07:30, 1m
    Swap tables         :swap, 08:30, 5s
    
    section Buffer
    Headroom            :buffer, 08:35, 1m25s
```

Total cycle: ~8.5 minutes with 1.5 min buffer before next cycle.

---

## Migration Steps

1. **Enable CDC** on 4 source tables in SQL Server
2. **Create ADF CDC Resource** pointing to Delta Lake
3. **Deploy dbt models** (bronze → silver → gold)
4. **Create ADF pipeline** for push-back + swap
5. **Test** with one tenant's data
6. **Validate** output matches existing view
7. **Switch** Yellowfin to new gold table
8. **Deprecate** old view

