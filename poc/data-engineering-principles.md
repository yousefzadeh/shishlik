# Data Engineering Principles — POC Quick Reference

## Core Principles

### 1. Fail Fast
- Validate data at ingestion (Bronze -> Silver), not at query time
    - fail the pipeline if they don't meet the requirements
- Log / quarantine malformed JSON/unexpected types immediately 
    - implement alerting to notify people when this happens
    - is DQX the right choice?

### 2. Idempotency
- Every pipeline run with the same input produces the same output
- Use `MERGE` (upsert) instead of `INSERT` for Silver/Gold tables
- Include `_loaded_at` timestamp to track data freshness, not correctness

### 3. Immutable Bronze
- Bronze is append-only raw data — never update or delete
- Keeps full audit trail; enables reprocessing if Silver logic changes

---

## The JSON Update Problem

**Scenario:** `Question.ComponentStr` has 5 options today. Tomorrow, 2 options are removed.

### If Silver stores unnested rows (`question_options` table):

| Approach | Pros | Cons |
|----------|------|------|
| **Full refresh** (truncate + reload) | Simple, always correct | Slow for large tables |
| **MERGE with soft delete** | Handles adds/updates/deletes | Need to track `_is_deleted` flag |
| **SCD Type 2** | Full history preserved | Complex, overkill for POC |

### Recommended for POC:

```sql
-- Silver refresh pattern: MERGE with delete detection
MERGE INTO silver.question_options AS target
USING (
    SELECT question_id, option_value, option_rank, option_risk_status
    FROM bronze.question
    LATERAL VIEW EXPLODE(...) 
) AS source
ON target.question_id = source.question_id 
   AND target.option_value = source.option_value
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
WHEN NOT MATCHED BY SOURCE AND target.question_id IN (changed_questions)
    THEN DELETE;
```

**Key insight:** The `NOT MATCHED BY SOURCE` clause handles removed options — but you must scope it to only questions that were updated in this CDC batch, otherwise you'll delete options from questions that weren't in the batch.

---

## CDC Batch Considerations

1. **Track which source rows changed** — don't reprocess unchanged rows
2. **Process in order** — if same row updated twice in batch, apply final state
3. **Handle late arrivals** — timestamp-based deduplication in Silver

---

*Keep it simple for POC. Optimize later.*
