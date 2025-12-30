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

## Recommendation for 6clicks

Given:
- Tables already have `LastModifiedDate` 
- Using `IsDeleted` soft-delete pattern
- Modest data volumes

**Start with Option 3 (Watermark Pattern)** – zero source overhead, simple to implement.

If hard deletes become a requirement, upgrade to **Option 2 (Change Tracking)** for lower overhead than full CDC.

