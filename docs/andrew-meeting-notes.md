# Andrew Meeting Notes - PoC Planning

## Databricks Account & Azure Setup

### Azure Subscription Structure
- How does 6clicks organise subscriptions today? 
- Is there a separate subscription per environment (dev/staging/prod) or per region? 
- Or one subscription with resource groups for separation?
- What are the 11 regions, and what's driving them? (Data residency? Customer proximity? Both?)
- Do we really need Databricks in all 11, or can we consolidate to 3-4 based on data residency requirements (e.g., AU, US, UK, Germany)?

### Databricks Account vs Workspaces
Best practice is **one Databricks account** with **separate workspaces** for dev/staging/prod:
- Unified governance via Unity Catalog at the account level
- Environment isolation at the workspace level
- Single pane of glass for admin (users, groups, audit logs)

### Unity Catalog Metastore
- One metastore per region (for data residency)
- Shared across all workspaces in that region
- Who creates this? Platform team or Data team?

### Where Does the Databricks Account Live?
- Typically in the "prod" or "shared services" subscription
- Workspaces can span subscriptions if needed, but simpler to keep them together for PoC

---

## Multi-Region Considerations (11 Regions)

### Do We Need Workspaces in All 11 Regions?

**Short answer:** Probably not. The 11 SQL Server regions are for *operational* data residency. Analytics may be different.

### Key Questions to Answer

1. **What's driving the 11 regions?** 
   - Data residency/GDPR? 
   - Customer proximity for app latency? 
   - Both?

2. **Does *analytics* data need to stay in-region?**
   - GDPR requires personal data to stay in EU, but analytics on aggregated/anonymised data may be more flexible
   - Some customers may have contractual requirements

3. **Can we consolidate analytics to fewer regions?**
   - Likely candidates: Australia, US, UK, Germany (4 main regions instead of 11)
   - Each consolidated region would have its own metastore + workspaces

### Options

| Approach | Workspaces | Pros | Cons |
|----------|------------|------|------|
| **One per SQL region** | 11 × 3 = 33 | Full data residency | Massive overhead, expensive |
| **Consolidate to 4 main regions** | 4 × 3 = 12 | Manageable, covers major jurisdictions | Need to map 11 → 4 |
| **Single region (PoC only)** | 3 | Simplest, fastest to deliver | Not production-ready |
| **Hub + spokes** | 1 hub + 11 federated | Central governance | Cross-region query latency/cost |

### Recommendation for PoC
- **Start with one region** (Australia East) for PoC
- Design for multi-region from the start (naming conventions, Terraform modules)
- Decide on consolidation strategy before scaling

### Architecture Pattern (Multi-Region)

```
Databricks Account (Global)
├── Unity Catalog (Account Level)
│
├── Australia East
│   ├── Metastore (regional)
│   └── Workspaces: dev / staging / prod
│
├── US East
│   ├── Metastore (regional)
│   └── Workspaces: dev / staging / prod
│
├── UK South
│   ├── Metastore (regional)
│   └── Workspaces: dev / staging / prod
│
└── Germany West Central
    ├── Metastore (regional)
    └── Workspaces: dev / staging / prod
```

Each region has:
- Its own ADLS storage account (data stays local)
- Its own Unity Catalog metastore
- Its own set of workspaces
- Same dbt code deployed via CI/CD

---

## Access Management

### Identity & SSO
- Is Azure AD already set up with SSO for other tools? (Databricks will use the same Azure AD tenant)
- Are there existing Azure AD groups for data roles, or do we need to create them?
- Who manages Azure AD groups? (Platform team? IT?)

### Service Principals
- Do you have a service principal for CI/CD pipelines (e.g., Azure DevOps)?
- Will we need a separate service principal for Databricks automation (ADF, dbt jobs)?

### RBAC & Permissions
- Who should have admin access to Databricks workspaces?
- Do you want environment-based access? (e.g., devs can access dev, only ops can access prod)
- Any compliance requirements for audit logging? (Unity Catalog audit logs are included with Premium)

### Databricks SKU
- **Premium is the only option** (Standard is EOL, Enterprise doesn't exist on Azure)
- Premium includes: Unity Catalog, RBAC, audit logs, SSO, IP access lists
- No decision needed here—just be aware of pricing (~$0.40–0.70/DBU depending on compute type)

---

## Team & Responsibilities

### Who Provisions Infrastructure?
- Platform team for Terraform?
  - Subscriptions, resource groups
  - Storage accounts
  - Databricks workspaces
  - Unity Catalog metastore
  - Azure AD groups
- Or is Data team self-service?

### Who Owns the Data Layer?
- Data team for:
  - Catalogs, schemas, tables
  - dbt models
  - Pipelines (ADF / Databricks Jobs)

### Access Control
- Do you already have Azure AD groups for data roles?
- Or need to create them (e.g., `grp-databricks-data-engineers`, `grp-databricks-analysts`)?

---

## Quick Reference: AWS vs Azure

| Concept | AWS | Azure |
|---------|-----|-------|
| Isolation boundary | Account | Subscription |
| Grouping accounts | AWS Organizations | Management Groups |
| Resource grouping | Tags / StackSets | Resource Groups |
| Identity | IAM (per account) | Azure AD (tenant-wide) |

---

## Next Steps

- [ ] Clarify subscription structure
- [ ] Decide on multi-region consolidation (11 → 4?)
- [ ] Identify Platform team contact for Terraform
- [ ] Define Azure AD groups needed
- [ ] Agree on PoC timeline and scope

