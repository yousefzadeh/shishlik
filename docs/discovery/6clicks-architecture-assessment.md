# 6clicks Data Architecture Assessment

## 1. Executive Summary

## 2. Introduction

- focus on data warhousing and yellowfin backend
- self-serve analytics
- multi-region deployment, data has to stay in the same region

## 3. Current State Assessment

### Strategy
- Big part is multi-region, whatever architecture needs to be able to be rolled out to different regions. Keep costs efficient
  - Flagship regions are AU and UK
- Doing more AI stuff, vector databases  
- Any nlp use cases?  
  - Could see that happening. Next 3-6 months lots of similarity searches. Not much training
- Analytics across regions is not possible right now  
  - Raw data can’t go out of the region

#TODO what are my goals in this assessment
- current setup
- exsiting initiatives
- business goals
- business problems and piroirty
- technology gaps
- resource strengths and opportunity

### Pain points
- Sometimes it would take 25 minutes for a report to load
- Have had customers churn because reporting wasn’t working
- 

### Benefits of current setup (dbt views)
- Faster to develop
- Didn’t need to do incremental updates, etc. get real-time data into the report


### Current data landscape
- data size
  - millions of rows
  - thousands of clients, with 10,000s rows max

### Team
- Good team, needs direction
  - Mostly analytics engineer persona, good domain knowledge and sql
- Small team focused on delivery, doesn't leave room for major initiatives
  - All major efforts were to make sure the user can see the reports in an efficient way
- Jake knows snowflake, etc  
- Bhanu more sql, one who picked up synapse  
- Sai from data analyst background, came as BA, knows the business domain well. Most proficient yellowfin user

### dbt Setup
- set up by an external agency, complex views, only a few moved to tables refreshing daily, 
- only views, not the standard pattern

### SQL Server & Synapse
- Everything is SQL Server. Synapse is only used for ETL pipeline architecture. No pool usage, still connecting all source and destination to SQL Server.
- Yellowfin points to SQL Server
- Pipeline architecture is basic using only ETL pipelines, haven't used advanced features

### Yellowfin
- Row-level filter is set up in Yellowfin as access filter (based on tenant id)

### Recent Improvements
- Moving to a different schema: _is it only to denormalize so the view doesn't have to run too many things?_
  - Everything was in one model
  - With new schema we tried to divide the context models, reduce the volume by dividing one model into 2 or 3. Reduce amount of nesting in views.
  - Used to take 10-20 seconds, now runs in 1 second

### Assessment Module (Most Problematic)
- Very complex, was taking around 5 minutes. Tried to shift it to Synapse.
- Taken away the real-time nature and shifted the entire thing to tables and indexes
- Synapse is maintaining the schedule intervals. Split all modules into what can be refreshed once a day.
- Assessment module is working for the clients now. It's not that frequently used, so they're happy with it.
- Able to generate in less than 7 seconds
- Synapse cost is high, so won't scale. Cost is not Synapse itself, how it's reading, back to views. Synapse trying to read SQL Server is expensive.

### Data Model Complexity
- questionAnswer tables very complicated
- JSON parsing, any errors will trickle down
- vwAnswerResponseChooseMany

### Scale & Regions
- Thousands of clients in Australia, UK, US each
- Hundreds in Germany
- Separate Azure databases for all these regions

### Internal Reporting
- Not much internal reporting, 5% of overall analytics used for internal
- For internal they just use unfiltered by default
- Product analysts use Yellowfin as host not tenants

### Data Privacy
- Personal information about clients is stored in those tables
- 10% of dbt models might have that data
- !!! GDPR requirements, retention policies

### Why Synapse (not Fabric)
- Fabric had issues connecting to Yellowfin
- Microsoft said Fabric integrates well with Power BI, not sure about Yellowfin
- Can use dedicated pools for Synapse to support Yellowfin
- Security restriction for Synapse serverless pools

### Issues and challenges I've noticed
- json fields, parsed in views and used in complex joins
- tenantId not present in all layers of views
- synapse reading those heavy views instead of cdc source tables

### Duplicated Business Logic in dbt Models

Searched dbt project and found significant code duplication - same business logic repeated across many files:

**1. RiskStatus Code Mapping** (16 files)
```sql
CASE RiskStatus 
  WHEN 0 THEN 'No Risk'
  WHEN 6 THEN 'Very Low Risk'
  WHEN 1 THEN 'Low Risk'
  WHEN 3 THEN 'Medium Risk'
  WHEN 4 THEN 'High Risk'
  WHEN 5 THEN 'Very High Risk'
END
```
Files include: `vwAnswer.sql`, `vAnswer.sql`, `vwQBA_QuestionAnswer.sql`, `vRisks.sql`, `vwQuestionOptionAnswerResponse_V3.sql`, etc.
⚠️ Inconsistent naming: "No Risk" vs "No risk", "Very High Risk" vs "Very High"

**2. JSON Parsing of ComponentStr** (8 files)
```sql
JSON_VALUE(ComponentStr, '$.RadioCustom')
JSON_VALUE(ComponentStr, '$.Radio')
JSON_VALUE(ComponentStr, '$.TextArea')
JSON_QUERY(ComponentStr, '$.MultiSelectValues')
```
Same parsing repeated in: `vwAnswer.sql`, `vAnswer.sql`, `vwQBA_QuestionAnswer.sql`, `vwAnswerResponse_source.sql`, etc.

**3. OPENJSON Array Explosion** (21 files!)
Used to unnest JSON arrays from ComponentStr - repeated in 21 different views across Assessment, Authority, Yellowfin Reports modules.

**4. Compliance Code Mapping** (4+ files)
```sql
CASE Compliance 
  WHEN 1 THEN 'Compliant'
  WHEN 2 THEN 'Not compliant'
  WHEN 3 THEN 'Partially compliant'
  ELSE 'None'
END
```

**5. Status Code Mapping** (multiple files)
```sql
CASE Status 
  WHEN 1 THEN 'Published'
  WHEN 2 THEN 'In Progress'
  WHEN 3 THEN 'Submitted'
END
```

**6. Question Type Mapping** (multiple files)
```sql
CASE Type 
  WHEN 1 THEN 'Yes No'
  WHEN 2 THEN 'Custom'
  WHEN 3 THEN 'Multiple Choice'
  WHEN 4 THEN 'Text Response'
  ...
END
```

**Impact of Duplication:**
- Risk of inconsistency (already seeing "No Risk" vs "No risk")
- Maintenance burden - changes need to be made in 16+ places
- If business adds new RiskStatus value, easy to miss files
- No single source of truth for domain logic

**Recommendation:**
- Parse JSON once in Silver layer (medallion architecture), reference downstream
- Example: `{{ get_risk_status_label('RiskStatus') }}` macro

## 4. Requirements

### Multi-region deployment
- whatever architecture needs to be able to be rolled out to different regions. Keep costs efficient

### Data Freshness
- freshness requirements
  - Hopefully in some use cases near real-time, other cases will be nightly refreshes
  - some synapse ones refresh every 20 min

### Data Governance

### Current Priority
- Shifting everything to analytics (new schema)

### Future Goals
- Would like to move to Spark pool to do more streaming


### Client Complaints
- How much time it takes to view the reports

## 5. Gap Analysis

### Synapse Issues
- Cost
- Efficiency
- Freshness
- Synapse trying to read SQL Server is expensive

## 6. Recommendations

idea: a 2 * 2 of sorts: value vs. effort?
### quick fixes
- push the tenantId down all views, use it in joins and filters everywhere possible
- 

### long-term ideas
- cdc data out
- dbt on synapse serverless to transform
- push back to sql server
    - synapse serverless itself could be an option too
    - all internal reporting better done from synapse itself
    - blue-green deployment for yellowfin db
- medallion architecture
- questions to consider
  - need to consider data retention and deletion too
  - one global or one per region? 

### principles - Data

- No analytics queries hitting production (we can make an exception in the short term and push our gold layer back to SQL server for YF queries if needed)
- No transformations on the operational database
- Use Serverless compute if possible to only pay for what we use
- Use incremental loads to only process changed data rather than full refreshes
- Medallion architecture; only gold layer available for repprting
  - **Bronze**: Raw CDC data, append-only, no transformations - just land the data
  - **Silver**: This is where the magic happens
    - Parse JSON **once** (ComponentStr → individual columns)
    - Explode arrays **once** (OPENJSON for multi-select → `question_options` table)
    - Apply soft-delete filters, deduplication
    - Currently: JSON parsing happens in 8+ files, OPENJSON in 21 files - every query re-parses
    - After: Parse once, reference everywhere downstream
  - **Gold**: Business logic and denormalization only
    - Just joins and CASE statements - no JSON, no explosions
    - Yellowfin queries simple tables, not 766-line views
    - TenantId baked in for access filtering
- Privacy and Data Residency: Raw data can’t go out of the region

### principles - Ways of Working
- How to eat an elephant? One bite at a time
  - Iterative development; ship incremental improvements rather than big-bang migrations
- Prefer managed services over self-hosted infrastructure; minimize platform maintenance so the team can focus on data engineering and delivering value to customers
- Keep it simple; avoid over-engineering
- Observability first; instrument pipelines with logging, metrics, and alerting from day one
  - Enables proactive issue detection and faster root cause analysis when things go wrong
- Infrastructure as code; define all resources in version-controlled templates (e.g., Bicep, Terraform)
  - Ensures reproducibility across environments and provides an audit trail of infrastructure changes

### Ideas to Explore
- !!! CDC somewhere and run ETL and push back, all dbt should be off the CDC files, not SQL Server. Can we run the existing dbt project on that? How does CDC schema look like? Synapse is doing a sink to parquet, but reading from the views, not source table. Can it run a dbt job to materialize everything? Also blue/green for pushing to Yellowfin schema. All Yellowfin from a different database or schema.
- !!! Make sure access filter (tenantId) is pushed down different layers

### process and ways of working

- CI/CD for yellowfin?
- data quality checks / dbt tests

### data governance

## 7. Roadmap & Risks

## Appendices



Amir notes
- create a roadmap, make team accountable, then tell them if the team can deliver or not
   - what skills do the team have, what's missing, who they need
   - 
- consulting hat
  - deliver some small value in each convo 
- my goal is: you give me budget and priority, and I'll fix your data issue
  - show him you are accountable and have ownership