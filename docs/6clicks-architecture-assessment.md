# 6clicks Data Architecture Assessment

## 1. Executive Summary

## 2. Introduction

- First around data warehouse, what we do with views on top of operational database, what we do with synapse
- At most 1 million rows in the database  
  - No tenant has a million rows, a tenant will have 10s of thousand  
    - Want the users to be able to self-serve analytics in a user friendly manner  
    - Hopefully in some use cases near real-time, other cases will be nightly refreshes  
      - At the moment it’s on top of operational database so they get immediate freshness

## 3. Current State Assessment

### dbt Setup
- Used to have a team from an agency that helped set up dbt (open source)
- Complex design, starting this year started to build a separate schema. Tried to keep it minimal. Exactly what is needed
- Most of dbt are views, three or four that are impossible to run in real time are tables
- Those four run once a day
- Scheduled in a cronjob in Azure DevOps
- ?? Once we build these models we push to Synapse -> staging -> production

### SQL Server & Synapse
- Everything is SQL Server. Synapse is only used for ETL pipeline architecture. No pool usage, still connecting all source and destination to SQL Server.
- Just using Synapse to accelerate deployment
- Yellowfin points to SQL Server
- Pipeline architecture is basic using only ETL pipelines, haven't used advanced features

### Yellowfin
- Row-level filter is set up in Yellowfin as access filter (based on tenant id)
- !!! Future work: CI/CD this with some checks

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

## 4. Requirements

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

### Ideas to Explore
- !!! CDC somewhere and run ETL and push back, all dbt should be off the CDC files, not SQL Server. Can we run the existing dbt project on that? How does CDC schema look like? Synapse is doing a sink to parquet, but reading from the views, not source table. Can it run a dbt job to materialize everything? Also blue/green for pushing to Yellowfin schema. All Yellowfin from a different database or schema.
- !!! Sharding?
- !!! Make sure access filter (tenantId) is pushed down different layers

## 7. Roadmap & Risks

## Appendices
