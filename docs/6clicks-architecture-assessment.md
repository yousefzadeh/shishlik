# 6clicks Data Architecture Assessment

## 1. Executive Summary

## 2. Introduction

- focus on data warhousing and yellowfin backend
- self-serve analytics


## 3. Current State Assessment

#TODO what are my goals in this assessment
- current setup
- exsiting initiatives
- business goals
- business problems and piroirty
- technology gaps
- resource strengths and opportunity

- data size
  - millions of rows
  - thousands of clients, with 10,000s rows max
- freshness requirements
  - Hopefully in some use cases near real-time, other cases will be nightly refreshes
  - some synapse ones refresh every 20 min
- Have had customers churn because reporting wasn’t working

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
- questins to consider
  - need to consider data retention and deletion too
  - one global or one per region? 

### principles
- move away from any logic in views, at most they should flatten some common joins -> performance impact
- cdc raw data out, ideally in delta lake format
- no analytics queries on production database -
- user serverless where possible
- cluster / partition per client for performance

### Ideas to Explore
- !!! CDC somewhere and run ETL and push back, all dbt should be off the CDC files, not SQL Server. Can we run the existing dbt project on that? How does CDC schema look like? Synapse is doing a sink to parquet, but reading from the views, not source table. Can it run a dbt job to materialize everything? Also blue/green for pushing to Yellowfin schema. All Yellowfin from a different database or schema.
- !!! Sharding?
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