# Sai and Bhanu, 15 Dec.
  
Used to have a team from an agency that helped set up dbt (open source)

- Complex design, starting this year started to build a separate schema. Tried to keep it minimal. Exactly what is needed  
  - Most of dbt are views, three or four that are impossible to run in real time is tables  
    - Those four run once a day  
    - Scheduled in a cronjob in azure devops  
- Once we build these models we push to synapse \-\> staging \-\> production  
  - Everything is sql server. Synapse is only used for etl pipeline architecture. No pool usage, still connecting all source and destination to sql server.  
  - Just using synapse to accelerate deployment   
- Yellowfin points to sql server  
  - Row-level filter is set up in yellowfin as access filter (based on their tenant id) \!\!\! future work: ci/cd this with some checks

Some solutions that have been implemented

- Moving to a different schema  
  - Everything was in one model  
  - With new schema we tried to divide the context models, reduce the volume by dividing one model into 2 or 3\. Reduce amount of nesting in views.  
  - Used to take 10-20 seconds, now runs in 1 second

Most problematic reports

- Assessment module: very complex, taking around 5 minutes. We tried to shift it to synapse.  
  - Taken away the real-time nature and shifted the entire thing to tables and indexes. Synapse is maintaining the schedule intervals. We split all modules into what can be refreshed once a day. Assessment module is working for the clients now. It’s not that frequently used, so they’re happy with it. Able to generate in less than 7 seconds. Synapse cost is high, so won’t scale. Cost is not synapse itself, how it’s reading, back to views. Synapse trying to read sql server is expensive. Pipeline architecture is basic using only ETL pipelines, we haven’t used advanced features.  
    - Issues with synapse is cost, efficiency, freshness   
      - \!\!\!idea: cdc somewhere and run etl and push back, all dbt should be off the cdc files, not sql server. Can we run the existing dbt project on that?  How does cdc schema look like? Synapse is doing a sink to parquet, but reading from the views, not source table. Can it run a dbt job to materialize everything? Also blue/green for pushing to yellowfin schema. All yellowfin from a different database or schema.  
      - \!\!\!idea : sharding?  
      - \!\!\!idea: make sure access filter is pushed down different layers  
  - Priority right now is shifting everything to analytics (new schema)  
    - Would like to move to spark pool to do more streaming  
- questionAnswer tables very complicated  
  - Json parsing, any errors will trickle down  
  - vwAnswerResponseChooseMany  
- Client complaints  
  - How much time it takes to view the reports  
- Internal reporting?  
  - 

How many clients

- Thousands in australia, uk, us each  
- Hundreds in germany  
- Separate azure databases for all these regions

How does internal reporting work

- Not much internal reporting, 5% of overall analytics used for internal. For internal we just use unfiltered by default. Product analysts use yellowfin as host not tenants.

Any personal information about clients 

- Yes it’s stored in those tables. 10% of dbt models might have that data.  
- \!\!\!GDPR requirements, retention policies.

Why synapse

- Fabric had issues connecting to yellowfin  
  - Microsoft said fabric integrates well with powerbi, not sure about yellowfin. We can use dedicated pools for synapse to support yellowfin. Security restriction for synapse serverless pools.

Compromises

- Some things couldn’t be removed

Followups and request

- Share the dbt code  
  - Try to look at dbt lineage  
- Synapse pipeline code  
- Team skills \-\> apache spark?  
- Need to do some reconciliations testing as well