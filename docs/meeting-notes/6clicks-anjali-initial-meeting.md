Questions

- Current 2026 plan?  
- Team’s day to day activities  
- Analytics schema?  
- Issues of the past and things the team has tried  
- Synapse serverless pools

Notes

- Have had customers churn because reporting wasn’t working  
  - Have a good team, just need a path  
- Team  
  - Jake knows snowflake, etc  
  - Bhanu more sql, one who picked up synapse  
  - Sai from data analyst background, came as BA, knows the business domain well. Most proficient yellowfin user  
- Transitioned from minera to manage it in house  
- Small team, couldn’t do major initiatives  
- All major efforts were to make sure the user can see the reports in an efficient way  
  - Sometimes it would take 25 minutes for a report to load  
- Setting up reports  
  - Set of standard reports  
    - The user can make cosmetic changes  
  - Evolved into self-service, we created self-service views and gave them permission to create reports  
    - The yellowfin view is combined from different dbt views  
  - Have organized dbt modules around the reporting modules in the ui  
    - People who initially built these views didn’t have this view  
    - They built very inefficient views  
    - Then decided to bring in indexed views, it was slowing down the application  
  - Consulted microsoft to see if there are other tools  
    - That’s what led us to synapse  
    - Rolled out to assessment module   
      - Requirement based (RBA)  
      - Questionnaire based (QBA)  
    - Got minimal support from microsoft  
      - Copy the data from operational data store into parquet tables in syanpse: materialize tables into a new schema in sql server  
        - Reading materialized tables reduced from 20 minutes to 2 or 3\. Still want it under 5 seconds  
          - We see a way of incremental cdc into synapse  
        - From march started moving to synapse  
          - Small team until march, no QA, just anjali and Sai  
          - Got bhanu and Jake mid-year  
        - We do some mop programming, to go through obsolete views and rewrite them  
        - Major bottleneck is moving existing customers from old views  
- Reasons for dbt views  
  - Faster to develop  
  - Didn’t need to do incremental updates, etc. get real-time data into the report  
- Usage statistics  
  - Yes, we check slow queries and move them to more optimized schema (reactive)  
- TenantId  
  - Looked into segregating data for different tenants  
    - Yellowfin only allows bringing data only for tenant only via api  
- Next steps  
  - New dbt project with analytics and reporting schemas  
  - docs

My own notes

- Create a checklist for the dbt project, like security, etc.