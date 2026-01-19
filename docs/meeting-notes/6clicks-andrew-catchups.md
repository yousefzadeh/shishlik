# Friday 16 Jan

**Topics to cover:**
- POC technical direction
  - Deep-dived 770-line vwQBA_QuestionAnswer query
  - Medallion architecture: Landing → Bronze (Delta) → Silver/Gold (views)
  - Start simple, upgrade to MVs only if needed
  - Using DAB for IaC
- Slow reports fix
  - Clustering on TenantId = scan only relevant data
  - Views first, test performance quickly
- Observability & access control
  - Monitoring plan? (query perf, job failures, costs)
  - Unity Catalog RBAC for access control?
  - Suggestion: data team member owns Databricks platform day-to-day
- Status
  - IR decision ✓ unblocked
  - Platform ahead, data catching up
  - Next: CDC → ADF → ADLS, then Bronze tables
- Ask: "What does success look like in 4 weeks?"

**Notes:**
- lakebase vs. sql warehouse
  - stick with serverless sql for the poc
- now we have 11 region
  - rolled out 7 last year. all went quite well
  - doing qatar and it's a nightmare all of a sudden. qatar is an extremely restricted region. even getting a VM is a big delay
  - !!!! need to make sure all goes well with Qatar -> is databricks avaialbe there?
- DAB is ok
- ongoing ownership -> nice for data team to own it
- look into some changes in the data team
   - will need to discuss this further
- RBAC: don't worry about it yet
  - government regions need security clearance to access
  - end goal is no human with account admin
- ADR -> no specific template, we put them in the wiki. use whatever format that works.

# Friday 19 Dec
- Started with synapse on its own server, was costing thousands per month  
- Strategy  
  - Big part is multi-region, whatever architecture needs to be able to be rolled out to different regions. Keep costs efficient  
  - Doing more AI stuff, vector databases  
  - Any nlp use cases?  
    - Could see that happening. Next 3-6 months lots of similarity searches. Not much training  
- Flagship regions AU and UK  
- Analytics across regions is not possible right now  
  - Raw data can’t go out of the region  
- 26 plan  
  - Before we proceed any further, let’s make sure we evaluate these options  
  - Let’s sort out the decision first  
    - Framework for making the data warehouse direction:   
      - synapse vs. databricks vs. fabric  
      - What are principles, criteria and requirements  
        - Cost  
        - Security  
        - Team skills  
        - Maintenance  
        -   
    - Does it scale across different regions  
      - Can the cost be manageable  
      - Is it appropriate for this solution?  
    - tentantId and partitioning  
      - The one we currently have is the spoke, and the hub can access those