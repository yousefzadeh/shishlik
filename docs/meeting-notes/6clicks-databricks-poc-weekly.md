# Monday 12 Jan
## Update from Anjali
First update on the status of our PoC workstreams:

**In Progress:**

- Databricks workspace setup (Platform team)
- ADLS storage configuration (Platform team)
- Azure Data Factory setup (Platform team)

**Not Yet Started:**

- Enabling Change Data Capture (CDC) on SQL Server source tables (Data Team)
- Testing Databricks to Yellowfin connectivity (Option A) (Data Team)

**To be Started:**

- Databricks workspace being provisioned – Anjali to begin testing the Yellowfin ↔ Databricks connection (Option A).
- CDC enablement for DB and Tables – Jake to start off on that today

**Current Blocker:**

- ADF Integration Runtime decision: Shankar has raised a question about whether we should use our own self-hosted Integration Runtimes (like our Synapse implementation) versus Azure’s managed runtime. This decision will be driven by pipeline activity requirements. - @Reza Yousef / @Jake Costa this might be something to discuss during the call
 

Resolution for the ADF question will help us to move forward with the ADF pipeline setup.

## Pre-meeting notes

**Questions re: Integration Runtime decision:**

- How is the current Synapse self-hosted IR set up? Is it deployed in all regions or just some?
- What's the current cost of running those self-hosted IRs (VMs)?
- How easy/hard would it be to set up networking for managed IR? (private endpoints, etc.)
- If we retire Synapse eventually, does that free up cost that could offset managed IR or other Databricks costs?

- Suggestion: if networking too hard, start with self-hosted
- Decision: Ryan instructed Shankar to go with IR

## Notes
- Platform
    - Created workspace and data lake
    - creating ADF factory
    - CDC: joel has enabled for the tables
    - In a position to hook things up. all the things that can are done via bicep.
        - created the connector to the catalog
    - enabled cdc, didn't look into change tracking
- Data
    - working on both development and production

- Notes
    - Set up ADLS containers and ADF in dev first, to test any impact on database (all bicep so should be ok)

- Next steps
    - Jake to enable cdc on those tables

- Two options
    - test CDC and ADF on dev, everything else in prod
    - test everything in dev (needs platform team to set up separate dev workspace)
- Emphasis: let's not have any dev data going into production
- CDC -> ADF -> ADLS 
    - bronze will be parquet