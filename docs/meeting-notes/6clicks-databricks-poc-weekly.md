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
