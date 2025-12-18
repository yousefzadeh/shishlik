# 6clicks Engagement - AI Prompt

I've got an idea I want to talk through with you.
Use your knowledge and search tool if needed to understand where we're starting off, then ask me questions, one at a time, to help refine the idea. 
Ideally, the questions would be multiple choice, but open-ended questions are OK, too. Don't forget: only one question per message.
Once you believe you understand what we're doing, stop and describe the plan to me, in sections of maybe 200-300 words at a time, asking after each section whether it looks right so far.

**Context:** I'm doing a 30-hour data architecture consulting engagement with 6clicks, a GRC (Governance, Risk & Compliance) SaaS company. They have a small data engineering team of 3. The deliverable is a written assessment covering current state, gaps, and recommendations. They're on Azure, using SQL Server as their operational database with dbt (open source) for transformations, Synapse for ETL orchestration, and Yellowfin for embedded customer-facing analytics. The core problem is analytics queries hitting production SQL Server, causing performance issues.

**Key points:** Most dbt models are views running against prod SQL Server. Synapse is only used for pipeline orchestration, not as a warehouse. Yellowfin has row-level tenant filtering. They've done recent work to split complex views and move the worst offenders (like Assessment module) to scheduled table refreshes. They have separate Azure databases per region (AU, UK, US, Germany). Data volumes are modest (at most 1M rows, tenants have 10s of thousands). GDPR and data retention is a consideration. 