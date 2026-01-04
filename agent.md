# 6clicks Engagement - AI Prompt

I've got an idea I want to talk through with you.
Use your knowledge and search tool if needed to understand where we're starting off, then ask me questions, one at a time, to help refine the idea. 
Ideally, the questions would be multiple choice, but open-ended questions are OK, too. Don't forget: only one question per message.
Once you believe you understand what we're doing, stop and describe the plan to me, in sections of maybe 200-300 words at a time, asking after each section whether it looks right so far.

**Context:** I completed a 20-hour data architecture consulting engagement with 6clicks, a GRC SaaS company (small data team of 3). The assessment and roadmap are in `docs/shared/6clicks-roadmap-cto.md`. Now moving to implementation: guiding them through a 4-week Databricks POC in a PoC region. Goal is to validate the architecture and understand implications before multi-region rollout.

**Current state:** Azure stack—SQL Server (prod), dbt (transforms), Synapse (orchestration only), Yellowfin (embedded analytics with row-level tenant filtering). Core problem: analytics queries hitting prod SQL Server. Separate databases per region (10 regions). Modest data volumes (~1M rows max).

**POC focus:** Databricks Serverless SQL with CDC from SQL Server, optionally implement medallion architecture (Bronze→Silver→Gold), dbt for transforms. CTO prefers Infrastructure as Code using Bicep. Target model: `vwQBA_QuestionAnswer` (already on Synapse, enables direct comparison).

**Folder structure:**
- `docs/discovery/` — my discovery notes from client interviews
- `docs/shared/` — docs shared with the client
- `docs/client-docs/` — docs, notes, diagrams the client has shared with me
- `meeting-notes/` — random meeting notes 