# 6clicks Data Architecture Engagement Plan

## Overview

- **Client:** 6clicks — GRC (Governance, Risk & Compliance) SaaS company
- **Engagement:** 30 hours over 3-4 weeks
- **Deliverable:** Written assessment/report (current state, gaps, recommendations)
- **Attendees (kickoff):** CTO + 3-person data engineering team
- **Environment:** Azure-based
- **Core Problem:** Yellowfin embedded analytics hitting production SQL Server, causing performance issues
- **Goal:** Build a proper data warehouse to separate analytics from production

---

## Section 1: Kickoff Meeting Structure (30 minutes)

### 0-5 min: Set the tone

Quick intros, then frame the session:

> "My goal today is to understand where you are, where you're thinking of going, and how I can be most useful — whether that's validating your approach, identifying blind spots, or helping shape the path forward."

---

### 5-15 min: Listen to their story

- "Walk me through how data flows today — from the application to what customers see in Yellowfin."
- "What's the pain you're feeling most acutely right now?"
- "How long has this been a problem, and what's driving the urgency now?"

---

### 15-22 min: Understand their thinking

Find out if they already have a direction:

- "Have you already started evaluating solutions or architectures? What are you leaning toward?"
- "Is there a specific approach you want me to validate, or are you looking for me to propose options?"
- "What would make this engagement a success for you — confirmation you're on the right track, or a fresh perspective?"

Then nudge toward broader thinking:

- "Beyond the Yellowfin performance issue, are there other use cases you'd want a data warehouse to support — internal reporting, product features, ML?"

Then probe non-functional requirements:

- "Do you have a budget range in mind for the data warehouse infrastructure? Monthly or annual?"
- "What are your expectations for data freshness — real-time, near-real-time, hourly, daily?"
- "Are there any SLAs you need to meet for customer-facing analytics in Yellowfin?"
- "Any security or compliance requirements I should know about — data residency, encryption, audit logging?"
- "How much data are we talking about — rough order of magnitude? GBs, TBs?"

---

### 22-28 min: Align on engagement structure

- Share proposed approach (see Section 2)
- Confirm deliverables
- Agree on access needed (repo, database, docs, any existing design docs or spikes)
- Schedule follow-up sessions

---

### 28-30 min: Next steps

- Confirm actions and owners
- Confirm next meeting date

---

## Section 2: Engagement Proposal — Phases and Hour Allocation

The structure flexes depending on what you learn in the kickoff.

### Variant A: They have a direction — validation focus

**Phase 1: Discovery & Review (6-8 hours)**
- Kickoff meeting (0.5 hrs)
- Review their existing plans, spikes, or architecture proposals (2-3 hrs async)
- Deep-dive session: understand their reasoning, assumptions, and constraints (2 hrs)
- Clarifying follow-up (1-2 hrs)

**Phase 2: Validation & Gap Analysis (12-14 hours)**
- Assess their proposed approach against best practices and their specific context
- Identify risks, blind spots, or missing considerations
- Evaluate alternatives only where their approach has gaps
- Validate (or challenge) tooling choices given Azure stack and team size
- Document findings

**Phase 3: Report & Readout (8-10 hours)**
- Write assessment: "here's what you've got right, here's what to watch out for, here's what I'd adjust"
- Provide a refined roadmap if needed
- Final presentation (1.5 hrs)
- Feedback incorporation and final delivery (2 hrs)

---

### Variant B: They're starting fresh — discovery focus

**Phase 1: Discovery (8-10 hours)**
- Kickoff (0.5 hrs)
- Deep-dive with data team on current pipelines, schema, pain points (2 hrs)
- Async review of codebase, schema, Yellowfin setup (4-5 hrs)
- Follow-up questions (1-2 hrs)

**Phase 2: Analysis & Options (10-12 hours)**
- Map current state architecture
- Identify gaps and requirements
- Propose target architecture options (likely 2-3 approaches)
- Evaluate fit for team size and Azure environment
- Draft recommendations

**Phase 3: Report & Readout (8-10 hours)**
- Write assessment document
- Architecture diagrams (current vs. proposed)
- Final presentation (1.5 hrs)
- Feedback incorporation and final delivery (2 hrs)

---

**Buffer:** Keep 2 hours unallocated for unexpected follow-ups or deeper dives.

---

## Section 3: Deliverable — Assessment Report Structure

### 1. Executive Summary (1 page)
For the CTO. Crisp summary: what we found, what we recommend, and why. No jargon. This is the "read this if you read nothing else" section.

### 2. Current State Assessment (3-5 pages)
- Architecture diagram: data sources → ETL → SQL Server → Yellowfin
- How data flows today, including key tables/views that Yellowfin hits
- Pain points identified: performance issues, bottlenecks, technical debt, scalability limits
- What's working well (important to acknowledge)

### 3. Requirements Summary (1-2 pages)
- Functional requirements: use cases the warehouse needs to support
- Non-functional requirements:
  - Budget constraints
  - Data freshness / latency expectations
  - SLAs for customer-facing analytics
  - Data volumes and growth projections
  - Security / compliance requirements (data residency, encryption, audit)

### 4. Gap Analysis (2-3 pages)
- Where the current setup falls short against their goals
- Risks of staying on the current path
- Missing capabilities (e.g., no separation of OLTP/OLAP, no proper transformation layer, no data modeling)

### 5. Recommendations (3-5 pages)
- Proposed target architecture with diagram
- Technology recommendations (with rationale — e.g., "Synapse because you're Azure-native and team is small")
- What to build vs. buy
- How Yellowfin connects to the new architecture
- Cost estimate for recommended approach

### 6. Roadmap (2-3 pages)
- Phased approach: quick wins first, foundational work, then expansion
- Suggested timeline and milestones
- Dependencies and risks
- Team considerations: what a team of 3 can realistically deliver

### 7. Appendix
- Detailed notes, schema observations, any raw findings

---

## Section 4: What to Request in the Kickoff

### Technical Access
- Read access to the SQL Server database (or at minimum, schema documentation)
- Access to the ETL codebase/repo — wherever the current pipelines live
- Access to Yellowfin — to see the dashboards and the views/queries it runs
- Any existing architecture diagrams or documentation (even if outdated)

### Context Documents
- Any previous spikes, proposals, or tech evaluations they've done
- List of the most problematic queries or reports (the ones killing prod)
- Rough data volumes: how many records, how fast is it growing

### People Access
- Confirm who your main point of contact is on the data team
- Ask if there's anyone else you should talk to (e.g., someone who owns Yellowfin config, or a backend engineer who knows the app's data model)

### Logistics
- Preferred communication channel (Slack, email, Teams?)
- How they want to schedule follow-up sessions
- Where to submit/share the final report

---

## Post-Kickoff Action

After the meeting, send a short email summarizing:
- What you heard (their situation and goals)
- What you agreed on (engagement structure, deliverables, timeline)
- What access/materials are pending
- Next steps and meeting schedule

This confirms alignment and gives them a chance to correct anything before you dive in.
