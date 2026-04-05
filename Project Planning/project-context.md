# VacciGuard Project Context

## Purpose
This file is the shared live context for the whole project.

It should answer:
- what is already done
- what is currently in progress
- what is still pending
- what is blocked
- what each member is expected to do next

This file is **not** meant to replace the detailed execution guides.  
It is meant to keep the team and the project in sync as work progresses.

## How This File Should Be Used
Each member should treat this file as the shared status board for the project.

The detailed "how to do the work" instructions remain in:
- [alok-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/alok-execution-guide.md)
- [aayush-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/aayush-execution-guide.md)
- [monty-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/monty-execution-guide.md)

This file should remain short, current, and status-oriented.

## Update Discipline
This is how the team should keep the file useful.

### Each member must update their section:
- when they start a meaningful new task
- when they hit a blocker
- when they finish a coordination gate
- when their output is ready for another member
- at the end of a work session if their status changed

### Each section should always contain:
- `Last Updated`
- `Done`
- `In Progress`
- `Pending`
- `Blocked By`
- `Output Ready For Others`
- `Next`

### Team rule
No one should claim a handoff or gate completion until this file is updated.

### Codex update rule
When a teammate completes a meaningful project step **with my help in this workspace**, I should
update this file in the same turn unless the user explicitly tells me not to.

This means I should update:
- the relevant member section
- the shared technical state if needed
- the baseline gate status if a gate was completed
- the "Next" or "Blocked By" fields if the completion changes team dependencies

Important limitation:
- I can only do this for work I actually help complete here
- if a teammate finishes work outside this conversation or without my involvement, someone must tell
  me so I can record it accurately

### Honest note
I can help keep this file well-structured and I can update it when you ask me, but I cannot force
human teammates to maintain it automatically. The practical way to ensure it stays synced is:
- make it the required handoff record
- review it at the start of team check-ins
- have Tanish treat it as the first file to inspect before asking me for the next coding task

## Current Project State
- `Current phase`: baseline pipeline implementation preparation
- `Current focus`: build the baseline pipeline first, then freeze it before optimization work
- `Architecture status`: locked
- `Schema status`: frozen
- `Folder structure status`: created
- `Member execution guides status`: Alok, Aayush, and Monty guides created
- `Baseline coordination status`: shared coordination plan created

## Baseline Gate Status
- `Gate 1: Schema Freeze` -> done
- `Gate 2: Processing Contract Ready` -> done at planning level, implementation pending
- `Gate 3: AWS Base Ready` -> pending
- `Gate 4: Replay Smoke Works` -> pending
- `Gate 5: Stream Smoke Works` -> pending
- `Gate 6: Batch Smoke Works` -> pending
- `Gate 7: Baseline Deployment Freeze` -> pending
- `Gate 8: Main Workload Freeze` -> pending
- `Gate 9: Baseline Sign-Off` -> pending

## Shared Technical State

### Done
- problem framing and scope are documented
- conceptual pipeline is documented
- dataset plan is documented
- technology stack is locked
- AWS deployment direction is locked
- monitoring direction is locked
- project folder structure is created
- input schema is frozen
- starter dataset templates exist
- architecture HTML overview exists

### In Progress
- baseline team coordination setup
- member-by-member execution planning

### Pending
- replay producer implementation
- Spark stream processor implementation
- Spark batch processor implementation
- Terraform implementation
- EKS deployment manifests
- baseline runtime profile implementation
- monitoring dashboards implementation

## Member Sections

## Tanish
### Last Updated
- `2026-04-05`

### Done
- project planning structure created
- scope and methodology documentation prepared
- architecture visual prepared
- folder structure defined
- schema freeze and starter artifacts created
- member execution guides started and shared
- baseline coordination flow documented

### In Progress
- team coordination
- baseline acceptance framing
- project context setup

### Pending
- end-to-end integration work
- final baseline freeze sign-off
- evaluation summary framework
- optimized comparison framework

### Blocked By
- runnable replay producer from Alok
- runnable Spark stream and batch jobs from Aayush
- usable AWS baseline environment from Monty

### Output Ready For Others
- planning documents
- architecture overview
- folder structure
- schema freeze
- coordination plan

### Next
- use this file as the central sync board
- review the first real implementation outputs as soon as they appear

## Alok
### Last Updated
- `2026-04-05`

### Done
- role and execution guide defined
- schema freeze available for use
- sample lookup, batch, and telemetry template files available
- workload folders created

### In Progress
- baseline task definition only; implementation not started yet

### Pending
- full lookup dataset creation
- full batch dataset creation
- workload-family generation
- precomputed workload files
- replay producer implementation
- replay smoke validation
- baseline workload freeze

### Blocked By
- no blocker to start early implementation
- later depends on Monty's AWS environment for serious cloud validation

### Output Ready For Others
- frozen schema and starter templates are already ready for Aayush and Monty

### Next
- start building the lookup dataset and workload files
- begin replay-producer implementation
- prepare for `Gate 4: Replay Smoke Works`

## Aayush
### Last Updated
- `2026-04-05`

### Done
- role and execution guide defined
- processing contract is documented at planning level
- stream and batch processing approach is clearly described
- sink behavior for Redis and S3 is explained

### In Progress
- baseline task definition only; implementation not started yet

### Pending
- stream processor implementation
- batch processor implementation
- validation logic implementation
- deduplication logic implementation
- enrichment logic implementation
- breach logic implementation
- Redis/S3 output implementation
- stream smoke test
- batch smoke test

### Blocked By
- no blocker to start early implementation
- later depends on Alok's real workload files and Monty's deployed sinks/runtime

### Output Ready For Others
- processing design is ready for Monty to understand deployment expectations

### Next
- start coding validation and enrichment flow using the frozen schema
- begin the stream processor first
- prepare for `Gate 5: Stream Smoke Works`

## Monty
### Last Updated
- `2026-04-05`

### Done
- role and execution guide defined
- AWS, EKS, monitoring, and autoscaling decisions are documented
- infrastructure folder structure is ready
- baseline versus optimized separation is documented

### In Progress
- baseline task definition only; infrastructure implementation not started yet

### Pending
- Terraform skeleton
- AWS base setup
- S3 provisioning setup
- ElastiCache Redis provisioning setup
- EKS setup
- baseline deployment manifests
- monitoring deployment
- Airflow deployment
- baseline runtime freeze
- scaling and failure test setup

### Blocked By
- no blocker to start AWS foundation work
- later depends on Alok for replay behavior and Aayush for runnable Spark jobs

### Output Ready For Others
- deployment direction is ready in planning form only; no runnable infra output yet

### Next
- start Terraform and AWS base setup
- prepare the baseline deployment shape before autoscaling work
- prepare for `Gate 3: AWS Base Ready`

## Active Blockers
- no hard blocker prevents the team from starting baseline implementation now
- the main current gap is that implementation work has not started yet even though planning is in good shape

## Next Coordination Gate
- `Gate 3: AWS Base Ready` from Monty

This is the next high-value gate because it unlocks serious AWS-side baseline work while Alok and
Aayush continue implementation in parallel.

## What Should Be Updated Next
- Alok should update his section when workload generation begins
- Aayush should update his section when stream-processor coding begins
- Monty should update his section when Terraform or EKS work begins
- Tanish should update this file after the next baseline coordination checkpoint
