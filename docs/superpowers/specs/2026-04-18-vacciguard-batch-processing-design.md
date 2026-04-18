# VacciGuard Batch Processing Design

**Date:** `2026-04-18`  
**Branch:** `baseline-spike-fix`  
**Owner:** `Codex + Tanish`

---

## Goal

Add a true batch-processing layer to VacciGuard that operates on historical data already archived to S3, rather than on live Kafka events. The batch layer must support manual triggering for demo and report purposes, align with the professor's workflow-scheduling requirement, and produce useful historical outputs for compliance reporting and audit analysis.

The design keeps the existing optimized streaming pipeline unchanged:

- `optimized-hot` remains responsible for low-latency Redis updates
- `optimized-cold` remains responsible for continuous archival writes to S3
- the new batch layer runs separately on top of archived data

---

## Problem Summary

The current optimized pipeline already supports:

- real-time processing through the hot path
- archival output generation through the cold path
- historical data persistence in S3

However, the project brief expects not only stream processing, but also explicit batch-oriented workflow execution. The present cold path is still a streaming service with a slower trigger interval. Although it produces archival outputs, it is not a true storage-first, scheduled batch workflow in the classical data-engineering sense.

This leaves two gaps:

1. the project does not yet demonstrate a clearly schedulable batch-processing workflow over historical data
2. the project does not yet fully satisfy the explicit `Workflow scheduling (Airflow / Dagster)` requirement from the brief

---

## Recommended Approach

The best-fit solution is to add a manually triggered Airflow workflow that launches a batch analytics job over archived S3 outputs.

The new batch workflow will:

- read historical datasets already produced by the optimized cold path
- compute compliance summaries and audit summaries
- write the derived summary datasets back to S3
- run independently from the low-latency hot path

### Why this approach

- It creates a real batch layer without disturbing the existing streaming architecture.
- It aligns directly with the professor's batch-processing definition: storage-first, historical, scheduled or manually launched, non-real-time.
- It fills the explicit Airflow/Dagster workflow requirement.
- It is small enough to implement quickly and explain clearly in the report.

### Alternatives considered

#### 1. Treat the current cold path as the batch layer

Pros:
- no extra code
- already writes archival outputs

Cons:
- not a true batch workflow
- still streaming internally
- weaker academic alignment with the batch-processing definition

#### 2. Build a second standalone offline batch script without Airflow

Pros:
- simpler than full workflow orchestration
- fast to implement

Cons:
- does not satisfy the Airflow/Dagster requirement well
- weaker orchestration story
- less professional for demo and report purposes

This design chooses the stronger option: a real manually triggered Airflow-based batch workflow over stored historical data.

---

## Architecture

### Existing Streaming Layer

The current streaming architecture remains unchanged:

- `optimized-hot` handles live Kafka-to-Redis state updates
- `optimized-cold` handles continuous Kafka-to-S3 archival outputs

These services continue to serve real-time and near-real-time needs.

### New Batch Layer

The new batch layer runs after archival storage and is composed of:

- one Airflow DAG
- one batch analytics job
- two derived S3 summary outputs

The Airflow DAG will be manually triggered and will orchestrate the batch job. The batch job will read stored historical data from S3 and generate analytical summaries suitable for reporting, compliance review, and audit use cases.

### Batch Inputs

The batch workflow will reuse the existing archived outputs:

- `processed/` parquet
- `invalid/` json
- `breach_windows/` json

### Batch Outputs

The batch workflow will produce:

- `daily_compliance_summary/`
- `daily_audit_summary/`

These outputs will also be written to S3 so they become part of the data lake and can be referenced in reports and demos.

---

## Data Flow

### Streaming Flow

`Kafka -> optimized-hot -> Redis`

`Kafka -> optimized-cold -> S3 archive outputs`

### Batch Flow

`S3 archived outputs -> Airflow-triggered batch job -> summary outputs in S3`

This separation gives the project a true hybrid architecture:

- streaming for live monitoring
- batch for historical analysis

### Why the Separation Matters

The batch workflow must not compete with the SLA-critical hot path. By reading from already archived S3 outputs, the batch job works on stored data rather than on the live Kafka stream. This makes it both academically valid as batch processing and operationally safe for the existing optimized design.

---

## Batch Outputs

### Daily Compliance Summary

The compliance summary is intended to answer operational questions such as whether facilities and devices stayed within expected storage conditions.

Planned fields:

- `event_date`
- `facility_id`
- `facility_name`
- `total_processed_events`
- `safe_events`
- `breach_events`
- `breach_rate_pct`
- `avg_temperature_c`
- `min_temperature_c`
- `max_temperature_c`
- `unique_devices_seen`

### Daily Audit Summary

The audit summary is intended to answer data-quality and historical breach-analysis questions.

Planned fields:

- `event_date`
- `invalid_events_total`
- `invalid_unknown_device`
- `invalid_corrupt_payload`
- `invalid_missing_fields`
- `breach_window_count`
- `facilities_with_breaches`
- `devices_with_repeated_breaches`

### Design Intent

Together, these two outputs give the project a meaningful batch-processing story:

- compliance reporting from historical operational data
- audit and quality analysis from invalid and breach-related historical records

---

## Execution Model

The initial version will be manually triggered rather than scheduled on a timer.

### Why Manual Trigger First

- better suited to report and demo use
- easier to validate in a controlled way
- avoids unnecessary scheduling complexity close to the deadline
- still demonstrates workflow orchestration through Airflow

The DAG can later be extended with a real schedule if desired, but that is not required for the first implementation.

### DAG Behavior

The Airflow DAG will:

- accept or resolve the relevant S3 input prefix
- launch the batch analytics job
- wait for completion
- record or print the generated output paths
- succeed or fail as a single workflow execution

This keeps Airflow as the scheduler/orchestrator and keeps the batch logic inside the batch job itself.

---

## Implementation Shape

The implementation should stay intentionally small and reuse the existing evaluation and AWS infrastructure patterns.

### Components

- one Airflow DAG
- one batch analytics script or job entrypoint
- one pair of S3 output prefixes for the summaries
- lightweight tests for batch summarization logic and DAG wiring

### Boundaries

The new feature should not:

- modify the optimized hot path behavior
- modify the optimized cold path behavior
- alter Redis state ownership
- change the existing evaluation-controller report contract

Instead, it should extend the platform by adding a separate historical analytics capability.

---

## Reliability And Error Handling

The batch workflow should fail clearly if:

- required input prefixes are missing
- archived data cannot be parsed
- summary outputs cannot be written

The Airflow run should expose these failures through its task state, making the workflow suitable for demos and operational inspection.

The batch job should also handle empty historical inputs gracefully. If no relevant historical files exist, it should produce an explicit empty-output or no-data signal rather than failing ambiguously.

---

## Testing Strategy

The implementation must include tests for:

- DAG wiring and manual-trigger configuration
- batch summary generation on representative historical input samples
- correct creation of compliance summary fields
- correct creation of audit summary fields
- no interference with the existing optimized streaming pipeline paths

Runtime verification should include:

- one manual Airflow-triggered batch run
- confirmation that summary outputs appear in S3
- confirmation that the streaming pipeline continues to run unchanged

---

## Success Criteria

The design is considered successful when:

1. VacciGuard has a true storage-first batch-processing workflow
2. the workflow is manually triggerable through Airflow
3. the batch job reads archived S3 data rather than live Kafka input
4. the workflow produces both compliance and audit summaries
5. the existing optimized streaming pipeline remains unchanged and functional
6. the project now satisfies the workflow-scheduling requirement more directly

---

## Scope Boundaries

Included:

- Airflow-based manual workflow orchestration
- batch analytics over archived S3 data
- compliance and audit summary generation
- S3 summary outputs
- tests and demo-ready verification

Not included:

- replacing the existing streaming cold path
- redesigning the optimized hot/cold architecture
- building a full recurring production scheduler
- adding autoscaling or other unrelated pipeline changes as part of this feature

---

## Risks And Tradeoffs

### Main tradeoff

The design adds a second analytical path to the system, which introduces some complexity. That complexity is acceptable because it closes a real functional gap in the project and strengthens the academic alignment with the brief.

### Main risks

- schema mismatch between archived S3 outputs and batch expectations
- ambiguous ownership of summary output locations
- accidental overlap between streaming outputs and batch outputs

### Mitigation

- reuse the existing archived output schema directly
- keep batch output prefixes separate and explicit
- avoid modifying stream-processor responsibilities
- keep the Airflow DAG limited to orchestration only

---

## Final Recommendation

Proceed with a manually triggered Airflow-based batch-processing workflow that reads archived VacciGuard S3 outputs and produces:

- `daily_compliance_summary`
- `daily_audit_summary`

This gives the project a true batch layer, strengthens alignment with the professor's brief, and complements the existing SLA-aware streaming architecture without destabilizing it.
