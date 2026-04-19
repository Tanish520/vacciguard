# Phase 4 Stabilization Design

## Goal

Stabilize the existing Phase 4 local VacciGuard stream pipeline so it is repeatable, observable, and easy to extend toward AWS deployment and evaluation without changing the current project scope.

## Scope

This design covers only the local Phase 4 stream path:

- Kafka replay producer
- Spark stream processor
- Redis latest-state cache
- Filesystem-backed processed and invalid outputs
- Local smoke verification and operator runbook

This pass does not add the batch processor, AWS infrastructure, or full production monitoring stacks. It does shape the local processor logs and outputs so later CloudWatch-style monitoring and evaluation metrics can be mapped onto the same signals.

## Non-Goals

- Building `services/batch-processor`
- Adding dashboards, alerting systems, or external observability services
- Expanding beyond the Phase 4 architecture already chosen in the repo
- Replacing the current Redis or filesystem stand-ins with AWS services yet

## Why This Order

The repo already has a smallest working stream path in [services/stream-processor/job.py](/Users/tanishgupta/Downloads/vacciguard/services/stream-processor/job.py) and [services/replay-producer/producer.py](/Users/tanishgupta/Downloads/vacciguard/services/replay-producer/producer.py), but it is not yet defensible for evaluation because the local execution loop is still manual and the current logs do not expose the main stream behaviors clearly enough.

The shortest safe path is:

1. Make the local flow repeatable.
2. Add stream-level observability at the micro-batch boundary.
3. Add targeted tests for duplicate and late-arrival behavior.
4. Add the planned 5-minute facility breach-rate window.
5. Only after the stream path is testable and observable, start the batch path.

## Proposed Approaches

### Approach 1: Stabilize first, then extend stream behavior

Keep the current architecture intact, add smoke automation, add batch-level summaries to the stream processor, and then add Phase 4.2 behaviors with tests.

Pros:

- Lowest risk of deviating from project requirements
- Improves local repeatability immediately
- Produces monitoring-friendly signals that can carry into AWS evaluation
- Keeps the batch path gated behind a trustworthy stream path

Cons:

- Delays batch implementation slightly
- Requires a small amount of test harness work before visible new features

### Approach 2: Add richer stream behavior and stabilization in one large change

Implement smoke flow, metrics, late-arrival handling, duplicate tests, and 5-minute windows together in one pass.

Pros:

- Faster path to a feature-complete stream processor
- Fewer intermediate iterations

Cons:

- Harder to diagnose regressions
- Makes it easier to blur Phase 4 stabilization with Phase 4.2 expansion
- Weakens TDD discipline because multiple behaviors change at once

### Approach 3: Start AWS and monitoring scaffolding early

Add deployment and monitoring abstractions now, even before the local stream path is fully stabilized.

Pros:

- Earlier cloud-facing structure

Cons:

- Highest risk of overbuilding before local behavior is proven
- Adds scope that is not yet needed for current project requirements
- Creates more surface area to maintain while core stream behavior is still settling

## Recommendation

Use Approach 1.

It stays closest to the repo plan, improves local developer experience immediately, and gives the project the observability primitives it will need for AWS evaluation later without prematurely introducing cloud-only tooling.

## Design

### 1. Local smoke test and runbook

Add a smoke verification script under `tests/smoke` that validates the current Phase 4 local path end to end. The smoke flow should:

- ensure the replay workload exists
- start the stack or assume it has been started by the helper script
- wait for processed output files to appear
- inspect Redis for at least one latest device status entry
- verify that the processed output directory contains records
- verify that the invalid output directory is readable if invalid rows are present

The smoke test should be intentionally simple and runnable from the command line without hidden dependencies beyond the repo’s existing local stack. It should favor deterministic checks over broad assertions so failures are actionable.

Update [README.md](/Users/tanishgupta/Downloads/vacciguard/README.md) with an exact local runbook that documents:

- how to generate the replay workload
- how to start the Phase 4 services
- how to run the replay producer
- how to inspect Redis state
- how to inspect `data/output/processed`
- how to run the smoke verification

### 2. Phase 4 helper script

Add one helper script in `scripts` that makes the local Phase 4 rerun loop fast and repeatable. The script should orchestrate:

- optional cleanup of prior local output and checkpoints
- `docker compose up` for Kafka, Redis, and stream processor
- replay execution
- smoke verification or basic inspection summary
- `docker compose down`

The script should be conservative:

- keep the sequence explicit and readable
- print each stage clearly
- fail fast if one stage fails
- avoid deleting anything outside known local output/checkpoint paths

This script is a developer UX improvement, not a replacement for the manual README steps.

### 3. Stream micro-batch observability

Extend the stream processor so each processed micro-batch emits structured summary logs with at least these counts:

- valid records
- invalid records
- deduplicated records
- breach records

These summaries should be computed from the actual per-batch DataFrames inside the stream write path rather than inferred later from file counts. The output should be log-friendly and stable enough to map into future monitoring or evaluation capture in AWS.

At this stage, observability means:

- counts per batch
- meaningful batch identifiers
- clear logs when a batch contains no valid data
- enough information to compare replay size with processed results

This is the minimum viable monitoring signal for Phase 4. It supports later evaluation without introducing a monitoring stack too early.

### 4. Duplicate handling and late-arrival validation

After stabilization, add targeted tests around the behavior already implied by watermarking and `dropDuplicates(["event_id"])`.

The goals are:

- prove duplicates with the same `event_id` do not create duplicate processed outputs
- validate what happens when a record arrives later than the watermark boundary
- make the current late-arrival behavior explicit in tests so later AWS evaluation can rely on known semantics

The design should prefer extracting small pure helper functions or testable transformation builders where needed so stream behavior can be verified without relying only on full containerized integration runs.

### 5. Five-minute tumbling breach-rate window by facility

After duplicate and late-arrival tests are in place, add a 5-minute tumbling aggregation keyed by facility that summarizes breach-rate behavior.

The aggregation should:

- use event time rather than ingest time
- be facility-based
- count total processed records and breach records per window
- derive a breach-rate metric from those counts

The initial local implementation can write these summaries to a filesystem-backed output location or log them in a way consistent with the existing Phase 4 stand-in model. The key requirement is that the behavior is observable and testable locally.

## Data Flow Impact

The existing high-level flow stays the same:

Kafka -> parse -> validate -> deduplicate -> lookup join -> breach classification -> Redis latest state + filesystem outputs

This pass adds:

- batch summary logging around the stream outputs
- smoke verification around the full local loop
- an additional facility-window summary path after processed stream enrichment

It does not change the project’s architectural direction.

## Error Handling

- Smoke scripts should fail with clear messages when Docker services are unhealthy, replay data is missing, Redis has no expected keys, or processed outputs are absent.
- Helper scripts should stop on first failure and leave enough logs for diagnosis.
- Stream batch summary logging should handle empty batches explicitly and avoid ambiguous “nothing happened” states.
- Duplicate and late-arrival tests should encode the expected semantics so future changes cannot silently drift.

## Testing Strategy

### Unit and transformation tests

- add tests for the validation and deduplication path where practical
- add duplicate-focused tests first
- add late-arrival behavior tests next
- add facility breach-window aggregation tests after the aggregation is introduced

### Smoke verification

- run the local stack
- replay the dev workload
- assert Redis status entries are written
- assert processed output files are written
- optionally assert invalid output remains readable and structured

### Verification expectations

Before calling this pass complete, verify:

- the smoke command runs successfully from a clean local rerun
- the helper script works end to end
- the stream processor emits the new batch summaries
- tests covering duplicate and late-arrival behavior pass

## AWS and Monitoring Readiness

This design deliberately prepares for later AWS deployment without implementing it now.

The main readiness hooks are:

- stable, structured stream summary logs
- explicit batch-level counts that can map to CloudWatch metrics later
- deterministic smoke verification steps that can become deployment validation checks
- a clearly defined local runbook that will later translate into cloud deployment and evaluation procedures

When the project moves to AWS, the next layer can attach monitoring to the signals introduced here instead of inventing new ones.

## Implementation Boundary

The implementation for this design should be split into two passes:

1. Phase 4 stabilization
   - smoke test
   - README runbook
   - helper script
   - micro-batch summary logs

2. Phase 4.2 stream hardening
   - duplicate tests
   - late-arrival validation
   - 5-minute facility breach-rate tumbling window

Only after those are in place should the batch processor begin.
