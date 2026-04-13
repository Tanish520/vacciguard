# Evaluation Workload Family Design

## Purpose

Define the official workload family used to evaluate the VacciGuard baseline and optimized pipelines on AWS.

The current `data/workloads/dev/events.ndjson` file remains a smoke-test workload only. It is too small and too demo-oriented to serve as the professor-facing evaluation basis.

This design creates a workload family that is:

- realistic enough to defend in evaluation
- small enough to rerun repeatedly on AWS
- versionable so future changes do not invalidate past results
- aligned with the project success criteria for latency, spike tolerance, recovery, and cost

## Goals

- Establish one official evaluation workload family for repeatable AWS experiments.
- Separate steady-state, spike, and recovery scenarios so each metric is measured in the right context.
- Lock a first official workload version that can be used for baseline vs optimized comparison.
- Keep future workload revisions possible without silently changing previously reported results.

## Non-Goals

- Replace the dev smoke workload.
- Define the optimized pipeline itself.
- Add batch-processing evaluation in this same scope.
- Create every future workload variant up front beyond the first required family.

## Why A Workload Family Instead Of One File

The project objectives are not all measured under the same operating conditions:

- steady-state latency and correctness need a normal workload
- spike tolerance needs a burst workload
- recovery time needs a controlled failure scenario

One all-in-one dataset would be harder to explain, harder to compare fairly, and harder to debug when results differ. A workload family is cleaner and better aligned with the evaluation table.

## Official Workload Family

The first official workload family will have one common simulated world and three scenario variants.

### Shared Simulated World

Every scenario in the family is based on the same logical operating model:

- approximately 30 devices
- a 12-minute observation window
- vaccine cold-chain telemetry shaped around realistic facility/device behavior
- a mix of safe operation, targeted breach periods, and a small amount of imperfect data

The family is versioned. The first official version is:

- `evaluation-workload-v1`

Any future change to device count, rates, scenario mix, or fault model must create a new version rather than silently overwrite this one.

## Scenario Definitions

### 1. `normal`

Primary use:

- baseline vs optimized comparison
- average end-to-end latency
- P95 latency
- throughput
- processed, invalid, deduplicated, and breach counts
- later cost-per-run and cost-per-GB analysis

Definition:

- duration: 12 minutes
- device count: about 30
- normal replay rate: about 6 events per second total
- per-device intuition: about one reading every 5 seconds

Target event volume:

- base unique valid stream: about 4,320 events
- plus duplicates, late arrivals, and invalid records layered on top

Target data mix:

- valid on-time unique events: 88-92%
- duplicates: 4-6%
- late arrivals: 2-4%
- invalid records: 1-2%

Breach behavior:

- breach events must exist, but remain concentrated in a limited set of device windows
- breaches should represent meaningful cold-chain incidents, not random noise across every device

Reasoning:

This makes the normal workload large enough to be credible, but still practical for repeated AWS runs.

### 2. `spike`

Primary use:

- 10x traffic spike success/failure
- spike-time latency behavior
- throughput under pressure

Definition:

- same logical device/facility world as `normal`
- same event mix philosophy as `normal`
- replayed at 10x the normal rate

Locked first-rate choice:

- normal: about 6 eps
- spike: about 60 eps

Reasoning:

The spike workload must stress the runtime mainly through rate, not through a completely different dataset. That keeps baseline vs optimized comparisons fair and easy to explain.

### 3. `failure-recovery`

Primary use:

- recovery time after failure
- evidence that the pipeline resumes useful processing after disruption

Definition:

- same logical world and approximate data composition as `normal`
- one deliberate fault injected during the run

Locked first fault model:

- restart the stream processor deployment during the run

Reasoning:

This is visible, controllable, easy to script in Kubernetes, and directly relevant to the stream objective. It also avoids mixing multiple failures into one scenario.

## Data-Rate Decision

The normal data rate is chosen from the evaluation goal, not from the current dev smoke workload.

The first official choice is:

- about 1 reading every 5 seconds per device
- about 30 devices
- about 6 total events per second

Why this rate:

- realistic enough for telemetry monitoring
- meaningfully larger than the current 300-event smoke file
- still practical in cost and runtime for repeat AWS evaluations
- makes the spike scenario cleanly definable as 10x without immediately becoming absurd

Alternatives considered:

- 3 eps total: cheaper, but too light for a convincing evaluation
- 15 eps total normal: more aggressive, but pushes the normal scenario toward a stress test before the spike case even begins

## Data Quality And Messiness Policy

The official evaluation family should not be perfectly clean.

The `normal` scenario intentionally includes:

- some duplicates
- some late arrivals
- a small number of invalid records
- a limited number of breach windows

Why:

- the project is not only about fast processing
- correctness under realistic imperfect input is part of the evaluation
- the existing processor already tracks these categories, so the workload should exercise them intentionally

However, the workload should remain controlled:

- duplicates, lateness, and invalids are present in low but measurable proportions
- the normal scenario is not a chaos dataset

## Evaluation Mapping

The workload family maps to the chosen evaluation metrics as follows:

- Avg end-to-end latency: `normal`
- P95 latency: `normal`
- Throughput: `normal`, with optional reference to `spike`
- 10x spike success/failure: `spike`
- Recovery time after failure: `failure-recovery`
- Input events: all scenarios
- Processed events: all scenarios
- Invalid events: all scenarios
- Deduplicated events: all scenarios
- Breach events: all scenarios
- Cost per run: primarily `normal`, optionally also `spike`
- Cost per GB processed: primarily `normal`

This keeps each metric tied to the scenario that actually proves it.

## Reproducibility Rules

To keep evaluation defensible:

- the workload family must be deterministic or generated from deterministic seeds/config
- each official workload version must have stable documented parameters
- results must record which workload family version was used
- future changes must create `v2`, `v3`, and so on, instead of mutating `v1`

## Expected Repo Outcome

Implementation based on this design should produce:

- official evaluation workload definitions separate from the dev smoke workload
- generator support or versioned workload artifacts for `normal`, `spike`, and `failure-recovery`
- scenario-aware evaluation scripts that run the right workload for the right metric
- report metadata that records the workload family version and scenario used

## Risks

### Risk: Workload Too Small

If the official workload is too close to the smoke dataset, the evaluation will look underpowered and may not persuade reviewers.

Mitigation:

- use about 30 devices
- use a 12-minute window
- use about 6 eps for normal

### Risk: Workload Too Expensive To Repeat

If the workload is too large, repeated AWS runs become slow and costly.

Mitigation:

- keep the official `v1` family moderate
- keep long-run or larger-scale scenarios for later versions only if needed

### Risk: Ambiguous Comparison

If different scenarios use unrelated logical worlds, baseline vs optimized results will be harder to compare fairly.

Mitigation:

- define one shared simulated world
- derive scenario variants from it

## Decision Summary

The first official evaluation design for VacciGuard is:

- workload family, not one file
- versioned as `evaluation-workload-v1`
- shared world of about 30 devices over 12 minutes
- normal workload at about 6 eps
- spike workload at about 60 eps
- failure-recovery workload using a controlled stream-processor restart
- normal scenario includes limited duplicates, late arrivals, invalids, and breach windows

This is the reference evaluation family for the first baseline vs optimized comparison.
