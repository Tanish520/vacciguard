# VacciGuard Project Handover

## Purpose

This file is the main handover note for:

- teammates continuing the project
- future Codex sessions continuing the same work

It explains:

- what has already been achieved
- what is still pending
- how to reproduce the workload family locally
- how to evaluate the pipeline correctly
- what files to read, in order
- what needs to change when using a different AWS account

Use this file as the first entry point before making new changes.

## Quick Start

If a teammate opens this project for the first time, this is the simplest way to use this file:

1. Read `Current Project State` to understand what already works.
2. Read `What The Current Results Mean` to understand what has been proven and what has not.
3. Read `Most Important Pending Work` and `What Teammates Can Explore To Improve The Pipeline` to decide the next task.
4. Read `What Teammates Should Read, In Order` and follow that order.
5. Regenerate the workloads locally before trying to run anything.

This file is intentionally written for both:

- human teammates taking over the project
- future AI sessions that need fast context without rereading the whole repo

## Plain-Language Project Summary

VacciGuard currently has a working AWS baseline pipeline.

In simple words, the baseline pipeline already does this:

- replay producer sends synthetic telemetry events
- Kafka receives those events
- the stream processor consumes them
- valid processed output is written to S3
- invalid records are written separately
- breach-window output is also written
- Redis stores the live state needed by the application

So the project is not blocked on “getting something to run.”

The real problem now is performance and evaluation quality:

- the baseline works, but it still misses the latency target
- the current spike methodology is not yet valid because replay becomes the bottleneck first
- the optimized pipeline still needs to be built and tested fairly against the same workload family

## Important Terms

These terms are used throughout the rest of this file.

- `baseline pipeline`
  - the current AWS pipeline implementation used as the comparison starting point
- `optimized pipeline`
  - the future improved version that should reduce latency and handle stress better
- `normal`
  - the normal-load evaluation scenario, currently set to `100 eps`
- `spike`
  - the high-load scenario, currently intended as `10x normal`, so `1000 eps`
- `failure-recovery`
  - the scenario where the stream processor is deliberately restarted during the run
- `eps`
  - events per second
- `input events`
  - all events the replay producer tries to send
- `processed events`
  - only the records that successfully make it to the processed output path
- `invalid events`
  - records rejected by validation and written to the invalid output
- `aborted run`
  - a run that should not be treated as a valid benchmark because the intended conditions were not actually achieved

## Current Project State

The baseline pipeline is already functional on AWS:

- Kafka runs on EKS
- the stream processor runs on EKS
- replay jobs run on EKS
- processed, invalid, breach-window, and checkpoint outputs go to S3
- Redis is on AWS
- Prometheus and Grafana are available in-cluster
- there is an in-cluster evaluation-controller path in the repo

This means the project already has a real cloud deployment and a real evaluation workflow.

The main remaining work is not “make the baseline exist.”

The main remaining work is:

- finish trustworthy evaluation methodology
- finish the optimized pipeline
- make the spike scenario valid at the intended 10x load
- compare baseline vs optimized fairly

Another way to say this:

- deployment is mostly solved
- observability is usable
- experimentation is possible
- the next stage is improving quality, correctness of evaluation, and performance

## What Has Been Achieved

### 1. Evaluation workload family exists

The repo now has a deterministic generator for:

- `normal`
- `spike`
- `failure-recovery`

Source of truth:

- [generate-evaluation-workloads.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/scripts/generate-evaluation-workloads.py)
- [README.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/data/workloads/evaluation/v1/README.md)
- scenario manifests under [v1](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/data/workloads/evaluation/v1)

Current target rates:

- `normal = 100 eps`
- `spike = 1000 eps`
- `failure-recovery = 100 eps`

Current failure model:

- `failure-recovery` injects a stream-processor restart at `6 minutes`

### 2. AWS evaluation runner works for large workloads

The baseline evaluation runner can:

- isolate each run with a unique Kafka topic and S3 prefix
- stage oversized workload files to S3 automatically
- collect logs and write a markdown report
- inject the planned failure for `failure-recovery`

Main file:

- [run-aws-baseline-evaluation.sh](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/scripts/run-aws-baseline-evaluation.sh)

Why this matters:

- runs do not overwrite each other
- teammates can inspect one run at a time
- evaluation evidence is easier to keep organized
- the repo has moved beyond manual ad hoc testing

### 3. Replay producer supports S3-backed workloads

This was necessary because large spike workloads exceed ConfigMap size limits.

Main files:

- [producer.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/services/replay-producer/producer.py)
- [requirements.txt](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/services/replay-producer/requirements.txt)
- [test_operational_metrics.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/tests/monitoring/test_operational_metrics.py)

Why this matters:

- large workloads no longer need to live inside Kubernetes manifests
- the repo does not need to store giant generated data files
- teammates can regenerate the same workload locally and upload it when needed

### 4. Monitoring is operational enough for evaluation support

The baseline exposes Prometheus metrics and has a Grafana dashboard path.

Key files:

- [aws-baseline-foundation.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/docs/aws-baseline-foundation.md)
- [README.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/monitoring/README.md)
- [configmap-dashboard-baseline-overview.yaml](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml)

This is useful because evaluation should not depend only on one final markdown report.

During a run, teammates can inspect:

- replay progress
- stream processed counts
- invalid counts
- breach counts
- latency-related metrics

### 5. Baseline AWS evaluation reports exist

Important reports currently available:

- [baseline-normal-20260409t120000z.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/artifacts/aws-baseline-evaluations/baseline-normal-20260409t120000z.md)
- [baseline-spike-20260409t121800z.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/artifacts/aws-baseline-evaluations/baseline-spike-20260409t121800z.md)
- [baseline-failure-recovery-20260409t122500z.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/artifacts/aws-baseline-evaluations/baseline-failure-recovery-20260409t122500z.md)

## What The Current Results Mean

### Normal

Observed:

- replay achieved `100 eps`
- reported avg latency `7.6 s`
- reported p95 latency `13.79 s`

Interpretation:

- baseline can run the `100 eps` normal workload end to end
- baseline still misses the `< 5s` latency goal

What teammates should take from this:

- the system is not broken
- the system is also not fast enough yet
- this is a valid baseline result, because the intended input rate was actually achieved
- this is the correct reference point for optimized work

### Failure-Recovery

Observed:

- replay achieved `100 eps`
- planned restart was injected successfully
- reported avg latency `11.82 s`
- reported p95 latency `53.71 s`

Interpretation:

- baseline is resilient enough to keep functioning after restart
- baseline recovery is not low-latency enough for the target

What teammates should take from this:

- restart recovery works functionally
- non-functional quality is still weak during and after disruption
- the optimized version should improve both steady-state latency and recovery behavior

### Spike

Observed:

- requested replay rate was `1000 eps`
- replay producer only sustained about `146.9 eps`
- the run was intentionally aborted and documented as such

Interpretation:

- the current spike failure is primarily a replay bottleneck
- this is not yet a valid 10x end-to-end pipeline benchmark

What teammates should take from this:

- the spike run did not prove the pipeline can or cannot handle `1000 eps`
- it only proved the current replay method cannot generate that load by itself
- replay methodology must be fixed before making any performance claim about 10x spike handling

## Most Important Pending Work

### 1. Fix spike methodology

This is the most important next technical task.

Recommended short-term solution:

- run multiple replay producers in parallel
- shard the spike workload into multiple files
- send all shards to the same Kafka topic

Why:

- a single replay producer cannot currently generate the intended `1000 eps`
- without fixing this, the spike benchmark is not valid

In practice, this is probably the single highest-value next task, because without a valid spike generator the final baseline vs optimized comparison will always have a major weakness.

### 2. Improve final-count methodology

Current reports use batch-summary metrics gathered from logs.

This is useful, but not perfect.

To improve trustworthiness:

- compute final processed counts from S3 output directly
- compute invalid counts from S3 invalid output directly
- optionally compute latency from a stricter S3-based method after full drain

Why this matters:

- some current report fields come from batch-summary logs
- those logs are useful, but they are not the cleanest source of truth for final counts
- teammates should treat S3 output as the best final evidence whenever possible

### 3. Build the optimized pipeline

The optimized pipeline should not just “add more CPU.”

The best current design direction is:

- split the hot alert path from the heavier archival/reporting path
- measure alert-path latency directly
- keep the comparison fair by using the same workload family and evaluation method

This is important because the baseline evidence suggests the current stream graph is doing too much work in the latency-critical path.

### 4. Compare baseline vs optimized using the same evaluation methodology

Hold these constant:

- workload family version
- scenario definitions
- report format
- measurement method

Only change:

- pipeline design and runtime profile

This is what makes the comparison defendable in a report or demo.

## What Teammates Can Explore To Improve The Pipeline

This section is intentionally practical. It lists the most promising areas to explore, why they matter, and what a good outcome would look like.

### 1. Parallel replay producers for spike

This should be explored first.

Current problem:

- one replay producer could only sustain about `146.9 eps`
- the target spike rate is `1000 eps`

Best exploration path:

- split the spike workload into multiple shards
- run multiple replay jobs in parallel
- send all shards to the same Kafka topic
- keep the combined target rate equal to the intended spike rate

Good outcome:

- the replay layer can actually generate the full intended spike load
- the spike benchmark becomes valid

### 2. Fairer final metric collection

Current problem:

- some metrics in the reports still rely on streamed summaries gathered during execution

Exploration ideas:

- compute final processed row counts from S3 output after the run
- compute invalid counts from S3 invalid output
- add a clear drain window after replay completes
- distinguish between “replay finished” and “pipeline fully drained”

Good outcome:

- final reports become easier to trust
- teammates can explain exactly where each metric came from

### 3. Direct alert-path latency measurement

Current problem:

- current latency metrics are still closer to output-write timing than true application alert timing

Exploration ideas:

- measure from Kafka receipt or replay send time to Redis update
- expose this as a first-class Prometheus metric
- report it separately from archival-output latency

Good outcome:

- the optimized pipeline can be judged against the real SLA
- the final latency story becomes much stronger

### 4. Separate hot path from heavy reporting path

Current problem:

- the baseline stream path appears to be doing too much in one place

Exploration ideas:

- keep validation, enrichment, classification, and Redis update in the fast path
- move heavier summaries, archival output, and richer reporting to a slower side path
- preserve business logic while simplifying the critical path

Good outcome:

- lower end-to-end alert latency
- more realistic chance of getting under `5s`

### 5. Replay and Kafka parallelism

Current problem:

- throughput limits may be caused by the replay side and Kafka partition shape, not only by the stream processor

Exploration ideas:

- revisit partition counts
- ensure replay sharding maps well to Kafka partitions
- check whether producer batching and in-flight settings are limiting throughput

Good outcome:

- the system can generate and receive stress more realistically
- bottlenecks become easier to attribute correctly

### 6. Stronger monitoring during evaluation

Current problem:

- current monitoring is usable, but could be more evaluation-specific

Exploration ideas:

- add panels or metrics specifically for replay target rate vs actual rate
- add a clearer “last successful batch” signal
- show when the stream is drained versus merely still running

Good outcome:

- teammates can tell during the run whether the experiment is valid
- fewer confusing post-run interpretations

### 7. Cost-aware evaluation

This is not the first priority, but it is useful for the final report.

Exploration ideas:

- use cost-allocation tags for baseline vs optimized
- record run start and end times carefully
- estimate run cost separately from idle AWS cost

Good outcome:

- stronger non-functional comparison between baseline and optimized
- more complete final evaluation section

## Workload Regeneration Instructions

Teammates should generate the evaluation workloads locally.

Do not rely on large generated `.events.ndjson` files being present in Git.

Use:

```bash
cd /path/to/repo
python3 scripts/generate-evaluation-workloads.py
```

Then verify:

```bash
python3 -m unittest tests.workload.test_generate_evaluation_workloads -v
```

This must produce:

- `normal.manifest.json`
- `spike.manifest.json`
- `failure-recovery.manifest.json`
- corresponding `.events.ndjson` files locally

The shared workload contract is defined by:

- generator code
- committed manifests
- this handover note

## Recommended Evaluation Methodology

Use this methodology unless there is a strong reason to change it.

### Step 1. Regenerate workloads locally

Use the generator so all teammates use the same workload shape.

This is important because the large generated files are not supposed to be pushed casually into GitHub. The generator and manifests are the shared contract, not the giant generated files.

### Step 2. Bring AWS baseline up

- scale EKS nodegroup up
- restore Kafka and stream processor
- deploy monitoring if needed

Before running any benchmark, verify:

- Kafka is healthy
- stream processor is healthy
- Prometheus and Grafana are reachable if monitoring is needed
- the target S3 bucket and Redis endpoint are correct for the AWS account being used

### Step 3. Run baseline scenarios

Run all three:

- `normal`
- `failure-recovery`
- `spike`

Current baseline runner:

- [run-aws-baseline-evaluation.sh](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/scripts/run-aws-baseline-evaluation.sh)

Recommended run order:

1. `normal`
2. `failure-recovery`
3. `spike`

This order is safer because:

- `normal` confirms the setup is fundamentally healthy
- `failure-recovery` checks resilience at a rate the replay path can already sustain
- `spike` is the most fragile scenario and should come after the setup is proven

### Step 4. Treat spike carefully

Do not call spike “successful” unless:

- the replay side actually reaches the intended target rate
- the pipeline receives the full intended load

If replay itself is the bottleneck:

- record that honestly
- fix replay methodology first

### Step 5. Write one markdown report per run

Each report should include:

- run id
- scenario
- workload family version
- configured replay rate
- actual throughput
- avg latency
- p95 latency
- input events
- processed events
- invalid events
- deduplicated events
- breach events
- fault injection result if applicable
- important findings

Also add a short interpretation paragraph in each report:

- did the replay side actually achieve the intended rate?
- is the run valid as an end-to-end benchmark?
- what is the most important takeaway from that run?

### Step 6. After baseline, evaluate optimized with the same method

Do not invent a different evaluation style for the optimized pipeline.

## What Teammates Should Read, In Order

Read these in this order:

1. [project-handover-2026-04-09.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/Project%20Planning/team-guides/project-handover-2026-04-09.md)
2. [aws-baseline-foundation.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/docs/aws-baseline-foundation.md)
3. [README.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/data/workloads/evaluation/v1/README.md)
4. [generate-evaluation-workloads.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/scripts/generate-evaluation-workloads.py)
5. [run-aws-baseline-evaluation.sh](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/scripts/run-aws-baseline-evaluation.sh)
6. [producer.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/services/replay-producer/producer.py)
7. [06-latency-optimization-plan.md](/Users/tanishgupta/Downloads/vacciguard/Project%20Planning/02-phase-3/non-functional-requirements/06-latency-optimization-plan.md)
8. [baseline-normal-20260409t120000z.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/artifacts/aws-baseline-evaluations/baseline-normal-20260409t120000z.md)
9. [baseline-failure-recovery-20260409t122500z.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/artifacts/aws-baseline-evaluations/baseline-failure-recovery-20260409t122500z.md)
10. [baseline-spike-20260409t121800z.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/artifacts/aws-baseline-evaluations/baseline-spike-20260409t121800z.md)

Then, for implementation detail:

- [test_generate_evaluation_workloads.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/tests/workload/test_generate_evaluation_workloads.py)
- [test_operational_metrics.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/tests/monitoring/test_operational_metrics.py)
- [test_aws_baseline_metrics.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/tests/evaluation/test_aws_baseline_metrics.py)
- [controller.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/services/evaluation-controller/controller.py)
- [main.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/services/evaluation-controller/main.py)

Why this order matters:

- the first group explains the project and current evaluation story
- the second group explains how the workload is generated and how AWS runs are orchestrated
- the final group explains lower-level implementation details and tests

## Working In A Different AWS Account

This is important.

Teammates may continue the project in a different AWS account, so they must not assume the current account-specific values remain valid.

### Things That Must Be Recreated Or Updated

1. Terraform foundation

Apply the Terraform stack in the new AWS account:

- VPC / networking
- EKS cluster
- S3 bucket
- Redis
- IAM roles

Start here:

- [infra/terraform](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/terraform)
- [README.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/terraform/README.md)

2. ECR repositories and image paths

Current manifests contain account-specific ECR URIs such as:

- `347038623570.dkr.ecr.ap-south-1.amazonaws.com/...`

In a new AWS account, teammates must:

- build and push images into the new account’s ECR
- update image references in Kubernetes manifests

Important files:

- [deployment-stream-processor.yaml](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/kubernetes/base/deployment-stream-processor.yaml)
- [job-replay-producer.yaml](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/kubernetes/base/job-replay-producer.yaml)

3. S3 bucket names and Redis endpoints

These will differ by account and environment.

Teammates must update the pipeline config from Terraform outputs before running evaluations.

4. Monitoring access

Grafana and Prometheus may need redeployment or re-exposure in the new account’s cluster.

Practical advice for teammates in a new account:

- do not assume any current bucket name, ECR path, IAM role, or Redis endpoint is reusable
- regenerate infrastructure from Terraform outputs
- treat the current AWS account values as examples, not fixed truth
- verify every account-specific value before running evaluations

## Exact Recommended Next Tasks For Teammates

1. Regenerate the evaluation workload family locally.
2. Bring the AWS baseline up in the new or chosen AWS account.
3. Verify `normal` and `failure-recovery` still run with the same method.
4. Implement parallel replay producers for spike.
5. Re-run spike until the replay side can actually achieve the intended aggregate rate.
6. Tighten final metrics so counts and latency rely less on partial log summaries.
7. Build the optimized pipeline using the latency plan.
8. Re-run all scenarios on optimized with the same methodology.
9. Compare baseline vs optimized honestly.

If only one teammate starts first, the best first assignment is:

- own the spike methodology fix

If work is split across multiple teammates, a good split is:

- one person owns replay and workload sharding
- one person owns evaluation/report quality
- one person owns optimized pipeline design and implementation

## Important Warnings

- Do not treat the aborted spike run as a successful 10x benchmark.
- Do not assume `processed_events` from current reports are perfect final-output counts.
- Do not commit giant generated workload files casually.
- Do not compare optimized against a different workload family or different reporting logic.
- Do not hide replay bottlenecks inside “pipeline performance.”

## Final Short Summary

The baseline pipeline is working on AWS and can complete:

- `normal @ 100 eps`
- `failure-recovery @ 100 eps`

It cannot yet support a valid:

- `spike @ 1000 eps`

because the current replay path becomes the bottleneck around `146.9 eps`.

So the project is now in a good handoff state:

- workload family defined
- evaluation runner working
- monitoring present
- baseline evidence captured
- main next steps clearly identified

The simplest honest summary is:

- the project is already real and runnable
- the baseline is proven enough to use as a starting point
- the next big job is not deployment, it is better evaluation quality and better performance
