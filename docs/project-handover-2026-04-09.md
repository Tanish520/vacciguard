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

## Current Project State

The baseline pipeline is already functional on AWS:

- Kafka runs on EKS
- the stream processor runs on EKS
- replay jobs run on EKS
- processed, invalid, breach-window, and checkpoint outputs go to S3
- Redis is on AWS
- Prometheus and Grafana are available in-cluster
- there is an in-cluster evaluation-controller path in the repo

The main remaining work is not “make the baseline exist.” The main remaining work is:

- finish trustworthy evaluation methodology
- finish the optimized pipeline
- make the spike scenario valid at the intended 10x load
- compare baseline vs optimized fairly

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

### 3. Replay producer supports S3-backed workloads

This was necessary because large spike workloads exceed ConfigMap size limits.

Main files:

- [producer.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/services/replay-producer/producer.py)
- [requirements.txt](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/services/replay-producer/requirements.txt)
- [test_operational_metrics.py](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/tests/monitoring/test_operational_metrics.py)

### 4. Monitoring is operational enough for evaluation support

The baseline exposes Prometheus metrics and has a Grafana dashboard path.

Key files:

- [aws-baseline-foundation.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/docs/aws-baseline-foundation.md)
- [README.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/monitoring/README.md)
- [configmap-dashboard-baseline-overview.yaml](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml)

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

### Failure-Recovery

Observed:

- replay achieved `100 eps`
- planned restart was injected successfully
- reported avg latency `11.82 s`
- reported p95 latency `53.71 s`

Interpretation:

- baseline is resilient enough to keep functioning after restart
- baseline recovery is not low-latency enough for the target

### Spike

Observed:

- requested replay rate was `1000 eps`
- replay producer only sustained about `146.9 eps`
- the run was intentionally aborted and documented as such

Interpretation:

- the current spike failure is primarily a replay bottleneck
- this is not yet a valid 10x end-to-end pipeline benchmark

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

### 2. Improve final-count methodology

Current reports use batch-summary metrics gathered from logs.

This is useful, but not perfect.

To improve trustworthiness:

- compute final processed counts from S3 output directly
- compute invalid counts from S3 invalid output directly
- optionally compute latency from a stricter S3-based method after full drain

### 3. Build the optimized pipeline

The optimized pipeline should not just “add more CPU.”

The best current design direction is:

- split the hot alert path from the heavier archival/reporting path
- measure alert-path latency directly
- keep the comparison fair by using the same workload family and evaluation method

### 4. Compare baseline vs optimized using the same evaluation methodology

Hold these constant:

- workload family version
- scenario definitions
- report format
- measurement method

Only change:

- pipeline design and runtime profile

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

### Step 2. Bring AWS baseline up

- scale EKS nodegroup up
- restore Kafka and stream processor
- deploy monitoring if needed

### Step 3. Run baseline scenarios

Run all three:

- `normal`
- `failure-recovery`
- `spike`

Current baseline runner:

- [run-aws-baseline-evaluation.sh](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/scripts/run-aws-baseline-evaluation.sh)

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

### Step 6. After baseline, evaluate optimized with the same method

Do not invent a different evaluation style for the optimized pipeline.

## What Teammates Should Read, In Order

Read these in this order:

1. [project-handover-2026-04-09.md](/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/docs/project-handover-2026-04-09.md)
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

