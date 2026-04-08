# AWS-Native Evaluation Framework Design

## Goal

Make VacciGuard evaluation runs execute inside AWS for both the baseline and optimized pipelines, instead of depending on a laptop-local orchestration script. The framework should run evaluations from inside EKS, collect evidence in AWS, and store reports in S3 using a stable, comparison-friendly layout.

## Motivation

The current evaluation flow is mostly AWS-hosted, but the orchestration still depends on a local shell session:

- the user runs `scripts/run-aws-baseline-evaluation.sh` from a laptop
- kubeconfig and local AWS credentials drive the experiment
- reports are generated locally and only partially mirrored to AWS
- Grafana access is still operationally separate from evaluation execution

This weakens the “fully AWS” story for evaluation. We want the experiment itself to execute in AWS so that baseline vs optimized comparisons are more reproducible, more defensible, and less dependent on a human operator’s laptop state.

## Scope

This design covers:

- one shared in-cluster evaluation framework for both `baseline` and `optimized`
- parameterized evaluation runs for `normal`, `spike`, and `failure-recovery`
- S3-first report and evidence storage
- Kubernetes Job based orchestration inside EKS

This design does not cover:

- redesigning either pipeline’s processing logic
- replacing Prometheus or Grafana
- production scheduling or recurring automation
- CI/CD image build automation
- broad alerting and autoscaling work

## Recommended Approach

Use a single Kubernetes Job, called the evaluation controller, that runs inside the same EKS cluster as the pipelines. The controller is parameterized by pipeline target, scenario, run id, and workload family version. It orchestrates the selected run entirely inside AWS:

- selects the target pipeline configuration
- creates isolated Kafka topic and S3 prefix
- restarts the target stream processor
- launches the replay job for the selected workload scenario
- waits for completion and collection windows
- collects logs and output evidence
- writes `report.md` and `report.json` to S3

This is better than an external AWS orchestrator for the current project because it fits the existing EKS architecture, keeps complexity down, and makes the evaluation flow easier to explain.

## Alternatives Considered

### 1. Recommended: in-cluster evaluation controller Job

Pros:

- aligns with current EKS-based system
- simple control flow for Kubernetes-native operations
- easy to support both baseline and optimized from one framework
- evaluation becomes AWS-executed instead of laptop-executed

Cons:

- evaluator still needs Kubernetes permissions inside the cluster
- controller image becomes another deployable component to maintain

### 2. External AWS orchestrator using Lambda or Step Functions

Pros:

- stronger “AWS control plane” story on paper
- future-friendly if the system grows into more complex workflows

Cons:

- adds much more IAM and integration complexity
- EKS interaction becomes indirect and harder to debug
- slower path to a working academic evaluation flow

### 3. Keep the local script and only move report storage into S3

Pros:

- smallest implementation effort

Cons:

- does not solve the real laptop dependency problem
- weaker AWS-native evaluation story

## High-Level Architecture

### Components

#### 1. Evaluation controller Job

A Kubernetes Job that runs the orchestration logic inside EKS.

Responsibilities:

- validate run parameters
- resolve pipeline target and scenario
- derive per-run topic and S3 paths
- patch/select live configuration for the target pipeline
- restart the target stream processor
- create and monitor the replay job
- optionally inject a planned failure for `failure-recovery`
- collect evidence
- generate and upload reports

The controller orchestrates experiments only. It is not a long-lived service and does not perform stream processing itself.

#### 2. Shared evaluator image/script

One image containing the evaluation logic reused for both baseline and optimized runs. Pipeline selection happens through parameters, not through separate evaluator implementations.

#### 3. Target pipeline overlays

The baseline and optimized pipelines remain separate at the deployment/configuration level. The controller selects the target pipeline by parameter and applies the corresponding config or resource selection logic.

#### 4. Replay job

The controller launches the replay producer as a Kubernetes Job using the correct scenario workload and target topic.

#### 5. Report writer

The controller generates:

- `report.md`
- `report.json`

and stores them in S3 alongside the output evidence.

## Run Contract

Every evaluation run is parameterized by:

- `PIPELINE_TARGET`: `baseline` or `optimized`
- `SCENARIO`: `normal`, `spike`, or `failure-recovery`
- `RUN_ID`: explicit unique id
- `WORKLOAD_FAMILY_VERSION`: workload family version label

Optional future parameters may include:

- `RESET_REDIS_STATE`
- `FAILURE_INJECTION_MODE`
- `WAIT_TIMEOUT_SECONDS`

The controller derives:

- isolated Kafka topic
- target S3 prefix
- report paths
- workload file and manifest for the chosen scenario

## Execution Flow

1. Start `evaluation-controller` Job with run parameters.
2. Validate AWS, Kubernetes, and workload inputs.
3. Resolve target pipeline: `baseline` or `optimized`.
4. Resolve workload scenario: `normal`, `spike`, or `failure-recovery`.
5. Generate isolated Kafka topic and S3 prefix for the run.
6. Prepare target pipeline config for the selected run.
7. Restart the selected stream processor deployment.
8. Wait for the target stream processor to become query-ready.
9. Launch replay producer Job for the selected workload.
10. For `failure-recovery`, inject the planned fault at the defined point.
11. Wait for replay completion and post-run stabilization window.
12. Collect logs, output evidence, and metrics summaries.
13. Generate `report.md` and `report.json`.
14. Upload report artifacts to S3.

## Artifact Layout

Each run writes to:

`s3://<bucket>/evaluations/<pipeline-target>/<scenario>/<run-id>/`

Contents:

- `processed/`
- `invalid/`
- `breach_windows/`
- `checkpoints/`
- `report.md`
- `report.json`

This layout keeps:

- baseline vs optimized separated
- scenarios separated
- runs independently reproducible

## Target Selection Model

The framework uses one shared evaluator implementation and selects the target pipeline through `PIPELINE_TARGET`.

What stays shared:

- evaluator code
- run orchestration logic
- report generation
- evidence collection
- workload selection model

What remains target-specific:

- deployment/config selection
- target stream processor deployment name if needed
- any target-specific config patches

This preserves fairness by keeping evaluation logic identical across both pipelines.

## Data and Evidence

Run evidence includes:

- processed output
- invalid output
- breach-window output
- checkpoints
- replay logs
- stream processor logs
- generated evaluation report

These are all considered evaluation artifacts and should be retained in S3 under the run path.

## Monitoring Relationship

Prometheus and Grafana remain the runtime observability layer. The evaluator does not replace them. Instead:

- Grafana and Prometheus provide live visibility during the run
- the controller provides the formal run report after the run

This keeps runtime monitoring and formal evaluation reporting clearly separated.

## Failure Handling

The controller should fail fast and write a useful partial report when possible.

Expected failure cases:

- target pipeline never becomes ready
- replay job fails
- S3 evidence path is unreachable
- required workload file or manifest is missing
- failure injection step cannot be executed

Minimum failure behavior:

- mark run as failed in `report.json`
- include failure reason
- upload partial evidence where possible
- avoid silently succeeding with incomplete data

## Security and Access

The controller runs with a Kubernetes service account and AWS permissions through IRSA, similar to the existing pipeline jobs.

Required capabilities include:

- create/delete Jobs
- read and patch ConfigMaps or selected config resources
- read logs from pipeline workloads
- write reports to S3

The controller should receive only the minimum Kubernetes and AWS permissions needed for orchestration and report writing.

## Testing Strategy

Implementation should be validated through:

- unit tests for parameter resolution and report-path derivation
- unit tests for report generation and target selection logic
- manifest render checks for new Kubernetes resources
- at least one live in-cluster smoke run against `baseline`
- one live follow-up run against `optimized`

## Success Criteria

The design is successful when:

- evaluation runs execute from inside EKS, not from a laptop shell
- both `baseline` and `optimized` use the same evaluator framework
- reports are written to S3 in the defined structure
- scenario selection works for workload-family runs
- the run can be explained as AWS-executed and reproducible

## Implementation Boundaries

This design deliberately stops short of:

- full external dashboard exposure redesign
- CI/CD automation for image publishing
- scheduled recurring evaluations
- broad monitoring or alerting expansion

Those can follow later, but the first priority is making evaluation execution itself AWS-native.
