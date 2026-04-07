# AWS Baseline Foundation Design

## Goal

Create the first AWS deployment sub-project for VacciGuard: a reviewed, repo-native baseline deployment definition that prepares a single-region, single-cluster, fixed-capacity AWS environment for later deployment and evaluation.

This design is for the AWS baseline foundation only. It prepares the repository for deployment but does not apply real AWS resources yet.

## Scope

This pass includes:

- Terraform scaffold for the shared AWS foundation
- Kubernetes deployment structure split into `base`, `baseline`, and `optimized`
- baseline starter manifests for Kafka, stream processor, and replay producer
- configuration contracts for moving the pipeline from local filesystem and local Redis assumptions toward AWS-compatible S3 and ElastiCache assumptions
- deployment documentation for the baseline environment

This pass does not include:

- applying Terraform to create real AWS resources
- full monitoring stack rollout
- optimized/autoscaled deployment behavior
- batch processor deployment
- final evaluation runs or report-ready benchmark results

## Why This Sub-Project First

The current repository has a stabilized local Phase 4 stream path, but the AWS deployment surface is still only placeholders. The baseline pipeline cannot be evaluated on AWS until the deployment shape is explicit and reproducible.

This first AWS sub-project should therefore produce the deployment blueprint that later work will apply. That keeps the first cloud-facing pass reviewable, avoids premature billable resources, and gives the baseline pipeline a clean platform boundary.

## Approaches Considered

### Approach 1: Terraform foundation plus deployment structure only

Create Terraform scaffolding for AWS and create `base` / `baseline` / `optimized` Kubernetes layout, but do not add application manifests yet.

Pros:

- smallest infrastructure-first slice
- low review risk
- clarifies cloud responsibilities early

Cons:

- leaves the baseline application shape vague
- still requires a second design pass before the baseline runtime is concrete

### Approach 2: Recommended - Terraform foundation plus baseline application manifests

Create the AWS scaffold and add starter Kubernetes manifests for the current core runtime: Kafka, stream processor, and replay producer.

Pros:

- creates a concrete baseline deployment definition
- provides the bridge between the local pipeline and the future AWS deployment
- keeps baseline and optimized paths separated from the start

Cons:

- larger first pass than Terraform-only scaffolding
- requires careful boundaries so monitoring and optimization do not creep in

### Approach 3: Full AWS baseline deployment and observability in one pass

Define infra, deploy the workloads, wire monitoring, and try to reach evaluation-readiness at once.

Pros:

- fewer handoffs
- fastest path if everything goes smoothly

Cons:

- highest scope and failure risk
- mixes platform definition, workload deployment, and evaluation concerns together
- makes review and debugging harder

## Chosen Approach

Use Approach 2.

The repo should gain a complete baseline deployment definition, not just raw AWS scaffolding. That means Terraform plus the initial Kubernetes baseline manifests, but still stopping short of actually applying resources or rolling out full monitoring.

## Target Baseline Environment

The first baseline deployment target is intentionally constrained:

- one AWS region
- one EKS cluster
- one fixed-capacity baseline environment
- self-managed Kafka running on EKS
- stream processor and replay producer running on EKS
- Amazon S3 for historical outputs and checkpoints
- Amazon ElastiCache Redis for hot operational state

This is the reference deployment that later evaluation work can measure. The optimized deployment remains a separate later pass.

## Architecture

The baseline AWS shape should be:

1. Terraform provisions shared AWS infrastructure:
   - provider and environment settings
   - naming and tagging conventions
   - VPC/networking foundation appropriate for EKS
   - EKS cluster foundation
   - S3 buckets for processed outputs, invalid outputs, and checkpoints
   - ElastiCache Redis for live state
   - IAM roles, policies, and workload-access assumptions

2. Kubernetes manifests are split into three layers:
   - `infra/kubernetes/base`: shared namespace, config, secrets contracts, and common manifests
   - `infra/kubernetes/baseline`: fixed-capacity baseline overlays and replica/resource settings
   - `infra/kubernetes/optimized`: reserved structure only, not activated yet

3. Baseline application runtime on EKS includes:
   - Kafka
   - stream processor
   - replay producer

4. Application configuration is prepared so the current pipeline can later switch from local paths to AWS-backed values through environment variables and manifests rather than code rewrites.

## File Structure

The design should create or populate these areas:

- `infra/terraform/`
  - shared Terraform layout for provider, variables, outputs, modules or environment decomposition
- `infra/kubernetes/base/`
  - common manifests and shared configuration
- `infra/kubernetes/baseline/`
  - fixed-capacity baseline overlays or manifests
- `infra/kubernetes/optimized/`
  - reserved placeholder structure for later optimization work
- `docs/`
  - deployment-oriented documentation for the baseline environment

## Baseline Runtime Contract

The baseline deployment should stay intentionally simple:

- fixed Kafka replica count
- fixed stream processor replica/resource settings
- fixed replay producer invocation model for evaluation runs
- no KEDA
- no HPA for the core pipeline
- no Spark dynamic allocation in this first baseline pass

These settings should be explicit in manifests or deployment docs so the baseline can later be frozen for fair comparison.

## Configuration Contract

The current local stream processor uses local filesystem paths and local Redis assumptions. This AWS baseline pass should introduce a deployment contract that supports:

- S3-backed output and checkpoint locations
- ElastiCache Redis connection configuration
- environment-specific values injected through manifests and Terraform outputs

The code should only change where necessary to make these deployment-time values configurable. The goal is to preserve the current processing behavior while making the runtime portable.

## Error Handling And Operational Expectations

The baseline AWS scaffold should make failure modes diagnosable:

- missing required configuration should fail loudly at startup
- manifests should separate shared config from baseline-specific values
- deployment docs should state which values are placeholders until apply time
- the baseline deployment should be understandable without reading every implementation file

## Testing Strategy

This sub-project should verify the deployment definition, not pretend the AWS runtime already exists.

Expected verification for this pass:

- Terraform structure is syntactically valid and organized for later apply
- Kubernetes manifests are valid and consistently split across `base`, `baseline`, and `optimized`
- application config still supports the current local flow where appropriate
- documentation explains how the baseline deployment is intended to be applied and extended

Real AWS deployment verification belongs to the next execution pass, after this scaffold is reviewed.

## What “Done” Means For This Sub-Project

This design is complete when the repo contains:

- a clear Terraform scaffold for the AWS baseline foundation
- a clear Kubernetes split between `base`, `baseline`, and `optimized`
- baseline starter manifests for Kafka, stream processor, and replay producer
- documented configuration expectations for S3 and ElastiCache
- enough deployment structure that the next pass can focus on applying and iterating rather than inventing the baseline layout

## Deliberate Non-Goals

This pass should not:

- create live AWS resources
- claim the baseline is already deployed
- claim the pipeline is already evaluation-ready in AWS
- introduce optimized deployment behavior early
- mix in full monitoring implementation
- expand into batch deployment before the baseline stream deployment path is defined

## Follow-On Work

After this design is implemented and reviewed, the next likely sub-projects are:

1. apply the AWS baseline foundation in a real account
2. deploy the baseline workloads onto EKS
3. verify the stream path against S3 and ElastiCache
4. add monitoring and evaluation capture
5. create the optimized deployment path separately
