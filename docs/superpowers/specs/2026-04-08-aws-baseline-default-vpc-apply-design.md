# AWS Baseline Default VPC Apply Design

## Goal

Deploy the first real VacciGuard baseline environment into AWS account `347038623570` in region `ap-south-1` using the default VPC for speed.

This pass should move the project from a reviewed AWS baseline scaffold to a live, minimal baseline deployment foundation on AWS. The outcome is a fixed-capacity baseline environment that is deployed in AWS and ready for runtime verification before later evaluation runs.

## Scope

This pass includes:

- discovering and using the default VPC and default subnets in `ap-south-1`
- extending the existing Terraform scaffold so it can create a real baseline AWS foundation
- creating the first real S3 and ElastiCache resources for the baseline
- adding the missing EKS foundation and cluster provisioning pieces
- configuring local `kubectl` access to the created EKS cluster
- wiring the baseline Kubernetes manifests with live AWS values
- deploying the baseline manifests to the EKS cluster
- verifying that the baseline deployment reaches a minimally usable runtime state

This pass does not include:

- optimized/autoscaled deployment
- monitoring stack rollout
- batch processor deployment
- final evaluation metrics capture and result recording
- production-grade networking hardening beyond what is needed for the first baseline

## Why Default VPC

The priority for this pass is speed to first working baseline on AWS.

Using the default VPC:

- reduces the amount of new infrastructure that must be designed before the first deployment
- removes a major blocker between scaffold and apply
- keeps the focus on getting EKS, S3, Redis, and the baseline runtime working

This is acceptable because the default VPC choice is explicitly a first-baseline shortcut, not the final infrastructure standard.

## Approaches Considered

### Approach 1: Build a dedicated VPC first

Pros:

- cleaner infrastructure isolation
- closer to long-term best practice

Cons:

- significantly slower
- distracts from getting the baseline deployed

### Approach 2: Recommended - Use the default VPC now for the first baseline

Pros:

- fastest route to a live baseline
- keeps the deployment pass focused on EKS and application rollout
- avoids unnecessary networking detours

Cons:

- less isolated than a dedicated VPC
- likely to be replaced or refined later

## Chosen Approach

Use the default VPC in `ap-south-1` for the first real AWS baseline deployment.

## Target Baseline Environment

This pass should create:

- one EKS cluster in `ap-south-1`
- one S3 bucket for baseline data paths
- one ElastiCache Redis deployment for hot state
- one fixed-capacity Kubernetes baseline deployment
- self-managed Kafka on Kubernetes
- stream processor and replay producer on Kubernetes

This remains a baseline-only environment. Optimized deployment remains a later pass.

## Architecture

The deployment shape for this pass should be:

1. Discover the default VPC and subnets in the target region.
2. Update or extend Terraform inputs and resources so they can use those discovered network values.
3. Apply the AWS foundation in this order where practical:
   - S3 baseline bucket
   - Redis security group and subnet group
   - ElastiCache Redis
   - EKS cluster and required node capacity
4. Configure the local Kubernetes context for the created cluster.
5. Inject the live baseline values into the Kubernetes overlay:
   - real S3 bucket path
   - real Redis host
   - any live cluster-specific values needed by the baseline manifests
6. Deploy the baseline manifests.
7. Verify:
   - cluster access works
   - namespace and baseline resources render and apply successfully
   - the baseline runtime reaches a minimally usable state

## Infrastructure Changes Required

The current Terraform scaffold is not yet apply-ready for a real baseline deployment because it lacks EKS resources and still depends on manually provided network inputs.

This pass should therefore add:

- data discovery for default VPC and subnets, or explicit Terraform inputs populated from AWS discovery
- real EKS cluster resources or module usage
- worker node or node group configuration
- any IAM resources required for the cluster and workload access
- outputs that expose:
  - cluster name
  - bucket name
  - Redis endpoint

The Terraform should still remain baseline-focused and fixed-capacity.

## Kubernetes Deployment Expectations

The Kubernetes layer should remain minimal and fixed-capacity:

- one Kafka replica
- one stream processor replica
- one replay producer job model
- fixed resource requests/limits as already defined in the baseline overlays

The manifests must be updated with live AWS values before apply so they no longer rely on placeholder values.

## Verification Expectations

Before calling this pass complete, verify:

- AWS caller identity still matches the intended account
- Terraform plan/apply succeeds for the baseline infrastructure
- `kubectl` can talk to the new cluster
- base and baseline overlays still render after live-value injection
- the baseline namespace and workloads can be applied to EKS
- the baseline deployment is in a usable enough state that the next pass can focus on runtime verification and evaluation

## What “Done” Means For This Pass

This pass is complete when:

- the baseline AWS foundation exists in the live account
- the EKS cluster is reachable from the local machine
- live AWS values have been wired into the baseline Kubernetes deployment path
- the baseline manifests are deployed to the cluster
- the deployment is ready for the next phase of runtime verification and evaluation preparation

## Deliberate Non-Goals

This pass should not:

- introduce autoscaling
- build the optimized deployment
- add monitoring systems in the same pass
- claim the evaluation is already complete
- expand into unrelated infrastructure polish

## Main Risks

- EKS provisioning may require additional IAM or node-group adjustments once applied
- Kubernetes manifests may need one debug loop once real images, volumes, and live values hit the cluster
- the default VPC shortcut may impose later cleanup work, but that is acceptable for the first baseline

## Follow-On Work

After this pass, the next likely steps are:

1. baseline runtime verification in-cluster
2. stream-path verification against live S3 and Redis
3. workload replay on AWS
4. evaluation result capture
5. monitoring rollout
6. optimized deployment path
