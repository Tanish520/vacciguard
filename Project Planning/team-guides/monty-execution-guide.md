# Monty Execution Guide

## Role Summary
Monty owns the deployment, cloud infrastructure, autoscaling, and observability side of
VacciGuard. His work is what turns the team's code and datasets into a real cloud data pipeline on
AWS that can be measured, stressed, restarted, and compared fairly between baseline and optimized
versions.

He is responsible for building and operating:
- the AWS infrastructure with Terraform
- the Amazon EKS deployment environment
- the Kubernetes manifests and runtime wiring
- the baseline fixed-capacity deployment
- the optimized autoscaled deployment
- the observability stack with CloudWatch, Prometheus, and Grafana
- the infrastructure side of Redis, S3, Airflow, Kafka, and Spark runtime wiring

The most important risk in his area is this:

- if the baseline and optimized deployments are not kept structurally comparable, the evaluation
  becomes academically weak
- if the infrastructure is set up late, the team may discover cloud-specific issues too late to fix
- if monitoring and scaling are vague, the project will not convincingly satisfy the professor's
  elasticity, reliability, and evaluation requirements

Because of that, Monty should think of his work as building a controlled experimental platform, not
just "deploying some services."

## Main Objective
Build and operate the AWS environment so the team can:
- run the pipeline reliably on Amazon EKS
- provision the shared cloud services needed by the pipeline
- deploy a fixed-capacity baseline version
- deploy an optimized autoscaled version
- observe both versions using consistent metrics and dashboards
- run repeatable load, scaling, and recovery tests

## How Monty Should Think About His Role
Monty is not writing the business logic of the pipeline, but he controls whether that business
logic can be demonstrated and evaluated properly.

In practical terms:
- Alok provides the workloads and replay producer
- Aayush provides the stream and batch processing logic
- Monty makes those components run correctly on AWS
- Tanish uses the environment and metrics to compare baseline versus optimized behavior

So Monty's work answers questions like:
- where do the services run?
- how do they connect?
- how do they authenticate to AWS?
- how do we know if the system is healthy?
- what exactly changes between baseline and optimized?

## What Monty Must Deliver
- Terraform configuration for the AWS foundation
- Amazon EKS setup for the project environment
- deployment configuration for Kafka, Spark, Airflow, and monitoring components
- AWS-managed setup for `S3` and `ElastiCache Redis`
- baseline Kubernetes deployment manifests with fixed-capacity settings
- optimized Kubernetes deployment manifests with autoscaling enabled
- Prometheus and Grafana deployment and dashboards
- CloudWatch and Container Insights integration
- explicit SLA detection rules and dashboards for latency, lag, stalled processing, and recovery
- clear runtime configuration for the stream job, batch job, and replay producer
- scaling configuration for HPA, KEDA, and Spark dynamic allocation in the optimized version
- recovery and observability notes for the final report and evaluation

## How Other Members Use Monty's Work
- **Alok** uses Monty's environment to run replay workloads and prove that the input side can create real pressure on Kafka
- **Aayush** uses Monty's deployment setup to run the stream and batch Spark jobs with stable storage, checkpoint, and sink connectivity
- **Tanish** uses Monty's metrics, dashboards, and controlled deployments to compare baseline versus optimized behavior fairly

## Folders Monty Should Use
- [infra/terraform/](/Users/tanishgupta/Desktop/vacciguard/infra/terraform): Terraform code for AWS infrastructure
- [infra/kubernetes/base/](/Users/tanishgupta/Desktop/vacciguard/infra/kubernetes/base): manifests common to both pipeline versions
- [infra/kubernetes/baseline/](/Users/tanishgupta/Desktop/vacciguard/infra/kubernetes/baseline): fixed-capacity baseline deployment configuration
- [infra/kubernetes/optimized/](/Users/tanishgupta/Desktop/vacciguard/infra/kubernetes/optimized): autoscaled and optimization-aware deployment configuration
- [infra/monitoring/cloudwatch/](/Users/tanishgupta/Desktop/vacciguard/infra/monitoring/cloudwatch): AWS monitoring setup notes or manifests
- [infra/monitoring/prometheus/](/Users/tanishgupta/Desktop/vacciguard/infra/monitoring/prometheus): Prometheus setup
- [infra/monitoring/grafana/](/Users/tanishgupta/Desktop/vacciguard/infra/monitoring/grafana): Grafana dashboards and configuration
- [orchestration/airflow/dags/](/Users/tanishgupta/Desktop/vacciguard/orchestration/airflow/dags): Airflow DAGs after they are ready to deploy
- [orchestration/airflow/configs/](/Users/tanishgupta/Desktop/vacciguard/orchestration/airflow/configs): Airflow runtime configuration
- [tests/failure/](/Users/tanishgupta/Desktop/vacciguard/tests/failure): failure and recovery test notes or checks
- [tests/workload/](/Users/tanishgupta/Desktop/vacciguard/tests/workload): workload/scaling test notes and calibration steps

## Baseline Coordination Rules
For the shared dependency flow, Monty should also read:
- [baseline-dependency-and-coordination-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/baseline-dependency-and-coordination-plan.md)

### When Monty can start
Monty can start immediately because the cloud target, stack, and deployment model are already fixed.

### When Monty must inform others
Monty should stop and inform the team at these baseline checkpoints:
- after the AWS base and Terraform structure are ready
- after S3 and ElastiCache assumptions are ready
- after the fixed-capacity baseline deployment is stable enough to use
- after monitoring is visible and useful
- after the baseline deployment profile is frozen

He should inform:
- **Alok**, so replay testing can move to the real environment
- **Aayush**, so stream and batch jobs can be deployed with the correct runtime assumptions
- **Tanish**, so the baseline environment and evaluation assumptions can be frozen properly

### When Monty's baseline task is complete
For the baseline pipeline, Monty's work is complete when:
- the AWS environment is usable
- the baseline deployment is fixed-capacity and stable
- S3 and ElastiCache are reachable from the pipeline
- monitoring is visible
- the baseline runtime profile is frozen and ready for end-to-end execution

## Inputs Monty Must Treat As Fixed
Monty should align all deployment work with these existing decisions:
- [phase-3-technology-stack.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/02-phase-3/phase-3-technology-stack.md)
- [phase-3-objective-mapping.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/02-phase-3/phase-3-objective-mapping.md)
- [project-folder-structure.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-folder-structure.md)
- [input-schema-freeze.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/input-schema-freeze.md)
- [alok-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/alok-execution-guide.md)
- [aayush-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/aayush-execution-guide.md)

He should not arbitrarily change:
- the chosen stack
- the sink/storage split
- the difference between baseline and optimized
- the fact that both versions must run on AWS and remain comparable

## Core Concepts Monty Must Understand Before Building

### 1. Baseline and optimized are not two different systems
Both deployments should use the same core architecture:
- Kafka
- Spark
- Airflow
- ElastiCache Redis
- S3
- EKS
- CloudWatch
- Prometheus
- Grafana

The difference is:
- baseline uses fixed capacity and simple settings
- optimized enables autoscaling and resource-tuning features

Monty should therefore think:
- same architecture
- same code
- same datasets
- same AWS environment model
- different scaling/resource behavior

### 2. Monty owns infrastructure setup, not business rules
Monty is the owner of:
- provisioning S3
- provisioning ElastiCache Redis
- making Spark able to reach those services
- setting up secrets, config, permissions, and network access

He is not the owner of:
- what business logic Spark uses
- how breach rules are defined
- what fields exist in the datasets

Those belong mainly to Aayush and Alok.

### 3. Airflow should orchestrate batch work, not own the streaming lifecycle
Monty should deploy Airflow for:
- daily batch jobs
- benchmark workflows
- replay or backfill runs
- report-generation runs

He should **not** design the always-on Spark stream job as a job that Airflow keeps starting and
stopping like a normal batch task. The streaming job should be treated as a long-running Spark
application on EKS.

### 4. CloudWatch and Prometheus have different purposes
Monty should maintain a clean monitoring split:

- `CloudWatch + Container Insights`
  - AWS and EKS infrastructure visibility
  - container logs
  - alarms and cluster-level health

- `Prometheus + Grafana`
  - Kafka lag
  - Spark metrics
  - Airflow metrics
  - custom pipeline dashboards

This is important because it makes the final evaluation easier to explain.

### 5. Autoscaling belongs only in the optimized deployment
Monty should keep the baseline clearly fixed-capacity.

That means baseline should not include:
- HPA-based autoscaling for core pipeline services
- KEDA-driven Kafka lag scaling
- Spark dynamic allocation

Those belong to the optimized deployment only.

### 6. SLA violation detection lives mainly in the monitoring layer
Monty should treat SLA detection as a monitoring and evaluation responsibility, not as business
logic inside Spark.

That means:
- Aayush exposes processing-side metrics such as latency and progress
- Kafka and Kubernetes expose runtime signals such as lag and health
- Monty turns those signals into explicit rules, dashboards, and violation indicators

Monty should keep this distinction clear:
- **business alerts**: breach conditions, produced by Spark business logic
- **SLA violations**: latency too high, lag too high, no progress, recovery too slow

## Detailed Step-by-Step Approach

### Step 1: Establish the AWS foundation first
Before worrying about application deployment details, Monty should create the Terraform skeleton
for the AWS foundation.

At minimum, he should plan for:
- VPC or networking setup appropriate for EKS
- Amazon EKS cluster
- worker-node setup
- S3 buckets
- ElastiCache Redis setup
- required IAM roles and policies

Why this step matters:
- the team needs the cloud environment early
- local-only success does not guarantee EKS success
- autoscaling and observability must be tested in a real Kubernetes environment

### Step 2: Separate infrastructure into base, baseline, and optimized layers
Monty should not keep all deployment config in one mixed folder.

He should structure deployment logic as:
- `base`: common shared configuration
- `baseline`: fixed-capacity settings
- `optimized`: autoscaling and tuning settings

This should be reflected in:
- [infra/kubernetes/base/](/Users/tanishgupta/Desktop/vacciguard/infra/kubernetes/base)
- [infra/kubernetes/baseline/](/Users/tanishgupta/Desktop/vacciguard/infra/kubernetes/baseline)
- [infra/kubernetes/optimized/](/Users/tanishgupta/Desktop/vacciguard/infra/kubernetes/optimized)

Why this step matters:
- it prevents the two versions from becoming mixed together
- it makes the report easier to explain
- it makes the final comparison cleaner and more reproducible

### Step 3: Provision S3 and ElastiCache Redis as shared cloud services
Monty should treat S3 and Redis as infrastructure responsibilities.

He should provision:
- S3 buckets or paths for raw data, processed outputs, and checkpoints
- ElastiCache Redis for the hot operational state

He should also ensure:
- the right IAM permissions exist
- the Spark jobs can reach S3
- the application can connect to Redis
- configuration values are exposed to the services cleanly

Why this step matters:
- Aayush needs working sinks for the stream and batch processors
- these services are central to the architecture and cannot be left vague

### Step 4: Bring up a fixed-capacity baseline environment first
Monty should deploy the baseline before touching autoscaling.

The baseline should be:
- fixed-capacity
- stable
- measurable
- intentionally simple

Recommended starting baseline assumptions:
- Kafka fixed at a small, stable replica count
- streaming Spark job with fixed driver and executor count
- batch Spark job with fixed resource settings when triggered
- replay producer with fixed test-time replica count
- Airflow deployed for batch orchestration only
- no KEDA
- no HPA for core pipeline path
- no Spark dynamic allocation

Why this step matters:
- the baseline is the reference point for the optimized comparison
- if the baseline is unclear, the optimized story becomes weak

### Step 5: Encode the baseline deployment profile explicitly
Monty should document the baseline runtime profile as part of the deployment setup.

At minimum, he should define:
- Kafka replica count
- Spark streaming driver count
- Spark streaming executor count
- batch job driver and executor settings
- replay producer replica count during experiments
- monitoring components that stay on

This profile should be frozen once the team agrees it is stable enough for fair testing.

Why this step matters:
- nobody should be guessing pod counts during the final evaluation
- the baseline must be repeatable

### Step 6: Deploy the monitoring stack early
Monty should not postpone monitoring until the end.

He should bring up:
- CloudWatch integration
- Container Insights
- Prometheus
- Grafana

He should aim to see at least:
- cluster and pod health
- Kafka runtime visibility
- Spark runtime behavior
- replay workload pressure

Why this step matters:
- early visibility makes debugging much easier
- the project needs graphs and evidence, not only "it worked" statements

### Step 7: Decide and wire the metrics that matter
Monty should define, as early as possible, the key metrics the team will later use in evaluation.

Important metrics include:
- throughput
- end-to-end delay
- Kafka lag
- replica count
- Spark executor count
- failure count
- recovery time
- resource usage
- cost proxy metrics

He should make sure the infrastructure and monitoring stack can surface those metrics in a form the
team can actually use.

Why this step matters:
- metrics are part of the methodology, not an optional extra

### Step 7A: Turn the chosen metrics into explicit SLA rules
After the metrics are visible, Monty should define the actual violation rules the team will use in
baseline and optimized comparison.

Recommended initial SLA rule set:
- **SLA-1 latency violation**
  - metric: p95 end-to-end latency from the stream path
  - meaning: near-real-time target is being missed
- **SLA-2 lag violation**
  - metric: Kafka consumer lag
  - meaning: processing is falling behind ingestion
- **SLA-3 stalled-processing violation**
  - signal: backlog or lag exists but processed-record count does not increase
  - meaning: the stream path is not making useful progress
- **SLA-4 recovery-time violation**
  - signal: measured time from a controlled failure to healthy resumed processing
  - meaning: the system recovered too slowly

Monty should implement these through the monitoring stack like this:
- `Prometheus` scrapes Kafka, Spark, and custom pipeline metrics
- `Grafana` visualizes the rule inputs and alert states
- `CloudWatch` provides AWS and EKS runtime visibility, logs, and infrastructure alarms

Monty should not guess thresholds casually during evaluation. He should work with Tanish to freeze
the thresholds once the baseline design is ready, and then reuse those same thresholds in the
optimized comparison.

### Step 8: Deploy Airflow with the correct role
Monty should deploy Airflow in a way that supports:
- daily batch jobs
- scheduled report generation
- benchmark orchestration
- controlled replay or backfill workflows

He should not use Airflow to "manage" the always-on stream lifecycle.

Why this step matters:
- Airflow is part of the project stack
- but using it incorrectly would confuse the architecture and operational story

### Step 9: Prepare the streaming job as a long-running application
Monty should work with Aayush to deploy the stream processor as a long-running Spark application
on EKS.

He should ensure:
- checkpoint locations are available and stable
- configuration is externalized cleanly
- restart behavior is understood
- logs and metrics are visible

Why this step matters:
- the stream path is continuous and must survive restarts meaningfully

### Step 10: Prepare the batch path as scheduled execution
Monty should work with Aayush to deploy the batch processor so that Airflow can trigger it on
demand or on schedule.

He should ensure:
- batch inputs are reachable
- outputs land in the correct S3 locations
- job completion and failure are visible
- resource settings are fixed in the baseline version

Why this step matters:
- the project requires both batch and stream processing
- the batch path should be observable and repeatable

### Step 11: Validate the baseline end to end before optimizing
Before enabling autoscaling, Monty should run a full baseline validation with the team.

He should confirm:
- Kafka is stable
- replay producer can send data
- Spark streaming processes data continuously
- Redis receives current state
- S3 receives historical outputs
- batch jobs complete successfully
- monitoring dashboards show useful signals
- the SLA rule inputs and alert states behave sensibly during smoke validation

Why this step matters:
- optimized tuning is meaningless if the baseline is not correct first

### Step 12: Add optimized autoscaling controls only after baseline is stable
After the baseline is accepted, Monty should build the optimized deployment.

This is where he adds:
- `KEDA` for Kafka lag-driven scaling
- `HPA` where resource-based scaling makes sense
- `Spark dynamic allocation`

He should keep everything else as similar as possible to the baseline.

Why this step matters:
- the optimized deployment should look like an improvement to the same system, not a new architecture

### Step 13: Make the optimized deployment measurable against the baseline
Monty should define how the optimized version will be shown to be better.

Examples:
- lower Kafka lag during spike
- faster recovery after pressure drops
- lower average active resources over time
- better cost proxy behavior without breaking latency goals

Why this step matters:
- optimization without evidence is not useful for the report

### Step 14: Run scaling experiments with Alok's workloads
Monty should use Alok's replay workloads to trigger:
- normal traffic
- burst traffic
- cooldown and recovery

He should record:
- lag growth
- scale-out behavior
- scale-in behavior
- processing stability during and after the spike

Why this step matters:
- this is how the project proves elastic behavior under realistic pressure

### Step 15: Run failure and recovery experiments
Monty should design simple but convincing failure tests.

Examples:
- restart a processing component
- simulate a pod failure
- verify replay and checkpoint recovery

He should measure:
- restart behavior
- time to recover
- whether the pipeline resumes useful processing

Why this step matters:
- reliability is one of the project's main non-functional goals

### Step 16: Keep the environment fair for comparison
Monty must help protect the validity of the baseline-versus-optimized comparison.

He should ensure:
- same AWS region
- same core infrastructure model
- same datasets
- same code
- same monitoring method
- same SLA thresholds and rule definitions

Only the optimization controls should differ.

Why this step matters:
- if too many things change at once, the comparison becomes hard to defend academically

### Step 17: Prepare deployment handoff for Tanish
Before the final evaluation and report work, Monty should give Tanish:
- the exact baseline deployment assumptions
- the exact optimized deployment assumptions
- key dashboards or metric sources
- the exact SLA rule definitions and where their evidence appears in dashboards or logs
- notes on what changed and what did not change between the two versions

Why this step matters:
- Tanish needs this to write the evaluation and architecture story clearly

## Practical Build Sequence
If Monty wants the shortest path, he should complete the work in this order:

1. Create the Terraform skeleton for AWS
2. Stand up EKS and core cloud services
3. Separate `base`, `baseline`, and `optimized` deployment config
4. Provision S3 and ElastiCache Redis
5. Deploy the fixed-capacity baseline environment
6. Bring up CloudWatch, Prometheus, and Grafana
7. Deploy Airflow for batch orchestration
8. Work with Aayush to deploy the long-running stream job
9. Work with Aayush to deploy the scheduled batch job
10. Validate the baseline end to end
11. Add KEDA, HPA, and Spark dynamic allocation in the optimized deployment
12. Run workload experiments with Alok
13. Run failure and recovery experiments
14. Freeze the deployment assumptions for final comparison
15. Hand the final environment notes to Tanish

## Done Criteria
Monty's task should be considered complete only when all of the following are true:
- the AWS environment can be recreated with Terraform
- EKS is running the project's core services successfully
- S3 and ElastiCache Redis are provisioned and reachable
- the fixed baseline deployment is stable and documented
- the optimized deployment exists separately and clearly
- CloudWatch, Prometheus, and Grafana provide useful visibility
- the team can observe scaling behavior during workload tests
- the team can observe recovery behavior during failure tests
- baseline and optimized assumptions are documented cleanly for the final comparison

## Common Mistakes To Avoid
- treating baseline and optimized as two unrelated architectures
- delaying AWS setup until after all local coding is "finished"
- using Airflow to manage the always-on stream lifecycle
- enabling autoscaling in the baseline deployment
- mixing baseline and optimized manifests in the same files without clear separation
- setting up monitoring too late
- changing infrastructure assumptions during evaluation without documenting them

## One-Sentence Summary
Monty's job is to build a reproducible AWS platform on Amazon EKS where the fixed baseline and the
autoscaled optimized versions of the same pipeline can be deployed, monitored, stressed, and
compared fairly.
