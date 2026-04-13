# Phase 3: Minimal Technology Stack

## Goal
Choose one minimal tool per layer, lock the stack, and avoid switching technologies mid-project unless a clear problem forces it.

This phase is guided by four rules:

- satisfy the professor's functional requirements with the fewest moving parts
- use one processing engine for both stream and batch if possible
- keep the stack realistic for deployment and evaluation
- avoid changing multiple architectural variables between baseline and optimized experiments

## Locked Stack
| Layer | Chosen Tool | Why This Is The Best Fit |
| --- | --- | --- |
| Ingestion | Apache Kafka | Matches the professor's safe default, supports event streams well, and is easy to test under burst load |
| Processing | Apache Spark Structured Streaming + Spark batch | One engine handles both stream and batch processing, which reduces learning overhead and keeps evaluation cleaner |
| Elastic scaling | Kubernetes HPA + KEDA + Spark dynamic allocation | Covers both metric-based and event-driven scaling while keeping the system inside the Kubernetes model |
| Hot storage | Amazon ElastiCache for Redis OSS or Valkey | Keeps the logical Redis design while using an AWS-managed in-memory hot-state layer |
| Cold storage | Amazon S3 | Simple object storage for raw and processed historical data |
| Analytical format | Apache Parquet | Columnar format for efficient reporting and historical analysis |
| Workflow orchestration | Apache Airflow | Good fit for scheduled batch jobs, benchmark runs, backfills, and report generation |
| Deployment | Docker + Amazon EKS | Directly matches the professor's Kubernetes requirement while giving us a clear AWS deployment target |
| Infrastructure as Code | Terraform | Directly matches the professor's requirement |
| Observability | Amazon CloudWatch + Container Insights + self-managed Prometheus + self-managed Grafana on EKS | Splits AWS infrastructure visibility from pipeline-level metrics and dashboards |
| Lineage | Airflow OpenLineage provider later | Keeps the initial stack small while preserving a clear path to the lineage requirement |

## The Recommendation In One Sentence
Build VacciGuard as a Kafka -> Spark -> Redis + S3/Parquet pipeline, orchestrated by Airflow, deployed on Amazon EKS with HPA and KEDA, observed through CloudWatch plus self-managed Prometheus and Grafana, and provisioned with Terraform.

## AWS Deployment Decision
Both the baseline and optimized pipelines should run on AWS.

This is a good idea because it gives:

- a fairer baseline versus optimized comparison
- direct cloud cost and resource visibility
- a stronger cloud-native project story
- easier demonstration of EKS deployment and recovery behavior

To keep the comparison valid:

- both versions should run in the same AWS region
- both versions should use the same datasets and workload generator
- both versions should use the same core architecture
- only the optimization settings should change

## How This Stack Covers The Project Objectives
| Project Objective | Main Stack Components | How The Stack Covers It |
| --- | --- | --- |
| Unified real-time and batch analytics | Kafka, Spark Structured Streaming, Spark batch, Redis, S3, Parquet | Kafka handles live telemetry ingestion, Spark uses one engine for both stream and batch logic, Redis serves live operational views, and S3 plus Parquet stores historical analytical data |
| Elastic scaling for ingestion and processing | Kubernetes HPA, KEDA, Spark dynamic allocation, Kafka partitions | HPA scales metric-driven services, KEDA scales Kafka-driven workers using lag, Spark dynamic allocation scales executors, and Kafka partitions allow parallel ingestion and consumption |
| Exactly-once or effectively-once semantics | Kafka, Spark checkpoints, event IDs, idempotent sink logic | Kafka preserves events durably, Spark checkpoints preserve progress, and `event_id` plus deduplication or idempotent writes prevents harmful duplicates after retries or restarts |
| Cost-aware scheduling and resource allocation | Airflow, Airflow pools, Spark dynamic allocation, KEDA, ElastiCache Redis, S3, Parquet | Airflow schedules heavy jobs only when needed, pools limit expensive concurrency, dynamic allocation reduces idle compute, KEDA scales down low-demand workers, Redis is limited to hot operational state, and S3 plus Parquet keeps historical storage efficient |
| Performance evaluation under realistic workloads | CloudWatch, Prometheus, Grafana, Airflow, Terraform, Amazon EKS, Kafka, Spark | Kafka and Spark provide controllable workload behavior, CloudWatch observes AWS runtime health, Prometheus and Grafana expose pipeline metrics, Airflow standardizes experiment runs, and Terraform plus EKS make the environment reproducible across baseline and optimized tests |

## What "Covered" Means In This Project
The stack is not just a list of tools. Each objective must be satisfied by a clear implementation mechanism and a measurable outcome.

For this project, that means:

- every objective is tied to one or more specific tools
- every tool has a clear role in the architecture
- every claim should be backed later by an experiment, metric, or failure test

This is important because the professor is asking for both implementation and evaluation, not just design.

## Why This Stack Is Safest
### 1. Spark reduces tool count
The assignment requires both stream and batch processing. Spark is the safest choice because one engine can do both.

That means:

- one programming model instead of two
- fewer integration points
- cleaner baseline vs optimized comparisons
- less time spent learning multiple frameworks

### 2. Kafka is better than mixing ingestion tools
You previously drifted toward comparing unrelated architectures. We should not repeat that mistake.

Kafka is a good choice because:

- it is explicitly listed in the professor's brief
- it is widely used in cloud data pipelines
- it is easy to stress-test with normal, burst, and failure scenarios
- it avoids locking the project too early to one cloud vendor's streaming service

### 3. Redis gives a simple hot path
The assignment asks for hot storage. Redis is a practical choice for:

- latest reading per device
- active breach state
- current dashboard values
- short-lived operational data

On AWS, the preferred deployment is `Amazon ElastiCache for Redis OSS or Valkey`, because it keeps the same logical Redis design while reducing the operational burden of managing the hot-store service ourselves.

### 4. S3 plus Parquet gives a strong historical layer
The assignment asks for cold storage and analytical storage. S3 plus Parquet covers both cleanly:

- S3 stores raw and processed files cheaply and simply
- Parquet gives a proper columnar analytical format for reporting
- this is easy to explain in both the report and the demo

### 5. CloudWatch plus Prometheus and Grafana gives a clean monitoring split
Because the project will run on AWS, monitoring should be split into two clear parts:

- `Amazon CloudWatch + Container Insights` for AWS infrastructure, EKS runtime health, container logs, and alarms
- `self-managed Prometheus + self-managed Grafana on EKS` for pipeline-specific metrics such as Kafka lag, Spark metrics, Airflow metrics, and custom experiment dashboards

This is easier to explain than forcing everything into one monitoring tool.

### 6. Airflow should orchestrate, not overcontrol
Airflow is included because the brief asks for workflow scheduling. In this project, Airflow should be used for:

- scheduled batch ingestion
- daily report jobs
- benchmark workflows
- backfill or replay jobs

Airflow should not be treated as the main engine for the streaming data path. The long-running stream job belongs to Spark.

### 7. Elastic scaling needs explicit autoscaling components
The objective says the system should automatically grow when load increases and shrink when load drops. In this project, that must happen in two places:

- the `ingestion layer`, where events enter and wait to be consumed
- the `processing layer`, where Spark handles the work

To explain that clearly, we should use named scaling mechanisms instead of saying only that Kubernetes can scale.

#### Ingestion layer scaling
When event rate increases, messages can begin to wait inside Kafka before downstream services consume them. That waiting backlog is often described as `Kafka lag`.

To handle this:

- `KEDA` watches the Kafka backlog
- if the backlog grows, it increases the number of relevant Kubernetes pods
- if the backlog falls again, it reduces those pods

In simple terms:

- more waiting messages -> more ingestion-side workers
- fewer waiting messages -> fewer ingestion-side workers

#### Processing layer scaling
Spark is responsible for processing the incoming data. If there is too much work for the current Spark resources, processing delay can grow.

To handle this:

- `Spark dynamic allocation` increases the number of Spark executors when more processing power is needed
- it reduces executors again when the workload becomes lighter

In simple terms:

- more records to process -> more Spark compute
- less work -> less Spark compute

#### Where HPA fits
`Kubernetes HPA` is useful for services that should scale based on normal infrastructure metrics such as CPU or memory usage. It is not the main explanation for Kafka backlog scaling, but it is still useful for helper or support services around the pipeline.

#### Why this matters
This gives us a clear and defendable scaling story:

- Kafka backlog tells us when the ingestion side is falling behind
- KEDA reacts by increasing ingestion-side workers
- Spark dynamic allocation reacts by increasing processing-side compute
- when demand falls, resources shrink again instead of staying overprovisioned

How we will demonstrate this later:

- run the pipeline under normal load and record stable metrics
- sharply increase the event rate
- show backlog growth in Kafka
- show more ingestion-side pods or workers being created
- show more Spark executors being allocated
- reduce the load again
- show the system scaling back down

### 8. Cost-aware control is implemented, not bought
There is no single tool called "cost-aware scheduling" in this stack. Instead, we create it from a few focused mechanisms:

- `Airflow` controls when heavy batch jobs run
- `Airflow pools` limit expensive concurrency
- `Spark dynamic allocation` reduces idle executor cost
- `KEDA` can scale some workers toward zero when there is no event backlog
- `ElastiCache Redis` is used only for hot operational state, not historical retention
- `S3 + Parquet` keeps historical storage efficient

This is a better academic design than introducing many extra cloud cost tools too early.

How we will demonstrate this later:

- compare fixed-capacity baseline resource usage with the optimized elastic setup
- compare compute time, average active replicas, or cost proxy metrics
- show whether cost drops without breaking the latency target

## Why We Are Not Choosing The Other Main Options
### Why Not Flink
Flink is excellent for streaming, but for this project Spark is safer because:

- the assignment needs both stream and batch
- Spark keeps both in one engine
- this reduces implementation risk and evaluation confusion

If your project were only about ultra-low-latency streaming, Flink would be a stronger candidate.

### Why Not Kinesis
Kinesis is valid in general, but not the best choice here because:

- Kafka is already named in the brief
- Kafka is easier to reason about across local and cloud environments
- using Kinesis now would increase cloud lock-in and make controlled experiments less portable

Most importantly, using Kinesis in one version and Kafka in another would again create an invalid baseline-vs-optimization comparison.

### Why Not DynamoDB
DynamoDB is a good AWS-managed NoSQL service, but Redis is still better for this project because:

- the requirement is hot storage, not a full operational system
- Redis is easier to use for current state and short-lived keys
- using ElastiCache keeps the service managed on AWS without changing the hot-state design
- DynamoDB would change the role of the hot layer from in-memory operational state to a more durable NoSQL table model

### Why Not BigQuery, Snowflake, Or Extra Warehouses
These would add cost and complexity without improving marks much.

S3 plus Parquet is enough for:

- historical storage
- downstream reporting
- analytical access
- benchmark experiments

## What This Means For Baseline And Optimized Pipelines
This stack is locked for both versions.

That means:

- both baseline and optimized pipelines use Kafka
- both use Spark
- both use the same datasets
- both use the same storage pattern
- both use the same deployment model

Later, when we optimize, we will change only one meaningful variable such as:

- Spark micro-batch settings
- Spark parallelism
- Kubernetes autoscaling thresholds
- KEDA lag thresholds
- Kafka partitioning
- cost-latency tuning strategy

We will not switch products between the two versions.

This is what makes the objective coverage academically defensible:

- the objective stays the same
- the datasets stay the same
- the stack stays the same
- only the optimization mechanism changes

That allows us to claim improvement based on evidence rather than architecture replacement.

## Mapping To The Three Datasets
### Dataset 1: Live telemetry events
- enters through Kafka
- processed by Spark Structured Streaming
- scaled by KEDA and Kubernetes controls when backlog or demand changes
- latest state written to ElastiCache Redis
- raw and processed records written to S3 in Parquet or raw format

### Dataset 2: Device and facility lookup data
- stored as a small batch file
- loaded by Spark for enrichment joins
- used for breach classification and reporting

### Dataset 3: Daily operations or maintenance logs
- ingested by Airflow-triggered batch jobs
- processed by Spark batch
- stored in S3 as analytical outputs

## Expected Deployment Path
### Development
- use Docker containers for local component testing
- use a local Kubernetes cluster once enabled

### Project deployment target
- one Amazon EKS cluster
- one AWS region
- small footprint first
- no autoscaling in the first working version
- CloudWatch plus Container Insights enabled
- self-managed Prometheus and Grafana deployed on EKS

### Optimized deployment target
- add HPA for metric-based services
- add KEDA for Kafka-driven workers
- enable Spark dynamic allocation
- keep the same EKS region and core service layout
- compare this against the fixed-capacity baseline

## Current Environment Check
On this machine, the following are already available:

- `docker`
- `kubectl`
- `terraform`
- `java`
- `python3`

What is still missing or not ready:

- `helm` is not installed yet
- a working Kubernetes context is not set yet

This does not block Phase 3. It simply means we should enable or install the Kubernetes deployment helper tooling before Phase 5, especially because KEDA and the monitoring stack will run inside the same Kubernetes model that later maps to EKS.

## What Not To Add Right Now
Do not add these unless a real problem forces them:

- a second stream processor
- a second message broker
- a separate data warehouse
- multiple dashboard tools
- multiple clouds
- service mesh
- complex event schema registry on day one
- extra databases beyond what the pipeline needs

## Final Stack Lock
For the rest of the project, assume the official stack is:

- Apache Kafka
- Apache Spark
- Amazon EKS
- Kubernetes HPA
- KEDA
- Amazon ElastiCache for Redis OSS or Valkey
- Amazon S3
- Apache Parquet
- Apache Airflow
- Amazon CloudWatch
- self-managed Prometheus
- self-managed Grafana
- Docker
- Terraform

This is the stack we should carry into the implementation unless you explicitly decide to change it before Phase 4 begins.
