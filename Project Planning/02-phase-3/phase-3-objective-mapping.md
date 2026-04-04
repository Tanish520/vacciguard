# Phase 3: Objective To Tool Mapping

## Purpose
This document explains why the selected tools are a strong fit for the exact objectives listed in the project brief.

## Objective Coverage Summary
| Objective | Tooling Choice | Concrete Mechanism | What We Will Measure Later |
| --- | --- | --- | --- |
| Unified stream + batch analytics | Kafka, Spark, ElastiCache Redis, S3, Parquet | Stream events flow through Kafka and Spark Structured Streaming, batch files flow through Spark batch, live state is served from Redis, and historical data is stored in S3 as Parquet | end-to-end delay, batch completion time, correctness of live and historical outputs |
| Elastic scaling | Amazon EKS, Kubernetes HPA, KEDA, Spark dynamic allocation, Kafka partitions | Scale services using resource metrics, Kafka lag, and executor demand | replica count, executor count, Kafka lag, throughput, recovery under burst |
| Exactly-once or effectively-once semantics | Kafka, Spark checkpoints, event IDs, sink deduplication | Replay safely after failure and suppress harmful duplicates using checkpoint and idempotent logic | duplicate count, data loss count, replay correctness after restart |
| Cost-aware scheduling and resource allocation | Airflow, Airflow pools, Spark dynamic allocation, KEDA, ElastiCache Redis, S3, Parquet | Schedule expensive work deliberately, limit concurrency, reduce idle compute, keep Redis limited to hot state, and use efficient storage | cost per GB, active compute time, average replicas or executors, latency-cost trade-off |
| Realistic enterprise evaluation | CloudWatch, Prometheus, Grafana, Airflow, Terraform, Amazon EKS | Run controlled scenarios repeatedly and capture both AWS runtime and pipeline metrics consistently | latency, throughput, lag, failures, recovery time, cost proxies |

## Objective 1: Build A Unified Cloud Data Pipeline For Real-Time And Batch Analytics
### What This Objective Really Means
The pipeline should support both:

- live event handling for near-real-time outputs
- scheduled or periodic processing for historical reports and analytics

### Chosen Tools
- `Apache Kafka` for real-time event ingestion
- `Apache Spark Structured Streaming` for stream processing
- `Apache Spark batch` for batch processing
- `Amazon ElastiCache for Redis OSS or Valkey` for hot operational state
- `Amazon S3 + Parquet` for historical and analytical storage

### Why These Tools Fit
`Spark` is the key choice here because one engine handles both stream and batch work. That gives you a more unified architecture than using separate engines for each path.

`Kafka` gives a clean stream entry point, `ElastiCache Redis` gives a fast live-state layer on AWS, and `S3 + Parquet` gives a strong historical analytics layer without needing a separate warehouse early in the project.

### How We Will Prove This Objective
We will demonstrate:

- live telemetry flowing through the stream path with low delay
- batch logs being processed separately on schedule
- current operational state available quickly
- historical analytical outputs available for reporting

### Why Not Flink As The Main Recommendation
`Flink` is a strong stream processor, but `Spark` is lower-risk for this assignment because:

- you need both stream and batch
- one engine keeps the design simpler
- evaluation becomes easier to defend academically

If the project focused only on advanced streaming behavior, Flink would be a stronger candidate.

## Objective 2: Implement Elastic Scaling For Ingestion And Processing Layers
### What This Objective Really Means
The system should grow when backlog or workload increases and shrink when demand falls.

### Chosen Tools
- `Amazon EKS`
- `Kubernetes HPA`
- `KEDA`
- `Spark dynamic allocation`
- `Kafka partitions and consumer scaling`

### Why These Tools Fit
`HPA` is useful when scaling based on resource usage such as CPU or memory.

`KEDA` is useful when scaling based on event workload, especially Kafka lag.

`Spark dynamic allocation` lets the processing layer request more or fewer executors based on work demand.

Together, these mechanisms give you a convincing answer for both:

- ingestion-side elasticity
- processing-side elasticity

In simple terms:

- if messages start piling up in Kafka, KEDA helps add more ingestion-side workers
- if Spark has too much work, dynamic allocation helps add more Spark executors
- when traffic drops, both sides can shrink again

### How We Will Prove This Objective
We will demonstrate:

- scale-out under burst traffic
- scale-in after workload drops
- reduced lag or backlog after scaling
- comparison against a fixed-capacity baseline

### Why This Is Better Than Just Saying "Kubernetes Can Scale"
That would be too vague for a project report. Your professor asked for elastic scaling, so you need named mechanisms that explain:

- what is being monitored
- what triggers scaling
- what component actually scales

## Objective 3: Ensure Exactly-Once Or Effectively-Once Processing Semantics
### What This Objective Really Means
The system should avoid incorrect duplicates even during retries, restarts, or temporary failures.

### Chosen Tools And Design
- `Kafka` with durable event storage
- `Spark Structured Streaming` with checkpoints
- idempotent event handling using `event_id`
- deterministic writes or deduplication at the sink

### Why These Tools Fit
For this project, the most practical target is usually `effectively-once` end to end.

That means:

- if the job restarts, Kafka and Spark can replay safely
- checkpointing preserves processing progress
- `event_id` based deduplication prevents harmful duplicates in outputs

This is easier to implement and explain than trying to prove strict exactly-once across every sink in the whole system.

### How We Will Prove This Objective
We will demonstrate:

- restart or failure during processing
- replay from Kafka and Spark checkpoint recovery
- no harmful duplicate business outputs after deduplication
- no intentional data loss in the tested scenarios

### Why I Recommend Effectively-Once As The Main Target
The professor allows either:

- exactly-once
- effectively-once

For a student project, effectively-once is often the safer and more defensible choice because it is easier to demonstrate under failure testing.

## Objective 4: Incorporate Cost-Aware Scheduling And Resource Allocation
### What This Objective Really Means
The system should avoid paying for more compute than needed and should schedule expensive work intelligently.

### Chosen Tools And Controls
- `Airflow` for job timing and workflow scheduling
- `Airflow pools` to limit concurrency for expensive jobs
- `Spark dynamic allocation` to reduce idle executors
- `KEDA` to reduce some workers when event backlog is low
- `Amazon ElastiCache for Redis OSS or Valkey` used only as a hot-state layer
- `S3 + Parquet` for lower-cost historical storage

### Why These Tools Fit
There is no single tool in this stack that automatically solves cost optimization. Instead, the design combines scheduling, autoscaling, and storage choices.

This gives you a practical and explainable optimization story:

- run heavy batch jobs when needed, not continuously
- restrict parallel expensive work
- let compute expand and shrink with load
- keep historical data in a cheaper format and storage layer

### How We Will Prove This Objective
We will demonstrate:

- a fixed-capacity baseline with always-on resource use
- an optimized setup using scheduling and elastic resource controls
- lower cost proxy metrics or lower cost per processed volume
- whether that saving preserves the latency target

### Why I Did Not Recommend A More Complex Cost Toolset
You could add more cloud-native cost tools later, but that would increase complexity without improving your marks much unless cost optimization itself becomes your chosen research improvement.

## Objective 5: Evaluate Performance Under Realistic Enterprise Workloads
### What This Objective Really Means
You need a repeatable experiment setup, not just a working demo.

### Chosen Tools
- `Kafka` for controllable event-rate experiments
- `Spark` for measurable processing behavior
- `Amazon CloudWatch + Container Insights` for AWS runtime logs and infrastructure metrics
- `self-managed Prometheus + self-managed Grafana on EKS` for pipeline metrics and dashboards
- `Airflow` for repeatable benchmark workflow runs
- `Terraform + Amazon EKS` for reproducible deployments

### Why These Tools Fit
`CloudWatch` gives you AWS-side runtime visibility, while `Prometheus + Grafana` gives you the pipeline-level graphs your professor wants:

- throughput
- lag
- delay
- failures
- recovery behavior

`Airflow` helps repeat experiments in a controlled way, and `Terraform` helps you recreate the same EKS environment for baseline and optimized runs.

### How We Will Prove This Objective
We will demonstrate controlled experiments for:

- normal load
- burst load
- late or replayed data
- failure and recovery

For each run, we will capture the same metrics so baseline and optimized results are directly comparable.

## AWS Monitoring And Hot-Store Decisions
### Why Self-Managed Prometheus And Grafana On EKS
For this project, self-managed Prometheus and Grafana on EKS are the better choice than adding AWS managed observability services everywhere because:

- the monitoring stack stays inside the same Kubernetes story as the rest of the platform
- custom Kafka, Spark, and Airflow metrics are easier to explain in one cluster-based setup
- the project remains easier to demo and reason about as a student system

### Why CloudWatch Is Still Included
CloudWatch is still important because the project runs on AWS and needs:

- EKS infrastructure visibility
- container logs
- alarms and runtime health signals

### Why Redis Stays And DynamoDB Does Not Replace It
The hot-state requirement is better matched by Redis than DynamoDB because:

- the design needs a fast live-state layer
- the main use case is current operational state, not a durable primary NoSQL table
- using ElastiCache keeps Redis managed on AWS without changing the architecture story
- DynamoDB would change the role of the hot layer from in-memory operational state to a more durable NoSQL table model

## Final Justification In One View
| Objective | Main Tools | Why This Combination Works |
| --- | --- | --- |
| Unified stream + batch analytics | Kafka, Spark, S3, Parquet, ElastiCache Redis | One stream path, one batch path, one processing engine |
| Elastic scaling | Amazon EKS, Kubernetes HPA, KEDA, Spark dynamic allocation | Covers both metric-based and backlog-based scaling |
| Exactly-once or effectively-once | Kafka, Spark checkpoints, event IDs | Strong replay and deduplication story |
| Cost-aware scheduling | Airflow, Spark dynamic allocation, KEDA, ElastiCache Redis, S3, Parquet | Controls compute timing, parallelism, and storage cost |
| Realistic evaluation | CloudWatch, Prometheus, Grafana, Airflow, Terraform, Amazon EKS | Supports controlled, repeatable benchmarking |

## Final Recommendation
If your goal is the highest chance of completing the project well and defending it confidently, the safest stack is:

- `Kafka`
- `Spark`
- `Amazon EKS`
- `Kubernetes HPA`
- `KEDA`
- `Amazon ElastiCache for Redis OSS or Valkey`
- `S3`
- `Parquet`
- `Airflow`
- `CloudWatch`
- `Terraform`
- `self-managed Prometheus`
- `self-managed Grafana`

This stack is not the only valid one. It is the one that best balances:

- assignment fit
- implementation difficulty
- evaluation quality
- risk of architectural mistakes
