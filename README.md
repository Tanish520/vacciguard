# VacciGuard

Real-time vaccine cold-chain monitoring pipeline for cloud-based research and performance evaluation.

## Overview

VacciGuard simulates vaccine refrigerator telemetry, streams it through Amazon Kinesis, processes it with Apache Flink, and stores the resulting records in Amazon DynamoDB.

The current project focus is to maintain a stable baseline pipeline deployed on AWS, measure its performance under different conditions, and later compare it with an optimized cloud deployment.

## Architecture

```text
Simulator -> Amazon Kinesis -> Apache Flink -> Amazon DynamoDB
                                   \
                                    -> Amazon SNS (optional Phase 5 alerts)
```

## What Has Been Achieved

- Built an end-to-end baseline pipeline for fridge telemetry.
- Verified simulator-to-Kinesis ingestion.
- Verified cloud-based Flink processing.
- Verified successful writes into DynamoDB.
- Added an AWS ECS/Fargate deployment path for the baseline pipeline.
- Added an AWS ECS/Fargate one-off simulator run path.
- Kept Phase 5 alert support configurable without changing the baseline flow.

## Baseline Pipeline

The baseline pipeline serves as the reference implementation for the project. It is the system against which future optimized versions will be evaluated during performance analysis.

The baseline includes the following components:

- **Simulator**: generates refrigerator telemetry records containing temperature, door status, battery level, location, district, state, and timestamp.
- **Amazon Kinesis**: acts as the real-time ingestion layer for incoming telemetry events.
- **Apache Flink**: performs stream consumption, input validation, and baseline event processing.
- **Amazon DynamoDB**: stores the processed refrigerator records for downstream inspection and analysis.
- **Amazon SNS**: publishes alert notifications when breach conditions are detected.
- **AWS ECS/Fargate**: hosts both the baseline pipeline service and the simulator in the cloud.

Functionally, the baseline pipeline:

- ingests real-time telemetry data
- processes events in the cloud
- stores processed records in DynamoDB
- evaluates breach conditions such as temperature greater than `8.0C` and `door_open == true`
- publishes SNS alerts for detected breach events

The baseline also now includes measurement instrumentation for performance analysis. It records:

- DynamoDB end-to-end latency summaries (`P50`, `P90`, `P99`)
- alert end-to-end latency summaries (`P50`, `P90`, `P99`)
- throughput in records per second
- total processed records
- total failed or invalid records
- total breach events
- total alerts published
- duplicate alert count
- SLA violation count for events exceeding the configured threshold

The baseline is intentionally kept stable and consistent so that later optimization results can be compared against a fixed cloud-deployed reference system.

## Current Status

| Phase | Description | Status |
|---|---|---|
| Phase 4 | Simulator -> Kinesis -> Flink -> DynamoDB | Complete |
| Phase 5 | Breach detection + SNS alerts | Implemented and configurable |
| Phase 6 | S3 cold storage + Glue + Athena | Planned |
| Phase 7 | EKS deployment + Terraform | Planned |
| Phase 8 | Predictive vs reactive auto-scaling experiments | Planned |

## Experiment Status

The baseline pipeline has been prepared for controlled performance evaluation, including CloudWatch metrics, SNS-enabled alerting, and experiment-oriented simulator configuration.

Formal experiment execution is currently paused. The project is intentionally preserving the current baseline state so that future optimization work can be compared against a stable and reproducible reference implementation.

When experimentation resumes, the same frozen baseline will be used as the comparison point for the optimized pipeline.

## AWS Resources

| Resource | Name | Region |
|---|---|---|
| Kinesis stream | `vacciguard-stream` | `ap-south-1` |
| DynamoDB table | `VacciguardFridgeState` | `ap-south-1` |
| SNS FIFO topic | `vacciguard-alerts.fifo` | `ap-south-1` |

## Quick Start

Follow the full setup guide in [SETUP.md](/Users/tanishgupta/Desktop/VACCIGUARD/SETUP.md).

Main cloud baseline flow:

```bash
cd /Users/tanishgupta/Desktop/VACCIGUARD
export AWS_DEFAULT_REGION=ap-south-1
export VACCIGUARD_ENABLE_ALERTS=false
bash infra/ecs/deploy.sh
bash infra/ecs/run_simulator.sh
```

Verify results:

```bash
aws dynamodb scan \
  --table-name VacciguardFridgeState \
  --region ap-south-1 \
  --max-items 5
```

## Configuration

These environment variables control the baseline and Phase 5 behavior:

| Variable | Purpose | Default |
|---|---|---|
| `VACCIGUARD_ENABLE_ALERTS` | Enable or disable SNS alert publishing | `true` |
| `VACCIGUARD_SNS_ALERT_TOPIC_ARN` | SNS topic ARN for alert publishing | empty |
| `VACCIGUARD_BREACH_TEMP_CELSIUS` | Breach temperature threshold | `8.0` |
| `VACCIGUARD_WARNING_TEMP_CELSIUS` | Warning temperature threshold | `7.5` |
| `VACCIGUARD_SLA_THRESHOLD_MS` | SLA threshold for breach processing | `5000` |
| `VACCIGUARD_CHECKPOINT_INTERVAL_MS` | Flink checkpoint interval | `30000` |
| `VACCIGUARD_FLINK_PARALLELISM` | Flink parallelism | `1` |
| `VACCIGUARD_METRICS_SUMMARY_INTERVAL` | Records between metric summaries | `100` |
| `VACCIGUARD_METRICS_NAMESPACE` | CloudWatch custom metrics namespace | `VacciGuard/BaselinePipeline` |
| `VACCIGUARD_KINESIS_INITIAL_POSITION` | Stream start position | `LATEST` |

## Deployment

The repository includes a full-cloud ECS/Fargate deployment path:

- [infra/ecs/deploy.sh](/Users/tanishgupta/Desktop/VACCIGUARD/infra/ecs/deploy.sh) deploys the pipeline service
- [infra/ecs/run_simulator.sh](/Users/tanishgupta/Desktop/VACCIGUARD/infra/ecs/run_simulator.sh) runs the simulator as a one-off task
- [infra/ecs/README.md](/Users/tanishgupta/Desktop/VACCIGUARD/infra/ecs/README.md) explains the ECS deployment flow

## Metrics and Evaluation

The baseline pipeline emits periodic summary logs and CloudWatch custom metrics under:

```text
VacciGuard/BaselinePipeline
```

The most important baseline metrics are:

- `RecordsProcessedTotal`
- `RecordsFailedTotal`
- `BreachEventsTotal`
- `AlertsPublishedTotal`
- `DuplicateAlertsTotal`
- `SlaViolationsTotal`
- `ThroughputRecordsPerSecond`
- `DynamoDbLatencyP50Ms`
- `DynamoDbLatencyP90Ms`
- `DynamoDbLatencyP99Ms`
- `AlertLatencyP50Ms`
- `AlertLatencyP90Ms`
- `AlertLatencyP99Ms`

These metrics provide the pipeline-level baseline for later comparison with the optimized pipeline.

### Metric Definitions and Purpose

`RecordsProcessedTotal`  
Definition: Total number of records successfully processed by the pipeline.  
Purpose: Measures how much useful work the pipeline completed.

`RecordsFailedTotal`  
Definition: Total number of records that failed during processing.  
Purpose: Measures reliability and helps compare failure behavior.

`BreachEventsTotal`  
Definition: Total number of events identified as breach events.  
Purpose: Shows how many records triggered alert conditions.

`AlertsPublishedTotal`  
Definition: Total number of SNS alerts successfully published.  
Purpose: Measures how many detected breach events resulted in alert publication.

`DuplicateAlertsTotal`  
Definition: Total number of alerts identified as duplicates based on the pipeline deduplication logic.  
Purpose: Helps evaluate alert quality and duplicate suppression behavior.

`SlaViolationsTotal`  
Definition: Total number of events whose processing latency exceeded the configured SLA threshold.  
Purpose: Directly measures how often the pipeline fails the time-based alerting objective.

`ThroughputRecordsPerSecond`  
Definition: Number of records processed per second by the pipeline.  
Purpose: Measures processing capacity under load.

`DynamoDbLatencyP50Ms`  
Definition: 50th percentile end-to-end latency from event timestamp to DynamoDB write path.  
Purpose: Represents the typical processing latency.

`DynamoDbLatencyP90Ms`  
Definition: 90th percentile end-to-end latency from event timestamp to DynamoDB write path.  
Purpose: Shows the slower end of regular processing behavior.

`DynamoDbLatencyP99Ms`  
Definition: 99th percentile end-to-end latency from event timestamp to DynamoDB write path.  
Purpose: Captures tail latency and stress behavior, which is especially important during traffic surges.

`AlertLatencyP50Ms`  
Definition: 50th percentile end-to-end latency from event timestamp to SNS alert publish.  
Purpose: Represents the typical alert publication delay.

`AlertLatencyP90Ms`  
Definition: 90th percentile end-to-end latency from event timestamp to SNS alert publish.  
Purpose: Shows how alerting behaves for slower but still common events.

`AlertLatencyP99Ms`  
Definition: 99th percentile end-to-end latency from event timestamp to SNS alert publish.  
Purpose: Captures worst-case alert delay and is important for SLA-focused analysis.

## Evaluation Methodology

This section explains how the baseline pipeline will be evaluated and how the results will later be compared with the optimized pipeline.

### Evaluation Goal

The purpose of the evaluation is not simply to show that the pipeline works. The purpose is to measure how the baseline behaves under load, how reliably it processes healthcare-related events, and whether it continues to satisfy the alerting time requirement.

The baseline pipeline is treated as the fixed reference system. After its performance is measured, the optimized pipeline will be tested under the same conditions and compared against the baseline.

### Evaluation Principle

The evaluation follows a simple rule:

- keep the baseline pipeline stable
- run controlled workloads
- collect the same metrics in every run
- compare the optimized pipeline against the same measurement criteria

This ensures the comparison is fair and scientifically meaningful.

### Main Evaluation Categories

To make the analysis easy to interpret, the metrics are grouped into three main categories.

#### 1. Speed and Throughput

These metrics measure how much traffic the pipeline can process.

- `ThroughputRecordsPerSecond`
- `RecordsProcessedTotal`
- `RecordsFailedTotal`

These values answer questions such as:
- How many records can the pipeline process each second?
- Does throughput remain stable when load increases?
- Does the pipeline keep processing reliably under stress?

#### 2. Correctness and Alert Reliability

These metrics measure whether the pipeline is doing the right work, not just fast work.

- `BreachEventsTotal`
- `AlertsPublishedTotal`
- `DuplicateAlertsTotal`

These values answer questions such as:
- Did the pipeline correctly identify breach events?
- Did breach events actually result in alert publication?
- Did the system avoid sending the same alert multiple times?

This category is important because a healthcare monitoring system must be both fast and trustworthy.

#### 3. Latency and SLA Adherence

These metrics measure how quickly the pipeline reacts to events.

- `DynamoDbLatencyP50Ms`
- `DynamoDbLatencyP90Ms`
- `DynamoDbLatencyP99Ms`
- `AlertLatencyP50Ms`
- `AlertLatencyP90Ms`
- `AlertLatencyP99Ms`
- `SlaViolationsTotal`

These values answer questions such as:
- What is the typical processing delay?
- How bad does the delay become under stress?
- How often does the system violate the 5-second SLA?

The percentile-based latency metrics are especially important. In cloud systems research, average latency alone is often misleading. A system may appear fast on average while still producing very poor worst-case behavior. For that reason, the 99th percentile (`P99`) is treated as one of the most important evaluation metrics.

### Why Percentiles Matter

Latency percentiles describe how the system behaves across the full range of events.

- `P50` represents the median and shows the typical experience.
- `P90` represents slower but still common cases.
- `P99` represents the worst 1% of cases and highlights tail-latency behavior.

For example, a pipeline may have a reasonable median latency but still fail the SLA for a meaningful number of events if its `P99` latency becomes very high during a traffic surge.

### How the Baseline Will Be Tested

The baseline pipeline is evaluated by running controlled simulator workloads and collecting the CloudWatch metrics and logs produced by the pipeline.

Each experiment should follow the same basic process:

1. Start the cloud-deployed baseline pipeline.
2. Trigger a simulator workload.
3. Observe CloudWatch metrics and pipeline logs.
4. Verify processed records in DynamoDB.
5. Record the results for later comparison.

To keep the evaluation consistent, the same AWS region, resource setup, and workload definition should be used across repeated runs.

### How the Results Will Be Used

The collected metrics will be used to compare the baseline pipeline against the future optimized pipeline.

The comparison should answer questions such as:
- Does the optimized pipeline process more records per second?
- Does it reduce `P99` latency?
- Does it produce fewer SLA violations?
- Does it maintain alert correctness and duplicate suppression?
- Does it improve performance without increasing failures?

### Scope of the Current Evaluation

At the current stage, this methodology applies to the **pipeline baseline**. That means the focus is on:

- pipeline throughput
- latency behavior
- alert correctness
- SLA adherence
- failure counts

More advanced comparisons such as scaling responsiveness, recovery behavior, and predictive-versus-reactive scaling belong to the later **scaling baseline** stage of the project and will be added separately.

### Summary

In practical terms, the evaluation methodology is designed to answer one central research question:

**How well does the cloud-deployed baseline pipeline perform, and how much improvement does the optimized pipeline deliver under the same conditions?**

By keeping the baseline stable and measuring speed, correctness, and latency in a consistent way, the project creates a reliable foundation for a meaningful research comparison.

## Repository Structure

```text
vacciguard/
├── config.py
├── requirements.txt
├── simulator/
│   └── simulator.py
├── flink/
│   └── pipeline.py
├── lib/
│   └── flink-sql-connector-kinesis-4.3.0-1.18.jar
├── infra/
│   └── ecs/
├── Dockerfile
├── docker-compose.yml
├── setup.sh
├── SETUP.md
└── README.md
```
