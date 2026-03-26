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

The baseline is intentionally kept stable and consistent so that later optimization results can be compared against a fixed cloud-deployed reference system.

## Current Status

| Phase | Description | Status |
|---|---|---|
| Phase 4 | Simulator -> Kinesis -> Flink -> DynamoDB | Complete |
| Phase 5 | Breach detection + SNS alerts | Implemented and configurable |
| Phase 6 | S3 cold storage + Glue + Athena | Planned |
| Phase 7 | EKS deployment + Terraform | Planned |
| Phase 8 | Predictive vs reactive auto-scaling experiments | Planned |

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
| `VACCIGUARD_CHECKPOINT_INTERVAL_MS` | Flink checkpoint interval | `30000` |
| `VACCIGUARD_FLINK_PARALLELISM` | Flink parallelism | `1` |
| `VACCIGUARD_KINESIS_INITIAL_POSITION` | Stream start position | `LATEST` |

## Deployment

The repository includes a full-cloud ECS/Fargate deployment path:

- [infra/ecs/deploy.sh](/Users/tanishgupta/Desktop/VACCIGUARD/infra/ecs/deploy.sh) deploys the pipeline service
- [infra/ecs/run_simulator.sh](/Users/tanishgupta/Desktop/VACCIGUARD/infra/ecs/run_simulator.sh) runs the simulator as a one-off task
- [infra/ecs/README.md](/Users/tanishgupta/Desktop/VACCIGUARD/infra/ecs/README.md) explains the ECS deployment flow

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
