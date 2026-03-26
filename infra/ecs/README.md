# ECS Full-Cloud Deployment

This directory contains the simplest path from the current hybrid setup to a full-cloud deployment on AWS.

## What "full cloud" means here

- the Flink pipeline runs as an ECS/Fargate service in AWS
- the simulator runs as an ECS/Fargate task in AWS
- Kinesis, DynamoDB, SNS, and CloudWatch Logs remain in AWS
- your Mac is only used to build and push the image to ECR

## Why ECS/Fargate first

For this project stage, ECS/Fargate is the cleanest bridge from local Docker to cloud-hosted execution:

- same Docker image as local development
- no Kubernetes control-plane complexity yet
- fast path to measuring a true cloud-hosted baseline
- keeps the future EKS phase separate for comparison

## Files

- `deploy.sh` builds and pushes the image, creates AWS prerequisites, and deploys the long-running pipeline service
- `run_simulator.sh` starts a one-off simulator task in ECS

## Before you run it

Make sure these AWS resources already exist in `ap-south-1`:

- Kinesis stream `vacciguard-stream`
- DynamoDB table `VacciguardFridgeState`
- SNS FIFO topic if you want Phase 5 alerts

## Deploy the cloud pipeline

From the repo root:

```bash
export AWS_DEFAULT_REGION=ap-south-1
export VACCIGUARD_ENABLE_ALERTS=false
bash infra/ecs/deploy.sh
```

For Phase 5 alerting:

```bash
export AWS_DEFAULT_REGION=ap-south-1
export VACCIGUARD_ENABLE_ALERTS=true
export VACCIGUARD_SNS_ALERT_TOPIC_ARN='arn:aws:sns:ap-south-1:<account-id>:vacciguard-alerts.fifo'
bash infra/ecs/deploy.sh
```

## Run the simulator in the cloud

```bash
bash infra/ecs/run_simulator.sh
```

## What the deploy script creates

- ECR repository
- ECS cluster
- ECS execution role
- ECS task role with access to Kinesis, DynamoDB, and SNS
- CloudWatch log groups
- task definitions for the pipeline and simulator
- ECS service for the long-running pipeline

## Research workflow

Recommended sequence:

1. measure hybrid baseline: local containers + AWS services
2. deploy ECS full-cloud baseline
3. run the same workload from ECS using `run_simulator.sh`
4. compare throughput, lag, and stability between hybrid and full-cloud
5. later compare ECS against your future optimized or EKS-based pipeline
