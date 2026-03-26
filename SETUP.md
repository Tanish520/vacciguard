# VacciGuard Setup Guide

This guide is written for someone who is new to cloud deployment and wants to set up and run the VacciGuard baseline pipeline on AWS.

## What You Are Setting Up

You will create and run a simple cloud pipeline with this flow:

```text
Simulator -> Amazon Kinesis -> Apache Flink -> Amazon DynamoDB
```

In this project:
- the simulator sends test fridge data
- Amazon Kinesis receives the streaming data
- Apache Flink processes the incoming records
- Amazon DynamoDB stores the processed records

The baseline pipeline runs on AWS using ECS/Fargate.

## Before You Start

You need:

| Requirement | Why you need it |
|---|---|
| AWS account | To create and run cloud resources |
| AWS CLI installed | To create AWS resources from the terminal |
| Docker Desktop installed | To build the container image |
| Git installed | To clone the project |

You should also know your AWS region. This project uses:

```bash
ap-south-1
```

## Step 1 - Configure AWS CLI

Run:

```bash
aws configure
```

When asked, enter:
- your AWS Access Key ID
- your AWS Secret Access Key
- default region: `ap-south-1`
- default output format: `json`

To confirm it works:

```bash
aws sts get-caller-identity
```

If this command returns your AWS account details, your AWS CLI is ready.

## Step 2 - Clone the Project

```bash
git clone https://github.com/Tanish520/vacciguard.git
cd vacciguard
```

## Step 3 - Build the Project Locally

Run:

```bash
bash setup.sh
```

This script will:
- check Docker
- check AWS CLI
- verify your AWS login works
- download the required Kinesis connector JAR
- build the Docker image

## Step 4 - Create the AWS Resources

Create the Kinesis stream:

```bash
aws kinesis create-stream \
  --stream-name vacciguard-stream \
  --shard-count 1 \
  --region ap-south-1
```

Create the DynamoDB table:

```bash
aws dynamodb create-table \
  --table-name VacciguardFridgeState \
  --attribute-definitions \
    AttributeName=fridge_id,AttributeType=S \
    AttributeName=timestamp,AttributeType=S \
  --key-schema \
    AttributeName=fridge_id,KeyType=HASH \
    AttributeName=timestamp,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region ap-south-1
```

Wait until both are ready:

```bash
aws kinesis wait stream-exists \
  --stream-name vacciguard-stream \
  --region ap-south-1

aws dynamodb wait table-exists \
  --table-name VacciguardFridgeState \
  --region ap-south-1
```

You can also verify manually:

```bash
aws kinesis describe-stream-summary \
  --stream-name vacciguard-stream \
  --region ap-south-1

aws dynamodb describe-table \
  --table-name VacciguardFridgeState \
  --region ap-south-1
```

Both should show `ACTIVE`.

## Step 5 - Deploy the Baseline Pipeline to AWS

Run:

```bash
cd /Users/tanishgupta/Desktop/VACCIGUARD
export AWS_DEFAULT_REGION=ap-south-1
export VACCIGUARD_ENABLE_ALERTS=false
bash infra/ecs/deploy.sh
```

What this does:
- pushes the image to Amazon ECR
- creates or updates the ECS cluster
- creates or updates the ECS pipeline service
- starts the baseline pipeline on AWS

## Step 6 - Run the Simulator on AWS

After the pipeline is deployed, run:

```bash
cd /Users/tanishgupta/Desktop/VACCIGUARD
bash infra/ecs/run_simulator.sh
```

This starts a one-time simulator task in AWS that sends records into Kinesis.

## Step 7 - Verify the Pipeline

### Check ECS

In AWS Console:
- open ECS
- open cluster `vacciguard-cluster`
- check that service `vacciguard-flink-pipeline` is running

### Check CloudWatch Logs

Look at these log groups:
- `/ecs/vacciguard/flink-pipeline`
- `/ecs/vacciguard/simulator`

You should see:
- pipeline startup logs
- simulator sending records

### Check DynamoDB

Run:

```bash
aws dynamodb scan \
  --table-name VacciguardFridgeState \
  --region ap-south-1 \
  --max-items 5
```

If you see records, the pipeline is working.

## Optional Step - Enable Phase 5 Alerts

Only do this if you want to test alerting.

Create the SNS FIFO topic:

```bash
aws sns create-topic \
  --name vacciguard-alerts.fifo \
  --attributes FifoTopic=true,ContentBasedDeduplication=false \
  --region ap-south-1
```

Then set:

```bash
export VACCIGUARD_SNS_ALERT_TOPIC_ARN='arn:aws:sns:ap-south-1:<account-id>:vacciguard-alerts.fifo'
export VACCIGUARD_ENABLE_ALERTS=true
```

Then redeploy:

```bash
bash infra/ecs/deploy.sh
```

## How To Start Again From Scratch

If you deleted the service, cluster, Kinesis stream, or DynamoDB table, repeat:
1. create Kinesis
2. create DynamoDB
3. wait until they are active
4. run `bash infra/ecs/deploy.sh`
5. run `bash infra/ecs/run_simulator.sh`

## How To Stop and Reduce Cost

To stop the pipeline service:

```bash
aws ecs update-service \
  --cluster vacciguard-cluster \
  --service vacciguard-flink-pipeline \
  --desired-count 0 \
  --region ap-south-1
```

To reduce the main runtime cost after you finish:

```bash
aws ecs update-service \
  --cluster vacciguard-cluster \
  --service vacciguard-flink-pipeline \
  --desired-count 0 \
  --region ap-south-1

aws kinesis delete-stream \
  --stream-name vacciguard-stream \
  --region ap-south-1

aws dynamodb delete-table \
  --table-name VacciguardFridgeState \
  --region ap-south-1
```

## Beginner Summary

If you want the shortest possible setup flow, do this:

1. `aws configure`
2. `git clone ...`
3. `bash setup.sh`
4. create Kinesis
5. create DynamoDB
6. `bash infra/ecs/deploy.sh`
7. `bash infra/ecs/run_simulator.sh`
8. verify DynamoDB
