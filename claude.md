# CLAUDE.md — VacciGuard Project Briefing
# Read this file before answering anything about this codebase.

## What this project is
VacciGuard is a real-time cloud data pipeline that monitors 27,000 vaccine
refrigerators across India. It detects temperature breaches above 8°C and
alerts district health officers via SMS within 5 seconds.
Course: CSG527 Cloud Computing — BITS Pilani Hyderabad Campus, 2025-26 S2.
Deadline: 19th April 2026.

## Tech stack (AWS, region us-east-1)
- Data source:    Python simulator (boto3) → Amazon Kinesis (3 shards)
- Stream process: Apache Flink (PyFlink) on Amazon EKS
- Hot storage:    Amazon DynamoDB — composite key: fridge_id + timestamp
- Cold storage:   Amazon S3 — Parquet, partitioned by state/district/date
- Batch:          AWS Glue + Athena — nightly compliance report at 6am
- Orchestration:  Amazon MWAA (managed Airflow)
- Alerts:         Amazon SNS — SMS to duty officer on breach
- Monitoring:     CloudWatch + Grafana
- Infra as Code:  Terraform (infra/main.tf)

## Shared constants — always import from config.py, never hardcode
- KINESIS_STREAM_NAME   = "vacciguard-stream"
- DYNAMO_TABLE_NAME     = "VacciguardFridgeState"
- DYNAMO_METADATA_TABLE = "VacciguardFridgeMetadata"
- S3_BUCKET_NAME        = "vacciguard-data-lake-bits"
- SNS_ALERT_TOPIC_ARN   = "arn:aws:sns:us-east-1:ACCOUNT_ID:vacciguard-alerts"
- BREACH_TEMP_CELSIUS   = 8.0
- WARNING_TEMP_CELSIUS  = 7.5
- CHECKPOINT_INTERVAL_MS = 30000
- REGION                = "us-east-1"

## Processing semantics — effectively-once (NOT exactly-once)
1. Flink checkpoints to S3 every 30 seconds
2. DynamoDB writes use composite key fridge_id + timestamp (idempotent)
3. SNS alerts use MessageDeduplicationId = md5(fridge_id + breach_timestamp)
4. Flink uses EventTime NOT ProcessingTime

## Folder ownership — one owner per folder
- simulator/   → Person C (Data Lead)       writes fridge data to Kinesis
- flink/       → Person A (Pipeline Lead)   stream processing + breach detection
- batch/       → Person C (Data Lead)       Glue job + Airflow DAG
- infra/       → Person B (Infra Lead)      Terraform for all AWS resources
- monitoring/  → Person D (Research Lead)   Grafana dashboard + experiments
- docs/        → shared

## Branch structure
- main                  → always working, never push directly
- yourname-pipeline     → Person A
- teammate2-infra       → Person B
- teammate3-data        → Person C
- teammate4-research    → Person D

## Coding rules — enforce these in every file
1. NEVER hardcode AWS resource names — always import from config.py
2. NEVER commit .env files — credentials stay local only
3. DynamoDB writes MUST use composite key (fridge_id + timestamp)
4. SNS publish MUST include MessageDeduplicationId
5. All Flink time operations MUST use EventTime not ProcessingTime
6. Only Person B (Infra Lead) runs terraform apply
7. Validate all incoming readings before processing (see Gap 4 fix)

## Data schema — every fridge reading looks like this
{
  "fridge_id":    "VCF-0042",
  "temperature":  7.8,
  "door_open":    false,
  "battery_level": 91,
  "location":    "Rohini-PHC",
  "district":    "North-West-Delhi",
  "state":       "Delhi",
  "timestamp":   "2026-03-19T09:00:10Z"
}

## Flink enrichment — join with VacciguardFridgeMetadata table
Every reading must be enriched with:
- capacity_doses  (from metadata table)
- vaccine_types   (from metadata table)
- officer_phone   (for SNS alert routing)
- doses_at_risk   (capacity_doses if breach, else 0)

## Research contribution — predictive vs reactive scaling
We compare two auto-scaling strategies:
- Baseline (reactive): scales AFTER lag exceeds 5,000ms
- Ours (predictive):   pre-scales using Co-WIN vaccination drive schedule
Run each experiment 5 times. Compute mean, std, 95% CI, t-test.

## Experiments to run
1. Throughput under load: 1,000 → 5,000 → 10,000 → 20,000 → 27,000 fridges
2. Predictive vs reactive scaling during vaccination drive surge (5 runs each)
3. Fault recovery: kill one Flink worker, measure recovery time + data integrity

## How to run locally
cd simulator && python3 simulator.py        # start fridge data flow
cd flink && python3 pipeline.py             # start stream processing
aws kinesis list-streams --region us-east-1 # check Kinesis

## Files that must exist before coding starts
- config.py          shared AWS resource names (no secrets)
- .env               your personal AWS keys (in .gitignore, never pushed)
- .gitignore         excludes .env, terraform state, __pycache__
- CLAUDE.md          this file