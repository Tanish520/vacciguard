# VacciGuard — Real-Time Vaccine Cold Chain Monitor

> CSG527 Cloud Computing — BITS Pilani Hyderabad Campus, 2025-26 S2
> Deadline: 19 April 2026

---

## The Problem

India administers over 400 million vaccine doses annually. Every dose must be stored between **2°C and 8°C**. A single refrigerator failure can silently destroy thousands of doses before anyone notices.

VacciGuard is a real-time cloud-native data pipeline that monitors **27,000 vaccine refrigerators** across India. It detects temperature breaches, door-open events, and battery failures — and alerts district health officers within 5 seconds.

---

## Architecture

```
┌─────────────────────┐
│   Python Simulator  │  Generates mock telemetry for 27,000 fridges
│   simulator.py      │  (temperature, door, battery, location)
└────────┬────────────┘
         │ boto3 PutRecords
         ▼
┌─────────────────────┐
│   Amazon Kinesis    │  Real-time ingestion stream
│   vacciguard-stream │  Region: ap-south-1
│   (1 shard)         │
└────────┬────────────┘
         │ FlinkKinesisConsumer
         ▼
┌─────────────────────┐
│   Apache Flink      │  Stream processing (PyFlink 1.18)
│   pipeline.py       │  Runs in Docker container
│                     │  ├── Breach detection (Phase 5)
│                     │  ├── Enrichment (Phase 5)
│                     │  └── Alert routing (Phase 5)
└────────┬────────────┘
         │ boto3 PutItem
         ▼
┌─────────────────────┐
│   Amazon DynamoDB   │  Hot storage — latest fridge state
│   VacciguardFridge  │  Composite key: fridge_id + timestamp
│   State             │
└─────────────────────┘

         (Future phases)
         ▼
┌─────────────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Amazon S3         │────▶│   AWS Glue       │────▶│   Athena    │
│   Cold storage      │     │   ETL + Catalog  │     │   Reports   │
│   (Parquet)         │     └──────────────────┘     └─────────────┘
└─────────────────────┘

┌─────────────────────┐
│   Amazon SNS        │  SMS alerts to duty officers on breach
└─────────────────────┘
```

---

## Phase Status

| Phase | Description | Status |
|---|---|---|
| Phase 4 | Simulator → Kinesis → PyFlink → DynamoDB | ✅ Complete (2026-03-26) |
| Phase 5 | Breach detection + SNS alerts | 🔄 Next |
| Phase 6 | S3 cold storage + Glue + Athena | ⏳ Planned |
| Phase 7 | EKS deployment + Terraform | ⏳ Planned |
| Phase 8 | Auto-scaling experiments (predictive vs reactive) | ⏳ Planned |

---

## Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Data source | Python + boto3 | Simulates 27,000 fridge telemetry records |
| Ingestion | Amazon Kinesis | Real-time stream ingestion |
| Stream processing | Apache Flink (PyFlink 1.18) | Consumes stream, detects breaches |
| Hot storage | Amazon DynamoDB | Stores latest fridge state per event |
| Cold storage | Amazon S3 (Parquet) | Long-term data lake |
| Batch processing | AWS Glue + Athena | Nightly compliance reports |
| Orchestration | Amazon MWAA (Airflow) | Schedules batch jobs |
| Alerts | Amazon SNS | SMS to duty officers on breach |
| Monitoring | CloudWatch + Grafana | Pipeline observability |
| Infra as Code | Terraform | All AWS resources |
| Container | Docker (linux/amd64) | Consistent environment across all OS |
| AWS Region | ap-south-1 (Mumbai) | Closest to India operations |

---

## File Structure

```
vacciguard/
│
├── config.py                        # Shared constants — ALWAYS import from here
│                                    # Never hardcode stream names, table names, or region
│
├── requirements.txt                 # Python dependencies (boto3, apache-flink, etc.)
│
├── simulator/
│   └── simulator.py                 # Generates 500 mock fridge records
│                                    # Pushes to Kinesis using boto3 PutRecords
│                                    # PartitionKey = fridge_id
│
├── flink/
│   └── pipeline.py                  # PyFlink job: reads Kinesis → writes DynamoDB
│                                    # DynamoDBSink: composite key fridge_id + timestamp
│                                    # Add breach detection here in Phase 5
│
├── lib/
│   └── flink-sql-connector-         # Kinesis connector JAR for PyFlink
│       kinesis-4.3.0-1.18.jar       # NOT in git (63MB) — downloaded by setup.sh
│
├── Dockerfile                       # Container: eclipse-temurin:17-jdk + Python + PyFlink
│                                    # Uses linux/amd64 for prebuilt wheel compatibility
│
├── docker-compose.yml               # Two services:
│                                    # flink-pipeline — long-running Flink job
│                                    # simulator      — one-shot data producer
│
├── setup.sh                         # ONE-TIME setup script for all teammates
│                                    # Downloads JAR, builds Docker images
│
├── SETUP.md                         # Full step-by-step setup guide for teammates
│                                    # Includes AWS resource creation commands
│
└── README.md                        # This file
```

---

## Quick Start

**Full setup guide → see [SETUP.md](SETUP.md)**

**Short version:**

```bash
# 1. Clone
git clone https://github.com/Tanish520/vacciguard.git
cd vacciguard

# 2. One-time setup (downloads JAR + builds Docker images)
bash setup.sh

# 3. Terminal 1 — start the pipeline
docker compose up flink-pipeline

# 4. Terminal 2 — send test data
docker compose run --rm simulator

# 5. Verify — check DynamoDB
aws dynamodb scan --table-name VacciguardFridgeState --region ap-south-1 --max-items 5
```

---

## Fridge Telemetry Schema

Every record produced by the simulator and stored in DynamoDB:

```json
{
  "fridge_id":     "VCF-0042",
  "temperature":   7.8,
  "door_open":     false,
  "battery_level": 91,
  "location":      "Rohini-PHC",
  "district":      "North-West-Delhi",
  "state":         "Delhi",
  "timestamp":     "2026-03-26T09:00:10Z"
}
```

**DynamoDB key schema:**
- Partition key: `fridge_id` (String)
- Sort key: `timestamp` (String)

---

## AWS Resources

| Resource | Name | Region |
|---|---|---|
| Kinesis Stream | `vacciguard-stream` | ap-south-1 |
| DynamoDB Table | `VacciguardFridgeState` | ap-south-1 |
| S3 Bucket | `vacciguard-data-lake-bits` | ap-south-1 (Phase 6) |
| SNS Topic | `vacciguard-alerts` | ap-south-1 (Phase 5) |

---

## Team Structure

| Role | Owns | Responsibility |
|---|---|---|
| Pipeline Lead | `flink/` | Stream processing, breach detection |
| Infra Lead | `infra/` | Terraform for all AWS resources |
| Data Lead | `simulator/`, `batch/` | Fridge data, Glue jobs, Airflow DAG |
| Research Lead | `monitoring/` | Grafana dashboard, scaling experiments |

## Branch Structure

```
main                  → always working, never push directly
yourname-pipeline     → Pipeline Lead
teammate2-infra       → Infra Lead
teammate3-data        → Data Lead
teammate4-research    → Research Lead
```

---

## Coding Rules

1. **Never hardcode** AWS resource names — always import from `config.py`
2. **Never commit** `.env` files — credentials stay local only
3. **DynamoDB writes** must use composite key (`fridge_id` + `timestamp`)
4. **SNS publish** must include `MessageDeduplicationId`
5. **All Flink time operations** must use EventTime, not ProcessingTime
6. **Only the Infra Lead** runs `terraform apply`

---

## Research Contribution

We compare two auto-scaling strategies on the Flink pipeline:

- **Baseline (reactive):** scales after consumer lag exceeds 5,000ms
- **Ours (predictive):** pre-scales using Co-WIN vaccination drive schedule

Each experiment runs 5 times. Results reported with mean, std, 95% CI, and t-test.

**Throughput test levels:** 1,000 → 5,000 → 10,000 → 20,000 → 27,000 fridges
