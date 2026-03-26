# VacciGuard — Team Setup Guide

## What We Have Built (Phase 4 Complete)

VacciGuard is a real-time monitoring pipeline for vaccine refrigerators across India.

**Current working flow:**
```
Python Simulator → Amazon Kinesis → Apache Flink (PyFlink) → Amazon DynamoDB
```

- **Simulator** generates mock telemetry for 500 fridge units (temperature, door status, battery, location)
- **Kinesis** acts as the real-time ingestion stream
- **PyFlink** consumes the stream and processes each record
- **DynamoDB** stores every record with composite key `fridge_id + timestamp`

**Verified working on AWS (ap-south-1) on 26 March 2026.**

---

## Prerequisites

Install the following on your machine before starting:

| Tool | Purpose | Download |
|---|---|---|
| Docker Desktop | Runs the pipeline in a container | https://www.docker.com/products/docker-desktop |
| AWS CLI | Manage AWS resources from terminal | https://aws.amazon.com/cli/ |
| Git | Clone the repo | https://git-scm.com/ |

---

## Step 1 — AWS Setup

You need two AWS resources created in **ap-south-1 (Mumbai)**. Run these commands once.

**Configure AWS CLI with your credentials:**
```bash
aws configure
# Enter your Access Key ID, Secret Access Key, region: ap-south-1, output: json
```

**Create the Kinesis stream:**
```bash
aws kinesis create-stream \
  --stream-name vacciguard-stream \
  --shard-count 1 \
  --region ap-south-1
```

**Create the DynamoDB table:**
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

**Verify both are ACTIVE before continuing:**
```bash
aws kinesis describe-stream-summary --stream-name vacciguard-stream --region ap-south-1
aws dynamodb describe-table --table-name VacciguardFridgeState --region ap-south-1
```

Both should show `"StreamStatus": "ACTIVE"` and `"TableStatus": "ACTIVE"`.

---

## Step 2 — Clone and Setup

**Clone the repo:**
```bash
git clone https://github.com/Tanish520/vacciguard.git
cd vacciguard
```

**Run the one-time setup script:**

Mac / Linux / Windows WSL:
```bash
chmod +x setup.sh
./setup.sh
```

Windows Git Bash:
```bash
bash setup.sh
```

The setup script will:
1. Verify Docker and AWS CLI are installed
2. Verify your AWS credentials work
3. Download the Kinesis connector JAR (~63MB) — this is required by PyFlink and excluded from git due to size
4. Build the Docker images (~5 minutes on first run, cached after that)

---

## Step 3 — Run the Pipeline

**Terminal 1 — Start the Flink pipeline (keep this running):**
```bash
docker compose up flink-pipeline
```

Wait until you see:
```
[pipeline] Starting VacciGuard Flink job ...
[pipeline]   Source  : Kinesis stream 'vacciguard-stream' (ap-south-1)
[pipeline]   Sink    : DynamoDB table 'VacciguardFridgeState'
```

**Terminal 2 — Send 500 test records:**
```bash
docker compose run --rm simulator
```

Expected output:
```
[simulator] Sending 500 records to 'vacciguard-stream' ...
[simulator] Done. 500/500 records successfully sent.
```

---

## Step 4 — Verify Data in DynamoDB

```bash
aws dynamodb scan \
  --table-name VacciguardFridgeState \
  --region ap-south-1 \
  --max-items 5
```

You should see items like:
```json
{
  "fridge_id": "VCF-0042",
  "timestamp": "2026-03-26T08:47:15Z",
  "temperature": "7.8",
  "door_open": false,
  "battery_level": 91,
  "location": "Rohini-PHC",
  "district": "North-West-Delhi",
  "state": "Delhi"
}
```

If items appear — the pipeline is working end-to-end.

---

## Windows-Specific Notes

- `--platform=linux/amd64` is set in the Dockerfile. On Windows (WSL2/Docker Desktop), `linux/amd64` is the native platform — no emulation, no warnings, faster builds.
- Run all commands in **Git Bash**, **WSL**, or **PowerShell** with Docker Desktop running in the background.
- If Docker Desktop is not running, all `docker` commands will fail.

---

## Project Structure

```
vacciguard/
├── config.py                          # Shared constants (stream name, table name, region)
├── requirements.txt                   # Python dependencies
├── simulator/
│   └── simulator.py                   # Generates mock fridge telemetry → Kinesis
├── flink/
│   └── pipeline.py                    # PyFlink job: Kinesis → DynamoDB
├── lib/
│   └── flink-sql-connector-kinesis-4.3.0-1.18.jar   # Downloaded by setup.sh
├── Dockerfile                         # Linux/amd64 container with Java 17 + PyFlink
├── docker-compose.yml                 # Two services: flink-pipeline + simulator
├── setup.sh                           # One-time setup script
└── SETUP.md                           # This file
```

---

## Fridge Telemetry Schema

Every record in DynamoDB follows this exact schema:

| Field | Type | Example |
|---|---|---|
| `fridge_id` | String (PK) | VCF-0042 |
| `timestamp` | String (SK) | 2026-03-26T09:00:10Z |
| `temperature` | String | 7.8 |
| `door_open` | Boolean | false |
| `battery_level` | Number | 91 |
| `location` | String | Rohini-PHC |
| `district` | String | North-West-Delhi |
| `state` | String | Delhi |

---

## What's Coming Next (Phase 5)

- Breach detection: flag records where `temperature > 8.0°C` or `door_open == true`
- SNS alerts for breach events
- S3 cold storage sink
