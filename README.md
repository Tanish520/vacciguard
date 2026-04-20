# VacciGuard

A production-style cloud data pipeline for vaccine cold-chain monitoring, built on AWS. VacciGuard ingests live IoT telemetry from refrigeration devices, detects temperature breaches in real time, archives events for batch compliance reporting, and includes a full evaluation framework that compares a baseline pipeline against an optimized design across latency, throughput, spike resilience, and cost.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Evaluation Results](#evaluation-results)
- [Repository Structure](#repository-structure)
- [Quick Start — Local Dev](#quick-start--local-dev)
- [AWS Deployment](#aws-deployment)
- [Batch Analytics](#batch-analytics)
- [Running the Evaluation](#running-the-evaluation)
- [Success Criteria](#success-criteria)

---

## Overview

Vaccine storage requires continuous temperature monitoring. A single undetected breach can compromise entire batches. VacciGuard addresses this with a hybrid streaming and batch pipeline:

- **Hot path** — Kafka → Spark Structured Streaming → Redis. Breach alerts land in Redis within seconds of the sensor event.
- **Cold path** — Spark archives enriched Parquet files to Amazon S3 for durable long-term storage.
- **Batch path** — A scheduled Pandas/PyArrow job aggregates the cold-path archives into daily facility and device compliance summaries, queryable via Amazon Athena.

Two pipeline configurations were built and benchmarked: a **baseline** (single fixed-resource stream processor) and an **optimized** design (split hot/cold resource allocation with tuned batch intervals).

---

## Architecture

```
IoT Devices (120 devices across 20 facilities)
    │
    ▼
Apache Kafka  (KRaft mode, no Zookeeper)
    │
    ├──► Spark Structured Streaming ──► Redis          (hot path: breach detection, live device status)
    │            │
    │            └──► Amazon S3 / Parquet              (cold path: enriched event archive)
    │
    └──► Batch Analytics  (Airflow + Pandas + PyArrow)
                 │
                 └──► Amazon S3 Parquet summaries      (daily compliance + audit reports → Athena)
```

**Monitoring:** Prometheus + Grafana for in-cluster runtime metrics. AWS CloudWatch + Container Insights for infrastructure-level telemetry.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka (KRaft mode) |
| Stream processing | Apache Spark Structured Streaming |
| Hot store | Redis |
| Cold store | Amazon S3 (Snappy Parquet) |
| Batch orchestration | Apache Airflow |
| Batch processing | Pandas + PyArrow |
| Query layer | Amazon Athena |
| Infrastructure | Terraform + Kubernetes (EKS, `ap-south-1`) |
| Monitoring | Prometheus, Grafana, AWS CloudWatch |
| Local dev | Docker Compose |

---

## Evaluation Results

All scenarios ran against a live AWS EKS cluster at 100 eps (normal) and 1,000 eps (spike). The 5-second end-to-end latency SLA and 2-minute failure recovery target are the key benchmarks.

### Baseline Pipeline

| Scenario | Avg Latency | P95 Latency | Throughput | Cost/Run | Cost/GB |
|---|---|---|---|---|---|
| Normal load (100 eps) | 2.60 s ✅ | 3.11 s ✅ | 100 eps | ~$0.017 | ~$2.58/GB |
| 10× spike (1,000 eps) | 225.76 s ❌ | 225.77 s ❌ | 999.9 eps | ~$0.017 | ~$0.25/GB |
| Failure recovery | 3.18 s ✅ | 3.51 s ✅ | 100 eps | ~$0.033 | ~$2.51/GB |

> The baseline meets the SLA under normal load and recovers from failure. However, it cannot absorb a 10× traffic spike — latency blows out to 225 seconds as the fixed-resource processor falls behind.

### Optimized Pipeline

| Scenario | Avg Latency | P95 Latency | Throughput | Cost/Run | Cost/GB |
|---|---|---|---|---|---|
| Normal load (100 eps) | 14.19 s | 35.88 s | 100 eps | ~$0.017 | ~$2.58/GB |
| 10× spike (1,000 eps) | 22.94 s ✅ | 33.76 s ✅ | 999.9 eps | ~$0.017 | ~$0.25/GB |

> The optimized pipeline absorbs the 10× spike with stable latency, at the same cost per run. The trade-off: higher latency under normal load due to split resource allocation and longer batch intervals.

Full per-run reports are in [`Evaluation Result/`](Evaluation%20Result/).

---

## Repository Structure

```
vacciguard/
├── services/
│   ├── stream-processor/        # Spark Structured Streaming job (hot + cold path)
│   ├── batch-analytics/         # Daily compliance summary builders (Pandas + PyArrow)
│   ├── replay-producer/         # Kafka event replay for load tests and local dev
│   ├── batch-processor/         # Supporting batch utilities
│   └── evaluation-controller/   # Automated baseline vs. optimized evaluation runner
│
├── infra/
│   ├── terraform/               # AWS resource definitions (EKS, S3, IAM, VPC)
│   ├── kubernetes/              # Kustomize overlays: base, baseline, optimized
│   └── monitoring/              # Prometheus, Grafana, and CloudWatch configs
│
├── orchestration/
│   └── airflow/dags/            # Batch analytics Airflow DAG
│
├── data/
│   ├── reference/               # Device-to-facility lookup (120 devices, 20 facilities)
│   ├── workloads/               # Generated replay workloads
│   └── batch/                   # Daily operations log CSVs
│
├── Evaluation Result/
│   ├── Baseline Pipeline/       # Per-scenario metric reports (normal, spike, failure-recovery)
│   └── Optimized Pipeline/      # Per-scenario metric reports
│
├── tests/                       # Smoke, evaluation, monitoring, and unit tests
├── scripts/                     # Dev, demo, and evaluation helper scripts
├── docs/                        # Architecture and deployment documentation
└── docker-compose.yml           # Local dev stack (Kafka, Redis, stream processor)
```

---

## Quick Start — Local Dev

### Prerequisites

- Python 3.9+
- Docker and Docker Compose
- `pip install -r requirements-dev.txt`

### Run the full local pipeline

**1. Generate the dev workload** (300 events across 3 devices at 5 eps):

```bash
python3 scripts/generate-dev-workload.py
```

**2. Start Kafka, Redis, and the stream processor:**

```bash
docker compose up -d kafka redis stream-processor
```

**3. Replay the workload:**

```bash
docker compose run --rm replay-producer
```

**4. Inspect hot-path state in Redis:**

```bash
docker compose exec redis redis-cli GET device:status:FR-0102
docker compose exec redis redis-cli ZRANGE active_breaches 0 -1 WITHSCORES
```

**5. Inspect cold-path output:**

```bash
find data/output/processed      -name '*.parquet' | sort
find data/output/invalid        -name '*.json'    | sort
find data/output/breach_windows -name '*.json'    | sort
```

**6. Run smoke verification:**

```bash
python3 tests/smoke/verify_phase4.py
```

Or run everything in one command:

```bash
bash scripts/run-phase4-local.sh
```

---

## AWS Deployment

### Validate infrastructure configs

```bash
terraform -chdir=infra/terraform fmt -check

kubectl kustomize infra/kubernetes/base      > /tmp/vacciguard-base.yaml
kubectl kustomize infra/kubernetes/baseline  > /tmp/vacciguard-baseline.yaml
kubectl kustomize infra/kubernetes/optimized > /tmp/vacciguard-optimized.yaml
```

### Deploy monitoring

```bash
kubectl apply -k infra/monitoring/prometheus
kubectl apply -k infra/monitoring/grafana

# Access dashboards locally
kubectl port-forward -n monitoring svc/prometheus 9090:9090
kubectl port-forward -n monitoring svc/grafana    3000:3000
```

See [docs/aws-baseline-foundation.md](docs/aws-baseline-foundation.md) for the full baseline deployment structure.

---

## Batch Analytics

After the stream processor archives events to S3, the batch job aggregates them into three daily Parquet summaries:

| Table | Description |
|---|---|
| `facility_compliance` | Facility-level daily breach rate, temperature range, device count |
| `device_compliance` | Device-level daily breach rate, temperature stats, district/state |
| `audit_summary` | Invalid event breakdown, breach windows, repeated-breach devices |

### Run the batch job

```bash
python3 services/batch-analytics/job.py \
  --processed-input      s3://YOUR_BUCKET/processed/ \
  --invalid-input        s3://YOUR_BUCKET/invalid/ \
  --breach-windows-input s3://YOUR_BUCKET/breach_windows/ \
  --compliance-output           s3://YOUR_BUCKET/batch/latest/compliance \
  --device-compliance-output    s3://YOUR_BUCKET/batch/latest/device-compliance \
  --audit-output                s3://YOUR_BUCKET/batch/latest/audit
```

### Query results with Athena

Run the demo script to auto-create Athena tables and execute pre-built compliance queries:

```bash
bash scripts/demo-batch-athena.sh
```

This creates the `vacciguard_batch` Athena database with all three tables and runs four demo queries — breach rates by facility, top breaching devices, audit health, and facilities needing urgent attention.

The Airflow DAG at [`orchestration/airflow/dags/vacciguard_batch_analytics_dag.py`](orchestration/airflow/dags/vacciguard_batch_analytics_dag.py) orchestrates scheduled production runs.

---

## Running the Evaluation

```bash
# Baseline evaluation (normal, spike, failure-recovery scenarios)
bash scripts/run-aws-baseline-evaluation.sh

# Live demo: normal load → pod failure injection → recovery
bash scripts/demo.sh
```

The evaluation controller writes per-run JSON + Markdown reports to S3 and the `Evaluation Result/` directory. Key metrics captured: end-to-end latency (avg/p95/p99), throughput, consumer lag, pod restart count, recovery time, and cost per GB.

---


