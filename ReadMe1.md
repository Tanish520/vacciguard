# VacciGuard Streaming Pipeline

## Overview

VacciGuard is a real-time data processing pipeline built using:

- Apache Kafka (data ingestion)
- Apache Spark (stream processing)
- Redis (real-time state storage)
- Docker (containerization)

The system simulates IoT telemetry data (temperature, battery, door status) and detects anomalies such as **temperature breaches** in real time.

---

## 🧠 Architecture
Workload Generator → Kafka → Spark Streaming →
├── Parquet (processed data)
├── JSON (breach alerts)
└── Redis (real-time state)

---

## Features

- Real-time data streaming
- Temperature breach detection
- Micro-batch processing (Spark)
- Output storage in Parquet & JSON
- Real-time device status in Redis

---

## Prerequisites

Make sure you have:

- Docker & Docker Compose installed
- Python 3.8+
- Git

---

## IMPORTANT: JAR Files Setup

⚠️ Due to GitHub size limits (>100MB), JAR files are NOT included in this repository.

You must download them manually.

---

## 📥 Step 1: Download Required JARs

Download the following `.jar` files:

### 1. Spark Kafka
https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

### 2. Spark Token Provider
https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

### 3. Kafka Clients
https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

### 4. Commons Pool
https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

### 5. Hadoop AWS
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

### 6. AWS SDK Bundle (Large File)
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

---

## Step 2: Place JAR Files

In this folder:
services/stream-processor/jars/


Place ALL downloaded `.jar` files inside:


services/stream-processor/jars/
├── spark-sql-kafka-0-10_2.12-3.5.0.jar
├── spark-token-provider-kafka-0-10_2.12-3.5.0.jar
├── kafka-clients-3.4.1.jar
├── commons-pool2-2.11.1.jar
├── hadoop-aws-3.3.4.jar
└── aws-java-sdk-bundle-1.12.262.jar


---

##  Running the Project

### Step 1: Build Docker Images

```bash
docker compose build --no-cache
Step 2: Run Pipeline
bash scripts/run-phase4-local.sh
📊 Output

After execution, outputs are generated in:

data/output/
Processed Data
data/output/processed/
Format: .parquet
Contains clean structured data
Breach Alerts
data/output/breach_windows/
Format: .json
Contains temperature breach events
Redis (Real-time State)

Stores latest device status:

{
  "device_id": "FR-0102",
  "breach_status": "safe"
}

From sample run:

Total events: 300
Throughput: 5 events/sec (~300/min)
Average latency: 4–6 seconds
Breach events: 34
Invalid records: 0