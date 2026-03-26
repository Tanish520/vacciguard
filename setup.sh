#!/bin/bash
# setup.sh - One-time local setup for VacciGuard
# Works on: Mac, Linux, Windows (Git Bash or WSL)

set -e

JAR_DIR="lib"
JAR_NAME="flink-sql-connector-kinesis-4.3.0-1.18.jar"
JAR_PATH="$JAR_DIR/$JAR_NAME"
JAR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kinesis/4.3.0-1.18/$JAR_NAME"


echo "[setup] VacciGuard local setup starting..."

echo "[setup] Checking Docker..."
if ! command -v docker >/dev/null 2>&1; then
    echo "[setup] ERROR: Docker is not installed."
    echo "        Download Docker Desktop from https://www.docker.com/products/docker-desktop"
    exit 1
fi

echo "[setup] Checking AWS CLI..."
if ! command -v aws >/dev/null 2>&1; then
    echo "[setup] ERROR: AWS CLI is not installed."
    echo "        Download from https://aws.amazon.com/cli/"
    exit 1
fi

echo "[setup] Verifying local AWS credentials..."
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    echo "[setup] ERROR: AWS credentials are not configured for local development."
    echo "        Run: aws configure"
    exit 1
fi

mkdir -p "$JAR_DIR"
if [ -f "$JAR_PATH" ]; then
    echo "[setup] Kinesis connector JAR already present - skipping download."
else
    echo "[setup] Downloading Kinesis connector JAR (~63MB)..."
    curl -fL "$JAR_URL" -o "$JAR_PATH"
    echo "[setup] JAR downloaded."
fi

echo "[setup] Building local Docker images (linux/amd64 for PyFlink wheel compatibility)..."
docker compose build

echo ""
echo "[setup] Local setup complete."
echo ""
echo "  Start pipeline : docker compose up flink-pipeline"
echo "  Send test data : docker compose run --rm simulator"
echo ""
echo "[setup] Optional Phase 5 env vars:"
echo "  export VACCIGUARD_SNS_ALERT_TOPIC_ARN='arn:aws:sns:ap-south-1:<account-id>:vacciguard-alerts.fifo'"
echo "  export VACCIGUARD_ENABLE_ALERTS=true"
echo ""
echo "[setup] For AWS deployment later, use IAM roles instead of mounting ~/.aws into the container."
