#!/bin/bash
# setup.sh — One-time project setup for VacciGuard
# Run this once after cloning the repo.
# Works on: Mac, Linux, Windows (Git Bash or WSL)

set -e  # stop on first error

echo "[setup] VacciGuard one-time setup starting..."

# ── 1. Check Docker is installed ───────────────────────────────────────────
if ! command -v docker &> /dev/null; then
    echo "[setup] ERROR: Docker is not installed."
    echo "        Download Docker Desktop from https://www.docker.com/products/docker-desktop"
    exit 1
fi
echo "[setup] Docker found."

# ── 2. Check AWS CLI is installed ──────────────────────────────────────────
if ! command -v aws &> /dev/null; then
    echo "[setup] ERROR: AWS CLI is not installed."
    echo "        Download from https://aws.amazon.com/cli/"
    exit 1
fi
echo "[setup] AWS CLI found."

# ── 3. Check AWS credentials are configured ────────────────────────────────
if ! aws sts get-caller-identity &> /dev/null; then
    echo "[setup] ERROR: AWS credentials not configured."
    echo "        Run: aws configure"
    exit 1
fi
echo "[setup] AWS credentials verified."

# ── 4. Download the Kinesis connector JAR (if not already present) ─────────
JAR_DIR="lib"
JAR_NAME="flink-sql-connector-kinesis-4.3.0-1.18.jar"
JAR_PATH="$JAR_DIR/$JAR_NAME"
JAR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kinesis/4.3.0-1.18/$JAR_NAME"

mkdir -p "$JAR_DIR"

if [ -f "$JAR_PATH" ]; then
    echo "[setup] Kinesis JAR already present — skipping download."
else
    echo "[setup] Downloading Kinesis connector JAR (~63MB)..."
    curl -fL "$JAR_URL" -o "$JAR_PATH"
    echo "[setup] JAR downloaded."
fi

# ── 5. Build Docker images ─────────────────────────────────────────────────
echo "[setup] Building Docker images (this takes ~5 minutes on first run)..."
docker compose build
echo "[setup] Docker images built."

echo ""
echo "[setup] Setup complete. You are ready to run VacciGuard."
echo ""
echo "  Start pipeline : docker compose up flink-pipeline"
echo "  Send test data : docker compose run --rm simulator"
echo ""
