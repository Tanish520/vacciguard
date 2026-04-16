#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUN_ID_RAW="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
SCENARIO="${2:-normal}"
RUN_ID="$(printf '%s' "$RUN_ID_RAW" | tr '[:upper:]' '[:lower:]')"
REPORT_DIR="$ROOT_DIR/artifacts/aws-baseline-evaluations"
REPORT_MD_PATH="$ROOT_DIR/artifacts/aws-baseline-evaluations/${RUN_ID}.md"
REPORT_JSON_PATH="$ROOT_DIR/artifacts/aws-baseline-evaluations/${RUN_ID}.json"
export S3_BUCKET_NAME="${S3_BUCKET_NAME:-vacciguard-tanish-baseline-ap-south-1-data}"
export WORKLOAD_FAMILY_VERSION="${WORKLOAD_FAMILY_VERSION:-evaluation-workload-v1}"
# WORKLOAD_DURATION_MINUTES is forwarded through the inherited environment.

mkdir -p "$REPORT_DIR"

eval "$(aws configure export-credentials --format env)"

bash scripts/run-aws-evaluation-controller.sh baseline "$SCENARIO" "$RUN_ID"

aws s3 cp \
  "s3://${S3_BUCKET_NAME}/evaluations/baseline/${SCENARIO}/${RUN_ID}/report.md" \
  "$REPORT_MD_PATH" \
  >/dev/null

aws s3 cp \
  "s3://${S3_BUCKET_NAME}/evaluations/baseline/${SCENARIO}/${RUN_ID}/report.json" \
  "$REPORT_JSON_PATH" \
  >/dev/null

echo "Run ID: ${RUN_ID}"
echo "Scenario: ${SCENARIO}"
echo "Workload family version: ${WORKLOAD_FAMILY_VERSION}"
echo "S3 prefix: s3://${S3_BUCKET_NAME}/evaluations/baseline/${SCENARIO}/${RUN_ID}/"
echo "Report: ${REPORT_MD_PATH}"
