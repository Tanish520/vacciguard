#!/usr/bin/env bash
# VacciGuard batch-processing demo:
# 1. Download one archived optimized run from S3 to local disk
# 2. Run the batch analytics job locally against the staged inputs
# 3. Upload the generated summaries back to S3
# 4. Create Athena external tables over the uploaded Parquet outputs
# 5. Run a few demo queries and print the results inline
#
# Usage:
#   bash scripts/run-batch-demo.sh
#
# Optional overrides:
#   AWS_DEFAULT_REGION=ap-south-1
#   S3_BUCKET=vacciguard-tanish-baseline-ap-south-1-data
#   SOURCE_RUN_ROOT=s3://.../evaluations/optimized/normal/opt-normal-3-20260417t184552z
#   DEMO_RUN_ID=batch-demo-20260419t120000z
#   ATHENA_DB=vacciguard_batch_demo
#   ATHENA_WORKGROUP=primary

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REGION="${AWS_DEFAULT_REGION:-ap-south-1}"
S3_BUCKET="${S3_BUCKET:-vacciguard-tanish-baseline-ap-south-1-data}"
SOURCE_RUN_ROOT="${SOURCE_RUN_ROOT:-s3://${S3_BUCKET}/evaluations/optimized/normal/opt-normal-3-20260417t184552z}"
DEMO_RUN_ID="${DEMO_RUN_ID:-batch-demo-$(date -u +%Y%m%dt%H%M%Sz | tr '[:upper:]' '[:lower:]')}"

LOCAL_ROOT="${LOCAL_ROOT:-${ROOT_DIR}/artifacts/batch-demo/${DEMO_RUN_ID}}"
LOCAL_INPUT_ROOT="${LOCAL_ROOT}/input"
LOCAL_OUTPUT_ROOT="${LOCAL_ROOT}/output"

PROCESSED_INPUT_S3="${PROCESSED_INPUT_S3:-${SOURCE_RUN_ROOT}/processed/}"
INVALID_INPUT_S3="${INVALID_INPUT_S3:-${SOURCE_RUN_ROOT}/invalid/}"
BREACH_WINDOWS_INPUT_S3="${BREACH_WINDOWS_INPUT_S3:-${SOURCE_RUN_ROOT}/breach_windows/}"

OUTPUT_ROOT_S3="${OUTPUT_ROOT_S3:-s3://${S3_BUCKET}/batch-analytics/demo/${DEMO_RUN_ID}}"
COMPLIANCE_OUTPUT_S3="${COMPLIANCE_OUTPUT_S3:-${OUTPUT_ROOT_S3}/daily_compliance_summary}"
DEVICE_COMPLIANCE_OUTPUT_S3="${DEVICE_COMPLIANCE_OUTPUT_S3:-${OUTPUT_ROOT_S3}/daily_device_compliance_summary}"
AUDIT_OUTPUT_S3="${AUDIT_OUTPUT_S3:-${OUTPUT_ROOT_S3}/daily_audit_summary}"

ATHENA_DB="${ATHENA_DB:-vacciguard_batch_demo}"
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"
ATHENA_RESULTS_S3="${ATHENA_RESULTS_S3:-s3://${S3_BUCKET}/athena-results/batch-demo/${DEMO_RUN_ID}/}"

FACILITY_TABLE="${FACILITY_TABLE:-facility_compliance_demo}"
DEVICE_TABLE="${DEVICE_TABLE:-device_compliance_demo}"
AUDIT_TABLE="${AUDIT_TABLE:-audit_summary_demo}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[batch-demo]${RESET} $*"; }
success() { echo -e "${GREEN}[batch-demo]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[batch-demo]${RESET} $*"; }
die()     { echo -e "${RED}[batch-demo] ERROR:${RESET} $*" >&2; exit 1; }

banner() {
  echo ""
  echo -e "${BOLD}${GREEN}══════════════════════════════════════════════════════${RESET}"
  echo -e "${BOLD}${GREEN}  $1${RESET}"
  echo -e "${BOLD}${GREEN}══════════════════════════════════════════════════════${RESET}"
  echo ""
}

separator() { echo -e "${CYAN}──────────────────────────────────────────────────────${RESET}"; }

ensure_commands() {
  command -v aws >/dev/null 2>&1 || die "aws CLI not found."
  command -v python3 >/dev/null 2>&1 || die "python3 not found."
}

check_aws() {
  aws sts get-caller-identity --region "$REGION" --output text --query 'Account' >/dev/null \
    || die "AWS credentials are missing or expired. Run 'aws sso login' first."
}

verify_s3_prefix() {
  local prefix="$1"
  aws s3 ls "$prefix" --region "$REGION" >/dev/null \
    || die "Cannot access S3 prefix: ${prefix}"
}

prepare_local_dirs() {
  rm -rf "$LOCAL_ROOT"
  mkdir -p \
    "${LOCAL_INPUT_ROOT}/processed" \
    "${LOCAL_INPUT_ROOT}/invalid" \
    "${LOCAL_INPUT_ROOT}/breach_windows" \
    "${LOCAL_OUTPUT_ROOT}"
}

copy_s3_prefix_to_local() {
  local source_prefix="$1"
  local destination_dir="$2"
  info "Downloading ${source_prefix}"
  aws s3 cp "$source_prefix" "$destination_dir/" --recursive --region "$REGION" >/dev/null
}

upload_local_summary() {
  local local_dir="$1"
  local target_prefix="$2"
  local parquet_path="${local_dir}/summary.parquet"

  [[ -f "$parquet_path" ]] || die "Expected output not found: ${parquet_path}"

  info "Uploading ${parquet_path} -> ${target_prefix}/summary.parquet"
  aws s3 cp "$parquet_path" "${target_prefix}/summary.parquet" --region "$REGION" >/dev/null
}

wait_for_athena_query() {
  local exec_id="$1"
  local label="$2"
  local state="RUNNING"
  local attempts=0

  while [[ "$state" == "RUNNING" || "$state" == "QUEUED" ]]; do
    sleep 2
    state="$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$exec_id" \
      --output text \
      --query 'QueryExecution.Status.State')"
    attempts=$((attempts + 1))
    if (( attempts > 90 )); then
      die "${label} timed out after 180s (exec_id=${exec_id})"
    fi
  done

  if [[ "$state" != "SUCCEEDED" ]]; then
    local reason
    reason="$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$exec_id" \
      --output text \
      --query 'QueryExecution.Status.StateChangeReason' 2>/dev/null || echo "unknown")"
    die "${label} failed [${state}]: ${reason}"
  fi
}

run_ddl_no_db() {
  local sql="$1"
  local exec_id

  exec_id="$(aws athena start-query-execution \
    --region "$REGION" \
    --query-string "$sql" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS_S3}" \
    --work-group "$ATHENA_WORKGROUP" \
    --output text \
    --query 'QueryExecutionId')"

  wait_for_athena_query "$exec_id" "DDL"
}

run_ddl() {
  local sql="$1"
  local exec_id

  exec_id="$(aws athena start-query-execution \
    --region "$REGION" \
    --query-string "$sql" \
    --query-execution-context "Database=${ATHENA_DB}" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS_S3}" \
    --work-group "$ATHENA_WORKGROUP" \
    --output text \
    --query 'QueryExecutionId')"

  wait_for_athena_query "$exec_id" "DDL"
}

run_athena_query() {
  local label="$1"
  local sql="$2"
  local exec_id
  local raw

  echo ""
  echo -e "${BOLD}${YELLOW}▶  ${label}${RESET}"
  separator

  exec_id="$(aws athena start-query-execution \
    --region "$REGION" \
    --query-string "$sql" \
    --query-execution-context "Database=${ATHENA_DB}" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS_S3}" \
    --work-group "$ATHENA_WORKGROUP" \
    --output text \
    --query 'QueryExecutionId')"

  info "Execution ID: ${exec_id}"
  wait_for_athena_query "$exec_id" "$label"

  raw="$(aws athena get-query-results \
    --region "$REGION" \
    --query-execution-id "$exec_id" \
    --output json)"

  python3 - "$raw" <<'PYEOF'
import json
import sys

rows = json.loads(sys.argv[1])["ResultSet"]["Rows"]
if not rows:
    print("  (no rows returned)")
    raise SystemExit(0)

headers = [cell.get("VarCharValue", "") for cell in rows[0]["Data"]]
body = [[cell.get("VarCharValue", "") for cell in row["Data"]] for row in rows[1:]]

widths = [len(header) for header in headers]
for row in body:
    for idx, cell in enumerate(row):
        widths[idx] = max(widths[idx], len(cell))

fmt = "  " + "  ".join(f"{{:<{width}}}" for width in widths)
sep = "  " + "  ".join("-" * width for width in widths)

print(fmt.format(*headers))
print(sep)
for row in body:
    padded = row + [""] * (len(headers) - len(row))
    print(fmt.format(*padded))
PYEOF

  success "Done."
}

banner "VacciGuard Batch Demo"

ensure_commands
check_aws

banner "Step 1 – Verify archived inputs"
info "Source run root: ${SOURCE_RUN_ROOT}"
verify_s3_prefix "$PROCESSED_INPUT_S3"
verify_s3_prefix "$INVALID_INPUT_S3"
verify_s3_prefix "$BREACH_WINDOWS_INPUT_S3"
success "Archived input prefixes are reachable."

banner "Step 2 – Stage inputs locally"
prepare_local_dirs
copy_s3_prefix_to_local "$PROCESSED_INPUT_S3" "${LOCAL_INPUT_ROOT}/processed"
copy_s3_prefix_to_local "$INVALID_INPUT_S3" "${LOCAL_INPUT_ROOT}/invalid"
copy_s3_prefix_to_local "$BREACH_WINDOWS_INPUT_S3" "${LOCAL_INPUT_ROOT}/breach_windows"
success "Local staged inputs are ready under ${LOCAL_INPUT_ROOT}"

banner "Step 3 – Run batch analytics locally"
python3 services/batch-analytics/job.py \
  --processed-input "${LOCAL_INPUT_ROOT}/processed" \
  --invalid-input "${LOCAL_INPUT_ROOT}/invalid" \
  --breach-windows-input "${LOCAL_INPUT_ROOT}/breach_windows" \
  --compliance-output "${LOCAL_OUTPUT_ROOT}/daily_compliance_summary" \
  --device-compliance-output "${LOCAL_OUTPUT_ROOT}/daily_device_compliance_summary" \
  --audit-output "${LOCAL_OUTPUT_ROOT}/daily_audit_summary"
success "Batch job completed."

banner "Step 4 – Upload demo outputs to S3"
upload_local_summary "${LOCAL_OUTPUT_ROOT}/daily_compliance_summary" "$COMPLIANCE_OUTPUT_S3"
upload_local_summary "${LOCAL_OUTPUT_ROOT}/daily_device_compliance_summary" "$DEVICE_COMPLIANCE_OUTPUT_S3"
upload_local_summary "${LOCAL_OUTPUT_ROOT}/daily_audit_summary" "$AUDIT_OUTPUT_S3"
success "Demo outputs uploaded to ${OUTPUT_ROOT_S3}"

banner "Step 5 – Create Athena database & tables"
run_ddl_no_db "CREATE DATABASE IF NOT EXISTS ${ATHENA_DB}"

run_ddl "DROP TABLE IF EXISTS ${FACILITY_TABLE}"
run_ddl "
CREATE EXTERNAL TABLE ${FACILITY_TABLE} (
  event_date              STRING,
  facility_id             STRING,
  facility_name           STRING,
  total_processed_events  BIGINT,
  safe_events             BIGINT,
  breach_events           BIGINT,
  breach_rate_pct         DOUBLE,
  avg_temperature_c       DOUBLE,
  min_temperature_c       DOUBLE,
  max_temperature_c       DOUBLE,
  unique_devices_seen     BIGINT
)
STORED AS PARQUET
LOCATION '${COMPLIANCE_OUTPUT_S3}/'
TBLPROPERTIES ('parquet.compress'='SNAPPY')
"

run_ddl "DROP TABLE IF EXISTS ${DEVICE_TABLE}"
run_ddl "
CREATE EXTERNAL TABLE ${DEVICE_TABLE} (
  event_date              STRING,
  facility_id             STRING,
  facility_name           STRING,
  district                STRING,
  state                   STRING,
  storage_type            STRING,
  device_id               STRING,
  total_processed_events  BIGINT,
  safe_events             BIGINT,
  breach_events           BIGINT,
  breach_rate_pct         DOUBLE,
  avg_temperature_c       DOUBLE,
  min_temperature_c       DOUBLE,
  max_temperature_c       DOUBLE
)
STORED AS PARQUET
LOCATION '${DEVICE_COMPLIANCE_OUTPUT_S3}/'
TBLPROPERTIES ('parquet.compress'='SNAPPY')
"

run_ddl "DROP TABLE IF EXISTS ${AUDIT_TABLE}"
run_ddl "
CREATE EXTERNAL TABLE ${AUDIT_TABLE} (
  event_date                     STRING,
  invalid_events_total           BIGINT,
  invalid_unknown_device         BIGINT,
  invalid_corrupt_payload        BIGINT,
  invalid_missing_fields         BIGINT,
  breach_window_count            BIGINT,
  facilities_with_breaches       BIGINT,
  devices_with_repeated_breaches BIGINT
)
STORED AS PARQUET
LOCATION '${AUDIT_OUTPUT_S3}/'
TBLPROPERTIES ('parquet.compress'='SNAPPY')
"
success "Athena database and tables are ready."

banner "Step 6 – Demo queries"
run_athena_query \
  "Facility compliance overview" \
  "SELECT
     event_date,
     facility_id,
     facility_name,
     total_processed_events,
     breach_events,
     safe_events,
     breach_rate_pct,
     unique_devices_seen
   FROM ${FACILITY_TABLE}
   ORDER BY breach_rate_pct DESC, total_processed_events DESC
   LIMIT 20"

run_athena_query \
  "Top devices with the most temperature breaches" \
  "SELECT
     device_id,
     facility_name,
     district,
     state,
     storage_type,
     breach_events,
     total_processed_events,
     breach_rate_pct,
     ROUND(avg_temperature_c, 2) AS avg_temp_c,
     ROUND(max_temperature_c, 2) AS max_temp_c
   FROM ${DEVICE_TABLE}
   ORDER BY breach_events DESC, breach_rate_pct DESC
   LIMIT 15"

run_athena_query \
  "Audit summary" \
  "SELECT
     event_date,
     invalid_events_total,
     invalid_unknown_device,
     invalid_corrupt_payload,
     invalid_missing_fields,
     breach_window_count,
     facilities_with_breaches,
     devices_with_repeated_breaches
   FROM ${AUDIT_TABLE}
   ORDER BY event_date DESC"

banner "Demo Complete"
echo -e "  ${BOLD}Local staged inputs:${RESET}  ${LOCAL_INPUT_ROOT}"
echo -e "  ${BOLD}Local outputs:${RESET}        ${LOCAL_OUTPUT_ROOT}"
echo -e "  ${BOLD}S3 output root:${RESET}       ${OUTPUT_ROOT_S3}"
echo -e "  ${BOLD}Athena database:${RESET}     ${ATHENA_DB}"
echo -e "  ${BOLD}Athena tables:${RESET}       ${FACILITY_TABLE}, ${DEVICE_TABLE}, ${AUDIT_TABLE}"
echo -e "  ${BOLD}Athena results:${RESET}      ${ATHENA_RESULTS_S3}"
echo ""
echo -e "  Open AWS Console -> Athena -> Query editor and select database ${BOLD}${ATHENA_DB}${RESET}"
echo -e "  The tables now point at this demo run's uploaded Parquet summaries."
