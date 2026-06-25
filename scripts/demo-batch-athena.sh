#!/usr/bin/env bash
# VacciGuard – Batch analytics demo via Athena
# Reads the three summary.parquet files from S3 and runs demo queries.
#
# Usage:
#   bash scripts/demo-batch-athena.sh
#
# Required overrides (set as env vars or edit defaults below):
#   S3_BUCKET                bucket that holds batch output
#   COMPLIANCE_S3_PREFIX     s3 prefix (no trailing slash) for compliance output
#   DEVICE_COMPLIANCE_S3_PREFIX
#   AUDIT_S3_PREFIX
#   ATHENA_RESULTS_S3        where Athena writes query result CSVs
#
# Example (one-liner override):
#   S3_BUCKET=my-bucket \
#   COMPLIANCE_S3_PREFIX=s3://my-bucket/batch/2026-04-19/compliance \
#   ... bash scripts/demo-batch-athena.sh

set -euo pipefail

# ── configuration ─────────────────────────────────────────────────────────────
S3_BUCKET="${S3_BUCKET:-vacciguard-tanish-baseline-ap-south-1-data}"
REGION="${AWS_DEFAULT_REGION:-ap-south-1}"

# Prefix where each summary.parquet lives (the folder, not the file).
# Override if your batch job wrote to a different prefix.
BATCH_ROOT="${BATCH_ROOT:-s3://${S3_BUCKET}/batch/latest}"

COMPLIANCE_S3_PREFIX="${COMPLIANCE_S3_PREFIX:-${BATCH_ROOT}/compliance}"
DEVICE_COMPLIANCE_S3_PREFIX="${DEVICE_COMPLIANCE_S3_PREFIX:-${BATCH_ROOT}/device-compliance}"
AUDIT_S3_PREFIX="${AUDIT_S3_PREFIX:-${BATCH_ROOT}/audit}"

ATHENA_RESULTS_S3="${ATHENA_RESULTS_S3:-s3://${S3_BUCKET}/athena-results}"
ATHENA_DB="${ATHENA_DB:-vacciguard_batch}"
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"

# ── colours ───────────────────────────────────────────────────────────────────
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

# ── athena helpers ────────────────────────────────────────────────────────────

# run_athena_query <label> <sql>
# Executes the query, polls until done, prints results as a compact table.
run_athena_query() {
  local label="$1"
  local sql="$2"

  echo ""
  echo -e "${BOLD}${YELLOW}▶  ${label}${RESET}"
  separator

  # Start execution
  local exec_id
  exec_id=$(aws athena start-query-execution \
    --region "$REGION" \
    --query-string "$sql" \
    --query-execution-context "Database=${ATHENA_DB}" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS_S3}" \
    --work-group "$ATHENA_WORKGROUP" \
    --output text \
    --query 'QueryExecutionId')

  info "Execution ID: ${exec_id}"

  # Poll for completion
  local state="RUNNING"
  local attempts=0
  while [[ "$state" == "RUNNING" || "$state" == "QUEUED" ]]; do
    sleep 2
    state=$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$exec_id" \
      --output text \
      --query 'QueryExecution.Status.State')
    attempts=$((attempts + 1))
    if (( attempts > 60 )); then
      die "Query timed out after 120s (exec_id=${exec_id})"
    fi
  done

  if [[ "$state" != "SUCCEEDED" ]]; then
    local reason
    reason=$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$exec_id" \
      --output text \
      --query 'QueryExecution.Status.StateChangeReason' 2>/dev/null || echo "unknown")
    die "Query FAILED [${state}]: ${reason}"
  fi

  # Fetch and print results
  local raw
  raw=$(aws athena get-query-results \
    --region "$REGION" \
    --query-execution-id "$exec_id" \
    --output json)

  # Use Python for clean tabular formatting (already required by the project)
  python3 - "$raw" <<'PYEOF'
import sys, json

raw = sys.argv[1]
data = json.loads(raw)
rows = data["ResultSet"]["Rows"]
if not rows:
    print("  (no rows returned)")
    sys.exit(0)

# First row is column headers
headers = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
body    = [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows[1:]]

# Compute column widths
widths = [len(h) for h in headers]
for row in body:
    for i, cell in enumerate(row):
        widths[i] = max(widths[i], len(cell))

fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
sep = "  " + "  ".join("-" * w for w in widths)

print(fmt.format(*headers))
print(sep)
for row in body:
    # Pad row in case fewer columns
    padded = row + [""] * (len(headers) - len(row))
    print(fmt.format(*padded))
PYEOF

  success "Done."
}

# run_ddl <sql>  – executes DDL silently, returns when complete
run_ddl() {
  local sql="$1"
  local exec_id
  exec_id=$(aws athena start-query-execution \
    --region "$REGION" \
    --query-string "$sql" \
    --query-execution-context "Database=${ATHENA_DB}" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS_S3}" \
    --work-group "$ATHENA_WORKGROUP" \
    --output text \
    --query 'QueryExecutionId' 2>/dev/null || true)

  [[ -z "$exec_id" ]] && return  # DDL at db-creation level has no DB context

  local state="RUNNING"
  local attempts=0
  while [[ "$state" == "RUNNING" || "$state" == "QUEUED" ]]; do
    sleep 2
    state=$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$exec_id" \
      --output text \
      --query 'QueryExecution.Status.State')
    attempts=$((attempts + 1))
    (( attempts > 30 )) && die "DDL timed out"
  done

  [[ "$state" != "SUCCEEDED" ]] && die "DDL failed (state=${state}, id=${exec_id})"
}

# run_ddl_no_db <sql>  – DDL without a database context (for CREATE DATABASE)
run_ddl_no_db() {
  local sql="$1"
  local exec_id
  exec_id=$(aws athena start-query-execution \
    --region "$REGION" \
    --query-string "$sql" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS_S3}" \
    --work-group "$ATHENA_WORKGROUP" \
    --output text \
    --query 'QueryExecutionId')

  local state="RUNNING"
  local attempts=0
  while [[ "$state" == "RUNNING" || "$state" == "QUEUED" ]]; do
    sleep 2
    state=$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$exec_id" \
      --output text \
      --query 'QueryExecution.Status.State')
    attempts=$((attempts + 1))
    (( attempts > 30 )) && die "DDL timed out"
  done

  [[ "$state" != "SUCCEEDED" ]] && die "DDL failed (state=${state}, id=${exec_id})"
}

# ── pre-flight checks ─────────────────────────────────────────────────────────

banner "VacciGuard – Batch Analytics Demo (Athena)"

info "Checking prerequisites..."

command -v aws  >/dev/null 2>&1 || die "aws CLI not found – install it first."
command -v python3 >/dev/null 2>&1 || die "python3 not found."

aws sts get-caller-identity --region "$REGION" --output text --query 'Account' >/dev/null \
  || die "AWS credentials not configured or expired. Run 'aws sso login' or set env vars."

success "AWS credentials OK."

# ── confirm data is in S3 ─────────────────────────────────────────────────────

banner "Step 1 – Verify S3 data"

info "Looking for batch output files under ${BATCH_ROOT}/"
echo ""
echo -e "  ${BOLD}Compliance:${RESET}        ${COMPLIANCE_S3_PREFIX}/summary.parquet"
echo -e "  ${BOLD}Device compliance:${RESET} ${DEVICE_COMPLIANCE_S3_PREFIX}/summary.parquet"
echo -e "  ${BOLD}Audit:${RESET}             ${AUDIT_S3_PREFIX}/summary.parquet"
echo ""

missing=0
for prefix in "$COMPLIANCE_S3_PREFIX" "$DEVICE_COMPLIANCE_S3_PREFIX" "$AUDIT_S3_PREFIX"; do
  key="${prefix#s3://${S3_BUCKET}/}/summary.parquet"
  if aws s3api head-object \
       --bucket "$S3_BUCKET" \
       --key "$key" \
       --region "$REGION" \
       --output text >/dev/null 2>&1; then
    success "Found: ${prefix}/summary.parquet"
  else
    warn "MISSING: ${prefix}/summary.parquet"
    missing=$((missing + 1))
  fi
done

if (( missing > 0 )); then
  echo ""
  echo -e "${YELLOW}  ${missing} file(s) not found. Run the batch job first:${RESET}"
  echo ""
  echo -e "  ${BOLD}python3 services/batch-analytics/job.py \\${RESET}"
  echo -e "    --processed-input      s3://${S3_BUCKET}/output/processed/ \\"
  echo -e "    --invalid-input        s3://${S3_BUCKET}/output/invalid/ \\"
  echo -e "    --breach-windows-input s3://${S3_BUCKET}/output/breach_windows/ \\"
  echo -e "    --compliance-output           ${COMPLIANCE_S3_PREFIX} \\"
  echo -e "    --device-compliance-output    ${DEVICE_COMPLIANCE_S3_PREFIX} \\"
  echo -e "    --audit-output                ${AUDIT_S3_PREFIX}"
  echo ""
  die "Aborting – run the batch job first, then re-run this demo."
fi

# ── create athena database + tables ──────────────────────────────────────────

banner "Step 2 – Create Athena database & tables"

info "Creating database '${ATHENA_DB}' (if not exists)..."
run_ddl_no_db "CREATE DATABASE IF NOT EXISTS ${ATHENA_DB}"
success "Database ready."

info "Creating table: facility_compliance..."
run_ddl "
CREATE EXTERNAL TABLE IF NOT EXISTS facility_compliance (
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
LOCATION '${COMPLIANCE_S3_PREFIX}/'
TBLPROPERTIES ('parquet.compress'='SNAPPY')
"
success "facility_compliance ready."

info "Creating table: device_compliance..."
run_ddl "
CREATE EXTERNAL TABLE IF NOT EXISTS device_compliance (
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
LOCATION '${DEVICE_COMPLIANCE_S3_PREFIX}/'
TBLPROPERTIES ('parquet.compress'='SNAPPY')
"
success "device_compliance ready."

info "Creating table: audit_summary..."
run_ddl "
CREATE EXTERNAL TABLE IF NOT EXISTS audit_summary (
  event_date                    STRING,
  invalid_events_total          BIGINT,
  invalid_unknown_device        BIGINT,
  invalid_corrupt_payload       BIGINT,
  invalid_missing_fields        BIGINT,
  breach_window_count           BIGINT,
  facilities_with_breaches      BIGINT,
  devices_with_repeated_breaches BIGINT
)
STORED AS PARQUET
LOCATION '${AUDIT_S3_PREFIX}/'
TBLPROPERTIES ('parquet.compress'='SNAPPY')
"
success "audit_summary ready."

# ── demo queries ──────────────────────────────────────────────────────────────

banner "Step 3 – Demo Queries"

# Q1: facility compliance overview, sorted by breach rate descending
run_athena_query \
  "Q1: Facility compliance overview (breach rate, highest first)" \
  "SELECT
     event_date,
     facility_id,
     facility_name,
     total_processed_events,
     breach_events,
     safe_events,
     breach_rate_pct,
     unique_devices_seen
   FROM facility_compliance
   ORDER BY breach_rate_pct DESC, total_processed_events DESC
   LIMIT 20"

# Q2: top 15 devices by breach count
run_athena_query \
  "Q2: Top 15 devices with most temperature breaches" \
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
   FROM device_compliance
   ORDER BY breach_events DESC, breach_rate_pct DESC
   LIMIT 15"

# Q3: audit health – invalid event breakdown
run_athena_query \
  "Q3: Audit summary – invalid event breakdown" \
  "SELECT
     event_date,
     invalid_events_total,
     invalid_unknown_device,
     invalid_corrupt_payload,
     invalid_missing_fields,
     breach_window_count,
     facilities_with_breaches,
     devices_with_repeated_breaches
   FROM audit_summary
   ORDER BY event_date DESC"

# Q4: facilities with breach rate above threshold
run_athena_query \
  "Q4: Facilities needing urgent attention (breach_rate_pct > 10%)" \
  "SELECT
     facility_id,
     facility_name,
     breach_rate_pct,
     breach_events,
     total_processed_events,
     ROUND(avg_temperature_c, 2) AS avg_temp_c,
     ROUND(max_temperature_c, 2) AS max_temp_c,
     unique_devices_seen
   FROM facility_compliance
   WHERE breach_rate_pct > 10
   ORDER BY breach_rate_pct DESC"

# ── done ──────────────────────────────────────────────────────────────────────

banner "Demo Complete"

echo -e "  ${BOLD}Athena database:${RESET}  ${ATHENA_DB}"
echo -e "  ${BOLD}Tables:${RESET}           facility_compliance, device_compliance, audit_summary"
echo -e "  ${BOLD}Query results:${RESET}    ${ATHENA_RESULTS_S3}"
echo ""
echo -e "  Run ad-hoc queries in the AWS Console → Athena → Query Editor"
echo -e "  and select database: ${BOLD}${ATHENA_DB}${RESET}"
echo ""
