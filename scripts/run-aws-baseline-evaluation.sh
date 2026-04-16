#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NAMESPACE="vacciguard"
STREAM_DEPLOYMENT="stream-processor"
REPLAY_JOB_NAME="replay-producer"
REPORT_DIR="$ROOT_DIR/artifacts/aws-baseline-evaluations"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
SCENARIO="${2:-normal}"
RUN_TOPIC="vacciguard-eval-${RUN_ID}"
RESET_REDIS_STATE="${RESET_REDIS_STATE:-true}"
WORKLOAD_FAMILY_VERSION="${WORKLOAD_FAMILY_VERSION:-evaluation-workload-v1}"
WORKLOAD_BASE_DIR="${WORKLOAD_BASE_DIR:-$ROOT_DIR/data/workloads/evaluation/v1}"
SOURCE_WORKLOAD_FILE="${WORKLOAD_BASE_DIR}/${SCENARIO}.events.ndjson"
WORKLOAD_FILE="$SOURCE_WORKLOAD_FILE"
WORKLOAD_MANIFEST="${WORKLOAD_BASE_DIR}/${SCENARIO}.manifest.json"
MAX_WORKLOAD_CONFIGMAP_BYTES="${MAX_WORKLOAD_CONFIGMAP_BYTES:-921600}"
WORKLOAD_TEMP_DIR=""

mkdir -p "$REPORT_DIR"

cleanup() {
  if [[ -n "$WORKLOAD_TEMP_DIR" && -d "$WORKLOAD_TEMP_DIR" ]]; then
    rm -rf "$WORKLOAD_TEMP_DIR"
  fi
}

trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd aws
require_cmd kubectl
require_cmd python3
require_cmd rg

json_value() {
  local json_input="$1"
  local key="$2"
  python3 - "$json_input" "$key" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
key = sys.argv[2]
print(payload[key])
PY
}

json_file_value() {
  local file_path="$1"
  local key="$2"
  python3 - "$file_path" "$key" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
print(payload[sys.argv[2]])
PY
}

json_file_optional_json() {
  local file_path="$1"
  local key="$2"
  python3 - "$file_path" "$key" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
value = payload.get(sys.argv[2])
print("" if value is None else json.dumps(value, sort_keys=True))
PY
}

cm_value() {
  local key="$1"
  kubectl get configmap vacciguard-pipeline-config -n "$NAMESPACE" -o "jsonpath={.data.${key}}"
}

latest_stream_pod() {
  kubectl get pods -n "$NAMESPACE" -l app=stream-processor --sort-by=.metadata.creationTimestamp -o name | tail -n 1 | cut -d/ -f2
}

wait_for_stream_pod_ready() {
  local pod_name=""
  for _ in $(seq 1 36); do
    pod_name="$(kubectl get pods -n "$NAMESPACE" -l app=stream-processor --field-selector=status.phase=Running --sort-by=.metadata.creationTimestamp -o name | tail -n 1 | cut -d/ -f2)"
    if [[ -n "$pod_name" ]] && kubectl wait --for=condition=Ready "pod/${pod_name}" -n "$NAMESPACE" --timeout=5s >/dev/null 2>&1; then
      printf '%s\n' "$pod_name"
      return 0
    fi
    sleep 5
  done
  return 1
}

echo "[1/10] Confirming AWS account and cluster access"
AWS_IDENTITY="$(aws sts get-caller-identity)"
ACCOUNT_ID="$(json_value "$AWS_IDENTITY" Account)"
AWS_REGION="${AWS_REGION:-ap-south-1}"
CLUSTER_NAME="${CLUSTER_NAME:-$(terraform -chdir=infra/terraform output -raw eks_cluster_name 2>/dev/null || true)}"
if [[ -z "$CLUSTER_NAME" || "${#CLUSTER_NAME}" -gt 100 ]]; then
  CLUSTER_NAME="${DEFAULT_EKS_CLUSTER_NAME:-vacciguard-tanish-baseline-eks}"
fi
aws eks update-kubeconfig --region "$AWS_REGION" --name "$CLUSTER_NAME" >/dev/null
kubectl get nodes >/dev/null

if [[ ! -f "$SOURCE_WORKLOAD_FILE" ]]; then
  echo "Missing workload file: $SOURCE_WORKLOAD_FILE" >&2
  exit 1
fi
if [[ ! -f "$WORKLOAD_MANIFEST" ]]; then
  echo "Missing workload manifest: $WORKLOAD_MANIFEST" >&2
  exit 1
fi

echo "[2/10] Preparing run-scoped workload copy"
WORKLOAD_TEMP_DIR="$(mktemp -d "/tmp/vacciguard-workload-${RUN_ID}.XXXXXX")"
WORKLOAD_FILE="${WORKLOAD_TEMP_DIR}/${SCENARIO}.events.ndjson"
python3 scripts/shift_workload_timestamps.py \
  --input "$SOURCE_WORKLOAD_FILE" \
  --output "$WORKLOAD_FILE"

echo "[3/10] Reading current baseline config"
APP_NAME="$(cm_value APP_NAME)"
KAFKA_BOOTSTRAP_SERVERS="$(cm_value KAFKA_BOOTSTRAP_SERVERS)"
KAFKA_TOPIC_PARTITIONS="$(cm_value KAFKA_TOPIC_PARTITIONS)"
TRIGGER_INTERVAL="$(cm_value TRIGGER_INTERVAL)"
WATERMARK_DELAY="$(cm_value WATERMARK_DELAY)"
REDIS_HOST="$(cm_value REDIS_HOST)"
REDIS_PORT="$(cm_value REDIS_PORT)"
REDIS_DB="$(cm_value REDIS_DB)"
PROCESSED_BASE="$(cm_value PROCESSED_OUTPUT_PATH)"
BUCKET_NAME="$(printf '%s\n' "$PROCESSED_BASE" | sed -E 's#^s3a://([^/]+)/.*#\1#')"
RUN_PREFIX="evaluations/${RUN_ID}"
PROCESSED_OUTPUT_PATH="s3a://${BUCKET_NAME}/${RUN_PREFIX}/processed"
INVALID_OUTPUT_PATH="s3a://${BUCKET_NAME}/${RUN_PREFIX}/invalid"
BREACH_WINDOW_OUTPUT_PATH="s3a://${BUCKET_NAME}/${RUN_PREFIX}/breach_windows"
CHECKPOINT_ROOT="s3a://${BUCKET_NAME}/${RUN_PREFIX}/checkpoints"
REPORT_PATH="${REPORT_DIR}/${RUN_ID}.md"
REPLAY_PRODUCER_IMAGE="${REPLAY_PRODUCER_IMAGE:-$(awk '/image:/{print $2; exit}' infra/kubernetes/base/job-replay-producer.yaml)}"
REDIS_ACTIVE_BREACHES_KEY="${REDIS_ACTIVE_BREACHES_KEY:-active_breaches}"
WORKLOAD_TARGET_EPS="${WORKLOAD_TARGET_EPS:-$(json_file_value "$WORKLOAD_MANIFEST" target_eps)}"
WORKLOAD_EVENT_COUNT="$(json_file_value "$WORKLOAD_MANIFEST" event_count)"
FAULT_MODEL_JSON="$(json_file_optional_json "$WORKLOAD_MANIFEST" fault_model)"
WORKLOAD_SIZE_BYTES="$(wc -c < "$WORKLOAD_FILE" | tr -d '[:space:]')"
WORKLOAD_CONFIGMAP="vacciguard-workload-$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]')"
WORKLOAD_S3_URI="s3://${BUCKET_NAME}/${RUN_PREFIX}/workloads/${SCENARIO}.events.ndjson"
WORKLOAD_SOURCE_KIND="configmap"
WORKLOAD_RUNTIME_PATH="/data/workloads/evaluation/events.ndjson"
FAULT_RESULT="not-configured"
REPLAY_WAIT_TIMEOUT_SECONDS="$(python3 - "$WORKLOAD_EVENT_COUNT" "$WORKLOAD_TARGET_EPS" <<'PY'
import math
import sys

event_count = int(sys.argv[1])
events_per_second = float(sys.argv[2])
estimated_seconds = event_count / events_per_second if events_per_second > 0 else 0
print(max(180, math.ceil(estimated_seconds) + 180))
PY
)"

echo "[4/10] Resetting Redis evaluation state"
REDIS_RESET_RESULT="skipped"
if [[ "$RESET_REDIS_STATE" == "true" ]]; then
  RESET_POD="$(wait_for_stream_pod_ready)"
  if [[ -z "$RESET_POD" ]]; then
    echo "Stream processor did not become ready before Redis reset" >&2
    exit 1
  fi
  kubectl exec "$RESET_POD" -n "$NAMESPACE" -- python -c "
import redis

client = redis.Redis(host='${REDIS_HOST}', port=int('${REDIS_PORT}'), db=int('${REDIS_DB}'), decode_responses=True)
keys = list(client.scan_iter(match='device:status:*'))
if keys:
    client.delete(*keys)
client.delete('${REDIS_ACTIVE_BREACHES_KEY}')
print(f'cleared_device_status_keys={len(keys)}')
print('cleared_active_breaches=1')
" >/tmp/vacciguard-redis-reset.txt
  REDIS_RESET_RESULT="$(cat /tmp/vacciguard-redis-reset.txt)"
else
  REDIS_RESET_RESULT="RESET_REDIS_STATE=false"
fi

echo "[5/10] Patching live pipeline config for isolated run ${RUN_ID}"
kubectl create configmap vacciguard-pipeline-config \
  -n "$NAMESPACE" \
  --from-literal=APP_NAME="$APP_NAME" \
  --from-literal=KAFKA_TOPIC="$RUN_TOPIC" \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  --from-literal=KAFKA_TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-1}" \
  --from-literal=KAFKA_STARTING_OFFSETS="earliest" \
  --from-literal=TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
  --from-literal=WATERMARK_DELAY="$WATERMARK_DELAY" \
  --from-literal=REDIS_HOST="$REDIS_HOST" \
  --from-literal=REDIS_PORT="$REDIS_PORT" \
  --from-literal=REDIS_DB="$REDIS_DB" \
  --from-literal=PROCESSED_OUTPUT_PATH="$PROCESSED_OUTPUT_PATH" \
  --from-literal=INVALID_OUTPUT_PATH="$INVALID_OUTPUT_PATH" \
  --from-literal=BREACH_WINDOW_OUTPUT_PATH="$BREACH_WINDOW_OUTPUT_PATH" \
  --from-literal=CHECKPOINT_ROOT="$CHECKPOINT_ROOT" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl delete configmap "$WORKLOAD_CONFIGMAP" -n "$NAMESPACE" --ignore-not-found >/dev/null
if (( WORKLOAD_SIZE_BYTES <= MAX_WORKLOAD_CONFIGMAP_BYTES )); then
  kubectl create configmap "$WORKLOAD_CONFIGMAP" \
    -n "$NAMESPACE" \
    --from-file=events.ndjson="$WORKLOAD_FILE" >/dev/null
else
  WORKLOAD_SOURCE_KIND="s3"
  WORKLOAD_RUNTIME_PATH="$WORKLOAD_S3_URI"
  aws s3 cp "$WORKLOAD_FILE" "$WORKLOAD_S3_URI" >/dev/null
fi

echo "[6/10] Restarting stream processor"
kubectl rollout restart "deployment/${STREAM_DEPLOYMENT}" -n "$NAMESPACE" >/dev/null
kubectl rollout status "deployment/${STREAM_DEPLOYMENT}" -n "$NAMESPACE" --timeout=180s >/dev/null

STREAM_READY=0
STREAM_POD=""
for _ in $(seq 1 36); do
  STREAM_POD="$(latest_stream_pod)"
  if kubectl logs "$STREAM_POD" -n "$NAMESPACE" --tail=200 2>&1 | rg -q "Stream processor is running with [0-9]+ active queries"; then
    STREAM_READY=1
    break
  fi
  sleep 5
done

if [[ "$STREAM_READY" -ne 1 ]]; then
  echo "Stream processor did not become query-ready in time" >&2
  kubectl logs "$STREAM_POD" -n "$NAMESPACE" --tail=200 || true
  exit 1
fi

echo "[7/10] Launching replay job"
kubectl delete job "$REPLAY_JOB_NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null
REPLAY_VOLUME_MOUNTS=""
REPLAY_VOLUMES=""
if [[ "$WORKLOAD_SOURCE_KIND" == "configmap" ]]; then
  REPLAY_VOLUME_MOUNTS="$(cat <<EOF
          volumeMounts:
            - name: workload
              mountPath: /data/workloads/evaluation
EOF
)"
  REPLAY_VOLUMES="$(cat <<EOF
      volumes:
        - name: workload
          configMap:
            name: ${WORKLOAD_CONFIGMAP}
EOF
)"
fi
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${REPLAY_JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: vacciguard-pipeline
      restartPolicy: Never
      containers:
        - name: replay-producer
          image: ${REPLAY_PRODUCER_IMAGE}
          imagePullPolicy: Always
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: ${KAFKA_BOOTSTRAP_SERVERS}
            - name: KAFKA_TOPIC
              value: ${RUN_TOPIC}
            - name: WORKLOAD_FILE
              value: ${WORKLOAD_RUNTIME_PATH}
            - name: EVENTS_PER_SECOND
              value: "${WORKLOAD_TARGET_EPS}"
            - name: LOOP
              value: "false"
${REPLAY_VOLUME_MOUNTS}
${REPLAY_VOLUMES}
EOF

if [[ -n "$FAULT_MODEL_JSON" ]]; then
  FAULT_TYPE="$(python3 - "$FAULT_MODEL_JSON" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload.get("type", ""))
PY
)"
  if [[ "$FAULT_TYPE" == "stream-processor-restart" ]]; then
    FAULT_DELAY_SECONDS="$(python3 - "$FAULT_MODEL_JSON" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
offset_minutes = payload.get("offset_minutes", 0)
print(int(float(offset_minutes) * 60))
PY
)"
    (
      sleep "$FAULT_DELAY_SECONDS"
      kubectl rollout restart "deployment/${STREAM_DEPLOYMENT}" -n "$NAMESPACE" >/dev/null
      kubectl rollout status "deployment/${STREAM_DEPLOYMENT}" -n "$NAMESPACE" --timeout=180s >/dev/null
    ) >/tmp/vacciguard-fault-${RUN_ID}.log 2>&1 &
    FAULT_PID=$!
    FAULT_RESULT="scheduled:${FAULT_TYPE}:delay=${FAULT_DELAY_SECONDS}s:pid=${FAULT_PID}"
  else
    FAULT_RESULT="unsupported:${FAULT_TYPE}"
  fi
fi

kubectl wait --for=condition=complete --timeout="${REPLAY_WAIT_TIMEOUT_SECONDS}s" "job/${REPLAY_JOB_NAME}" -n "$NAMESPACE" >/dev/null

if [[ -n "${FAULT_PID:-}" ]]; then
  if wait "$FAULT_PID"; then
    FAULT_RESULT="completed:${FAULT_TYPE}:delay=${FAULT_DELAY_SECONDS}s"
  else
    FAULT_RESULT="failed:${FAULT_TYPE}:delay=${FAULT_DELAY_SECONDS}s"
  fi
fi

echo "[8/10] Collecting run evidence"
sleep 15
REPLAY_LOGS="$(kubectl logs "job/${REPLAY_JOB_NAME}" -n "$NAMESPACE" --tail=200)"
STREAM_SUMMARY_LOGS="$(kubectl logs "deployment/${STREAM_DEPLOYMENT}" -n "$NAMESPACE" 2>&1 | rg "Batch .* summary|wrote .* latest device states|Stream processor is running with [0-9]+ active queries" || true)"
S3_LISTING="$(aws s3 ls "s3://${BUCKET_NAME}/${RUN_PREFIX}/" --recursive || true)"
POD_SUMMARY="$(kubectl get all -n "$NAMESPACE")"
REPLAY_LOG_PATH="/tmp/vacciguard-replay-${RUN_ID}.log"
STREAM_LOG_PATH="/tmp/vacciguard-stream-${RUN_ID}.log"
printf '%s\n' "$REPLAY_LOGS" >"$REPLAY_LOG_PATH"
printf '%s\n' "$STREAM_SUMMARY_LOGS" >"$STREAM_LOG_PATH"
EVALUATION_METADATA_JSON="$(python3 - <<PY
import json
print(json.dumps({
    "workload_family_version": "${WORKLOAD_FAMILY_VERSION}",
    "scenario": "${SCENARIO}",
    "configured_events_per_second": float("${WORKLOAD_TARGET_EPS}"),
}, sort_keys=True))
PY
)"
EVALUATION_TABLE="$(python3 - "$REPLAY_LOG_PATH" "$STREAM_LOG_PATH" "$WORKLOAD_FAMILY_VERSION" "$SCENARIO" "$WORKLOAD_TARGET_EPS" <<'PY'
import json
import sys
from pathlib import Path

from scripts import aws_baseline_metrics

replay_logs = Path(sys.argv[1]).read_text(encoding="utf-8")
stream_logs = Path(sys.argv[2]).read_text(encoding="utf-8")
metadata = {
    "workload_family_version": sys.argv[3],
    "scenario": sys.argv[4],
    "configured_events_per_second": float(sys.argv[5]),
}
base = aws_baseline_metrics.extract_metrics(replay_logs, stream_logs)
base.update(metadata)
print(aws_baseline_metrics.render_markdown_table(base))
PY
)"
EVALUATION_METRICS_JSON="$(python3 - "$REPLAY_LOG_PATH" "$STREAM_LOG_PATH" "$WORKLOAD_FAMILY_VERSION" "$SCENARIO" "$WORKLOAD_TARGET_EPS" <<'PY'
import json
import sys
from pathlib import Path

from scripts import aws_baseline_metrics

replay_logs = Path(sys.argv[1]).read_text(encoding="utf-8")
stream_logs = Path(sys.argv[2]).read_text(encoding="utf-8")
metadata = {
    "workload_family_version": sys.argv[3],
    "scenario": sys.argv[4],
    "configured_events_per_second": float(sys.argv[5]),
}
base = aws_baseline_metrics.extract_metrics(replay_logs, stream_logs)
base.update(metadata)
print(json.dumps(base, indent=2, sort_keys=True))
PY
)"

echo "[9/10] Writing local report"
cat >"$REPORT_PATH" <<EOF
# AWS Baseline Evaluation ${RUN_ID}

- Run ID: \`${RUN_ID}\`
- AWS account: \`${ACCOUNT_ID}\`
- Region: \`${AWS_REGION}\`
- Cluster: \`${CLUSTER_NAME}\`
- Namespace: \`${NAMESPACE}\`
- Scenario: \`${SCENARIO}\`
- Workload family version: \`${WORKLOAD_FAMILY_VERSION}\`
- Source workload file: \`${SOURCE_WORKLOAD_FILE}\`
- Workload file: \`${WORKLOAD_FILE}\`
- Workload source: \`${WORKLOAD_SOURCE_KIND}\`
- Workload size bytes: \`${WORKLOAD_SIZE_BYTES}\`
- Declared input events: \`${WORKLOAD_EVENT_COUNT}\`
- Configured replay rate: \`${WORKLOAD_TARGET_EPS}\`
- Fault model: \`${FAULT_MODEL_JSON:-none}\`
- Kafka topic: \`${RUN_TOPIC}\`
- S3 prefix: \`s3://${BUCKET_NAME}/${RUN_PREFIX}/\`
- Redis reset: \`${RESET_REDIS_STATE}\`
- Report written at: \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`

## Redis Reset

\`\`\`text
${REDIS_RESET_RESULT}
\`\`\`

## Fault Injection

\`\`\`text
${FAULT_RESULT}
\`\`\`

## Evaluation Summary

${EVALUATION_TABLE}

## Evaluation Metrics JSON

\`\`\`json
${EVALUATION_METRICS_JSON}
\`\`\`

## Replay Logs

\`\`\`text
${REPLAY_LOGS}
\`\`\`

## Stream Summary Logs

\`\`\`text
${STREAM_SUMMARY_LOGS}
\`\`\`

## S3 Objects

\`\`\`text
${S3_LISTING}
\`\`\`

## Cluster Snapshot

\`\`\`text
${POD_SUMMARY}
\`\`\`
EOF

echo "[10/10] Evaluation complete"
echo "Run ID: ${RUN_ID}"
echo "Kafka topic: ${RUN_TOPIC}"
echo "S3 prefix: s3://${BUCKET_NAME}/${RUN_PREFIX}/"
echo "Report: ${REPORT_PATH}"
