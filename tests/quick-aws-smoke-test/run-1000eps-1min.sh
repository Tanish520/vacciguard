#!/usr/bin/env bash
# run-1000eps-1min.sh -- Replay 60,000 events at 1000 eps for 1 minute
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_CONTEXT="${K8S_CONTEXT:-vacciguard-aayush}"
NAMESPACE="vacciguard"
LABEL="test-replay-1000eps"
CONFIGMAP_NAME="test-workload-1000eps"
JOB_NAME="test-replay-1000eps"
EPS=1000
TOTAL_EVENTS=60000
IMAGE="347038623570.dkr.ecr.ap-south-1.amazonaws.com/vacciguard-aayush-baseline-replay-producer:bootstrap-20260413"
KAFKA_TOPIC="vacciguard-telemetry"
SERVICE_ACCOUNT="vacciguard-pipeline"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-}"

# Try to detect kafka bootstrap servers from existing config
if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "INFO: KAFKA_BOOTSTRAP_SERVERS not set, attempting to detect from existing environment..."
    KAFKA_BOOTSTRAP_SERVERS=$(kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" get configmap -o json 2>/dev/null | \
        grep -o '"bootstrap_servers"[^,}]*' | head -1 | sed 's/.*: *"\([^"]*\)".*/\1/' || echo "")
fi

if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "ERROR: Cannot determine KAFKA_BOOTSTRAP_SERVERS."
    echo "Please set it: export KAFKA_BOOTSTRAP_SERVERS=<your-kafka-broker:9092>"
    exit 1
fi

echo "============================================================"
echo "  Quick Smoke Test: 1000 EPS / 1 Minute"
echo "============================================================"
echo "  Events:       $TOTAL_EVENTS"
echo "  Target EPS:   $EPS"
echo "  Duration:     ~60 seconds"
echo "  Topic:        $KAFKA_TOPIC"
echo "  Namespace:    $NAMESPACE"
echo "  Image:        $IMAGE"
echo "============================================================"

# Step 1: Generate the workload file
echo ""
echo "[1/5] Generating test workload ($TOTAL_EVENTS events)..."
python3 "$SCRIPT_DIR/generate-test-workload.py" --events "$TOTAL_EVENTS" --eps "$EPS" --output "/tmp/workload-1000eps.ndjson"

# Step 2: Create ConfigMap from workload file
echo ""
echo "[2/5] Creating ConfigMap '$CONFIGMAP_NAME'..."
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" delete configmap "$CONFIGMAP_NAME" --ignore-not-found
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" create configmap "$CONFIGMAP_NAME" --from-file=events.ndjson="/tmp/workload-1000eps.ndjson"
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" label configmap "$CONFIGMAP_NAME" "app.kubernetes.io/managed-by=quick-smoke-test" "test-type=1000eps"

# Step 3: Launch the replay Job
echo ""
echo "[3/5] Launching replay Job '$JOB_NAME' at $EPS eps..."
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found --wait=true

cat <<EOF | kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  labels:
    app.kubernetes.io/managed-by: quick-smoke-test
    test-type: "1000eps"
spec:
  ttlSecondsAfterFinished: 300
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/managed-by: quick-smoke-test
        test-type: "1000eps"
    spec:
      serviceAccountName: ${SERVICE_ACCOUNT}
      restartPolicy: Never
      containers:
        - name: replay-producer
          image: ${IMAGE}
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "${KAFKA_BOOTSTRAP_SERVERS}"
            - name: KAFKA_TOPIC
              value: "${KAFKA_TOPIC}"
            - name: REPLAY_RATE_EPS
              value: "${EPS}"
            - name: WORKLOAD_FILE
              value: "/data/events.ndjson"
            - name: TOTAL_EVENTS
              value: "${TOTAL_EVENTS}"
          volumeMounts:
            - name: workload
              mountPath: /data
              readOnly: true
      volumes:
        - name: workload
          configMap:
            name: ${CONFIGMAP_NAME}
EOF

echo "Job submitted. Waiting for completion..."

# Note: ConfigMaps have a 1 MiB limit. 60k events may exceed this.
# If the ConfigMap creation fails due to size, the script will exit with set -e.
# For 1000 eps tests, consider splitting into multiple smaller ConfigMaps or using a PVC.
echo "NOTE: If ConfigMap exceeds 1 MiB limit, the Job may need to be adapted to use a PVC or external S3."

# Step 4: Wait for job to finish
echo ""
echo "[4/5] Waiting for Job to complete (timeout: 180s)..."
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" wait --for=condition=complete job/"$JOB_NAME" --timeout=180s 2>/dev/null || {
    echo "WARNING: Job did not complete within timeout. Fetching logs anyway..."
}

# Step 5: Print logs
echo ""
echo "[5/5] Replay Job Logs:"
echo "------------------------------------------------------------"
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" logs job/"$JOB_NAME" --tail=200 || echo "WARNING: Could not fetch logs (job may have been cleaned up)"
echo "------------------------------------------------------------"

# Print stream processor batch summaries
echo ""
echo "Stream Processor Logs (last 50 lines):"
echo "------------------------------------------------------------"
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" logs -l "app.kubernetes.io/component=stream-processor" --tail=50 2>/dev/null || echo "No stream processor logs found"
echo "------------------------------------------------------------"

echo ""
echo "Replay complete. Run check-telemetry.sh to verify metrics."
echo "Run cleanup.sh when done to remove test resources."
