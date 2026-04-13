#!/usr/bin/env bash
# run-100eps-1min.sh -- Replay 6,000 events at 100 eps for 1 minute (S3-based)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_CONTEXT="${K8S_CONTEXT:-vacciguard-aayush}"
NAMESPACE="vacciguard"
JOB_NAME="test-replay-100eps"
EPS=100
TOTAL_EVENTS=6000
IMAGE="347038623570.dkr.ecr.ap-south-1.amazonaws.com/vacciguard-aayush-baseline-replay-producer:bootstrap-20260413"
KAFKA_TOPIC="vacciguard-telemetry"
SERVICE_ACCOUNT="vacciguard-pipeline"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-}"
S3_BUCKET="vacciguard-aayush-baseline-data-347038623570"
S3_KEY="workloads/smoke-test-100eps/events.ndjson"
S3_URI="s3://${S3_BUCKET}/${S3_KEY}"
LOCAL_WORKLOAD_FILE="/tmp/workload-100eps.ndjson"

# Try to detect kafka bootstrap servers from existing config
if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "INFO: KAFKA_BOOTSTRAP_SERVERS not set, using default..."
    KAFKA_BOOTSTRAP_SERVERS="kafka.vacciguard.svc.cluster.local:9092"
fi

echo "============================================================"
echo "  Quick Smoke Test: 100 EPS / 1 Minute (S3-based)"
echo "============================================================"
echo "  Events:       $TOTAL_EVENTS"
echo "  Target EPS:   $EPS"
echo "  Duration:     ~60 seconds"
echo "  Topic:        $KAFKA_TOPIC"
echo "  Namespace:    $NAMESPACE"
echo "  Image:        $IMAGE"
echo "  S3 URI:       $S3_URI"
echo "============================================================"

# Step 1: Generate the workload file
echo ""
echo "[1/4] Generating test workload ($TOTAL_EVENTS events)..."
python "$SCRIPT_DIR/generate-test-workload.py" --events "$TOTAL_EVENTS" --eps "$EPS" --output "$LOCAL_WORKLOAD_FILE"

# Step 2: Upload to S3
echo ""
echo "[2/4] Uploading workload to S3 ($S3_URI)..."
aws s3 cp "$LOCAL_WORKLOAD_FILE" "$S3_URI" --region ap-south-1
echo "  Upload complete: $(aws s3 ls "$S3_URI" --region ap-south-1 | awk '{print $3, $4}')"

# Step 3: Launch the replay Job
echo ""
echo "[3/4] Launching replay Job '$JOB_NAME' at $EPS eps..."
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found --wait=true 2>/dev/null || true

cat <<EOF | kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  labels:
    app.kubernetes.io/managed-by: quick-smoke-test
    test-type: "100eps"
spec:
  ttlSecondsAfterFinished: 300
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/managed-by: quick-smoke-test
        test-type: "100eps"
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
            - name: EVENTS_PER_SECOND
              value: "${EPS}"
            - name: WORKLOAD_FILE
              value: "${S3_URI}"
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
              cpu: "500m"
EOF

echo "Job submitted. Waiting for completion..."

# Step 4: Wait for job to finish and print logs
echo ""
echo "[4/4] Waiting for Job to complete (timeout: 180s)..."
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" wait --for=condition=complete job/"$JOB_NAME" --timeout=180s 2>/dev/null || {
    echo "WARNING: Job did not complete within timeout. Fetching logs anyway..."
}

# Print logs
echo ""
echo "============================================================"
echo "  Replay Job Logs:"
echo "============================================================"
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" logs job/"$JOB_NAME" --tail=200 2>/dev/null || echo "WARNING: Could not fetch logs"
echo "------------------------------------------------------------"

# Print stream processor batch summaries
echo ""
echo "============================================================"
echo "  Stream Processor Logs (last 50 lines):"
echo "============================================================"
kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" logs -l "app.kubernetes.io/component=stream-processor" --tail=50 2>/dev/null || echo "No stream processor logs found"
echo "------------------------------------------------------------"

echo ""
echo "============================================================"
echo "  ✅ Replay complete. Run check-telemetry.sh to verify metrics."
echo "============================================================"
echo ""
echo "S3 Workload Location: $S3_URI"
echo "To clean up S3 file: aws s3 rm $S3_URI --region ap-south-1"
echo "To clean up Job: kubectl --context $K8S_CONTEXT -n $NAMESPACE delete job $JOB_NAME"
