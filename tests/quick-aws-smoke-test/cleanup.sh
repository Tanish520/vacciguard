#!/usr/bin/env bash
# cleanup.sh -- Remove all test resources created by quick smoke tests
set -euo pipefail

K8S_CONTEXT="${K8S_CONTEXT:-vacciguard-aayush}"
NAMESPACE="vacciguard"

echo "============================================================"
echo "  Cleaning up Quick Smoke Test Resources"
echo "============================================================"
echo "  Context:  $K8S_CONTEXT"
echo "  Namespace: $NAMESPACE"
echo "============================================================"

# List of test jobs to clean
TEST_JOBS=("test-replay-100eps" "test-replay-1000eps")
# List of test configmaps to clean
TEST_CONFIGMAPS=("test-workload-100eps" "test-workload-1000eps")

# Delete Jobs
echo ""
echo "Deleting test Jobs..."
for job in "${TEST_JOBS[@]}"; do
    echo "  - Deleting job: $job"
    kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" delete job "$job" --ignore-not-found --wait=true 2>/dev/null || \
        echo "    (job $job not found, skipping)"
done

# Delete ConfigMaps
echo ""
echo "Deleting test ConfigMaps..."
for cm in "${TEST_CONFIGMAPS[@]}"; do
    echo "  - Deleting configmap: $cm"
    kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" delete configmap "$cm" --ignore-not-found 2>/dev/null || \
        echo "    (configmap $cm not found, skipping)"
done

# Also clean up any local temp files
echo ""
echo "Cleaning up local temp files..."
rm -f /tmp/workload-100eps.ndjson /tmp/workload-1000eps.ndjson 2>/dev/null || true

echo ""
echo "Cleanup complete."
echo "Verify with: kubectl --context $K8S_CONTEXT -n $NAMESPACE get jobs,configmaps -l app.kubernetes.io/managed-by=quick-smoke-test"
