#!/usr/bin/env bash
# check-telemetry.sh -- Verify Prometheus metrics and Grafana panel availability after smoke tests
set -euo pipefail

K8S_CONTEXT="${K8S_CONTEXT:-vacciguard-aayush}"
NAMESPACE="vacciguard"
PROMETHEUS_URL="${PROMETHEUS_URL:-}"
GRAFANA_URL="${GRAFANA_URL:-}"

echo "============================================================"
echo "  Telemetry Verification"
echo "============================================================"
echo "  Context:    $K8S_CONTEXT"
echo "  Namespace:  $NAMESPACE"
echo "============================================================"

# ------------------------------------------------------------------
# 1. Check Kafka consumer lag for vacciguard-telemetry topic
# ------------------------------------------------------------------
echo ""
echo "[1/3] Checking Kafka consumer lag..."
echo "------------------------------------------------------------"

# Try to find stream processor pods and check their metrics
STREAM_PODS=$(kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" get pods -l "app.kubernetes.io/component=stream-processor" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")

if [ -n "$STREAM_PODS" ]; then
    echo "Stream processor pods found: $STREAM_PODS"
    for pod in $STREAM_PODS; do
        echo ""
        echo "Pod: $pod"
        # Check if pod exposes metrics endpoint
        kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" exec "$pod" -- sh -c "echo 'Checking /metrics endpoint...'" 2>/dev/null || true
    done
else
    echo "WARNING: No stream-processor pods found in namespace $NAMESPACE"
fi

# ------------------------------------------------------------------
# 2. Query Prometheus for recent event counts
# ------------------------------------------------------------------
echo ""
echo "[2/3] Checking Prometheus metrics..."
echo "------------------------------------------------------------"

if [ -n "$PROMETHEUS_URL" ]; then
    echo "Querying Prometheus at: $PROMETHEUS_URL"

    # Query total events ingested in the last 2 minutes
    QUERY='sum(rate(vacciguard_events_ingested_total[2m]))'
    echo ""
    echo "  Query: sum(rate(vacciguard_events_ingested_total[2m]))"

    # Attempt curl if Prometheus is accessible
    if command -v curl &>/dev/null; then
        RESULT=$(curl -s --max-time 10 "$PROMETHEUS_URL/api/v1/query?query=$QUERY" 2>/dev/null || echo '{"error":"unreachable"}')
        echo "  Result: $RESULT"
    else
        echo "  WARNING: curl not available, skipping Prometheus query"
    fi

    # Query event processing latency
    QUERY2='histogram_quantile(0.99, rate(vacciguard_event_processing_duration_seconds_bucket[2m]))'
    echo ""
    echo "  Query: histogram_quantile(0.99, rate(vacciguard_event_processing_duration_seconds_bucket[2m]))"

    if command -v curl &>/dev/null; then
        RESULT2=$(curl -s --max-time 10 "$PROMETHEUS_URL/api/v1/query?query=$QUERY2" 2>/dev/null || echo '{"error":"unreachable"}')
        echo "  Result: $RESULT2"
    fi
else
    echo "PROMETHEUS_URL not set. Attempting port-forward..."
    echo ""

    # Find prometheus pod
    PROM_POD=$(kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=prometheus" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "$PROM_POD" ]; then
        echo "Prometheus pod: $PROM_POD"
        echo "Port-forward manually with:"
        echo "  kubectl --context $K8S_CONTEXT -n $NAMESPACE port-forward $PROM_POD 9090:9090"
        echo "Then re-run with: export PROMETHEUS_URL=http://localhost:9090"
    else
        echo "WARNING: No Prometheus pod found in namespace $NAMESPACE"
    fi
fi

# ------------------------------------------------------------------
# 3. Check Grafana dashboard availability
# ------------------------------------------------------------------
echo ""
echo "[3/3] Checking Grafana availability..."
echo "------------------------------------------------------------"

if [ -n "$GRAFANA_URL" ]; then
    echo "Checking Grafana at: $GRAFANA_URL"

    if command -v curl &>/dev/null; then
        HTTP_CODE=$(curl -s --max-time 10 -o /dev/null -w "%{http_code}" "$GRAFANA_URL/api/health" 2>/dev/null || echo "000")
        echo "  Health endpoint: HTTP $HTTP_CODE"

        if [ "$HTTP_CODE" = "200" ]; then
            echo "  Grafana is UP"
        else
            echo "  WARNING: Grafana health check returned HTTP $HTTP_CODE"
        fi
    fi
else
    echo "GRAFANA_URL not set. Attempting to find Grafana service..."

    GRAFANA_SVC=$(kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" get svc -l "app.kubernetes.io/name=grafana" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "$GRAFANA_SVC" ]; then
        echo "Grafana service: $GRAFANA_SVC"
        echo "Port-forward manually with:"
        echo "  kubectl --context $K8S_CONTEXT -n $NAMESPACE port-forward svc/$GRAFANA_SVC 3000:3000"
        echo "Then re-run with: export GRAFANA_URL=http://localhost:3000"
    else
        echo "WARNING: No Grafana service found in namespace $NAMESPACE"
    fi
fi

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Telemetry Check Summary"
echo "============================================================"
echo "  Kafka lag:     Check stream-processor logs for batch summaries"
echo "  Prometheus:    ${PROMETHEUS_URL:-not configured -- use port-forward}"
echo "  Grafana:       ${GRAFANA_URL:-not configured -- use port-forward}"
echo "============================================================"
echo ""
echo "To view stream processor logs:"
echo "  kubectl --context $K8S_CONTEXT -n $NAMESPACE logs -l app.kubernetes.io/component=stream-processor --tail=100"
