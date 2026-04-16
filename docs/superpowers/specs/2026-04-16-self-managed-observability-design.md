# Self-Managed Observability Design

## Goal

Move the AWS evaluation path for VacciGuard away from Amazon Managed Grafana and onto the existing self-managed observability stack so baseline and optimized runs can be viewed immediately without region or Identity Center blockers.

## Problem Statement

The current AWS-managed observability approach is blocked in practice:

- Amazon Managed Grafana is not available in `ap-south-1`.
- Grafana creation in a supported region still depends on AWS account identity setup that is not in place.
- The project already has a working in-cluster Prometheus/Grafana stack for developer use, so duplicating the AWS-managed path adds complexity without improving the comparison workflow.

We still want a clear dashboard for AWS baseline vs optimized runs, but we want it to work with the least operational friction.

## Recommended Approach

Use the **full self-managed observability stack** for AWS evaluation runs:

- Prometheus runs in the cluster and scrapes the application metrics endpoints.
- Grafana runs in the cluster and renders the comparison dashboard.
- CloudWatch remains available for logs and AWS infrastructure visibility.

This preserves the existing metrics model and gives us a dashboard we can open without AWS-managed Grafana regional or account prerequisites.

## Alternatives Considered

### 1. Keep AMP, replace only Grafana

This would keep the metrics backend in AWS while moving the dashboard back in-cluster.

Tradeoff:
- lower change surface than a full rollback
- but still leaves the evaluation path split across AWS-managed and self-managed pieces

### 2. Use CloudWatch only

This would remove Prometheus and Grafana from the AWS path and rely on CloudWatch logs and basic metrics.

Tradeoff:
- simplest operationally
- but we lose the baseline-vs-optimized comparison dashboard and the Prometheus-style metric queries that are already working well

### 3. Full self-managed stack for AWS runs

This is the recommended option.

Tradeoff:
- one extra in-cluster service to operate
- but it removes the AWS Managed Grafana / IAM Identity Center / region dependencies and keeps the dashboard immediately usable

## Architecture

### Metrics Flow

The stream processor and replay producer continue to expose Prometheus-format metrics endpoints in the `vacciguard` namespace.

Prometheus scrapes:

- `stream-processor-metrics`
- `replay-producer-metrics`

Those metrics are labeled with `pipeline_target` so the dashboard can compare baseline and optimized runs on the same chart.

### Dashboard Flow

Grafana runs in-cluster and queries the in-cluster Prometheus data source.

The comparison dashboard keeps the same panels:

- average latency
- P95 latency
- ingest-to-Redis P95
- processed events
- invalid events
- deduplicated events
- breach events
- consumer lag

Every panel must show real data during normal AWS runs, not just a configured
empty chart. The dashboard is only considered complete when the baseline and
optimized series both populate with live values for the same run window.

### AWS Logs and Infrastructure Visibility

CloudWatch remains the AWS-native place for:

- stream processor logs
- replay producer logs
- evaluation controller logs
- alarms and cluster-level visibility where useful

This keeps the AWS console useful without making it the primary dashboarding path.

## Operational Model

1. Deploy the Kubernetes observability stack in the cluster.
2. Run baseline or optimized AWS evaluations as usual.
3. Open Grafana inside the cluster or through port-forwarding.
4. Inspect the baseline-vs-optimized dashboard.
5. Use CloudWatch only for logs, alerts, and AWS infrastructure checks.

## Non-Goals

- Do not keep the AWS Managed Grafana provisioning path alive.
- Do not require IAM Identity Center for the dashboard path.
- Do not change the pipeline processing behavior again as part of this observability switch.
- Do not change the metric names already used by the evaluation and dashboard code unless a test requires it.

## Testing Strategy

We should verify:

- the in-cluster monitoring manifests still render cleanly
- the dashboard JSON remains valid
- the evaluation run still emits the metrics that Grafana expects
- the AWS evaluation scripts still produce baseline and optimized runs with the same labels

Recommended checks:

```bash
kubectl kustomize infra/monitoring/prometheus
kubectl kustomize infra/monitoring/grafana
python3 -m unittest tests.monitoring.test_monitoring_manifests -v
python3 -m unittest tests.stream.test_job -v
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
```

## Success Criteria

- A user can run the AWS baseline and optimized evaluations.
- The dashboard can be opened without AWS Managed Grafana.
- Baseline and optimized series appear together on the same Grafana panels.
- All comparison panels show live metric data during a normal run.
- CloudWatch remains available as the AWS-native logging layer.
- The repo no longer depends on AWS Managed Grafana being available in the region.
