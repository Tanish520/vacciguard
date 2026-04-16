# Monitoring

This directory contains the baseline observability stack for VacciGuard.

- `cloudwatch/`: AWS and EKS runtime visibility guidance
- `prometheus/`: in-cluster metrics collection
- `grafana/`: baseline dashboard presentation
- `aws-managed/`: AMP, Amazon Managed Grafana, and CloudWatch guidance for EKS evaluation runs

The baseline monitoring split is:

- CloudWatch plus Container Insights for AWS and EKS health
- Prometheus plus Grafana for in-cluster metrics and dashboards

## Operational Monitoring

Prometheus and Grafana are now wired to real application-owned metrics rather than placeholder-only manifests.

Application endpoints:

- stream processor: `:9108/metrics`
- replay producer: `:9109/metrics`

Prometheus scrape jobs:

- `stream-processor-metrics`
- `replay-producer-metrics`

Current stream metrics:

- `vacciguard_stream_latest_batch_id`
- `vacciguard_stream_latest_batch_timestamp_seconds`
- `vacciguard_stream_processed_events_total`
- `vacciguard_stream_invalid_events_total`
- `vacciguard_stream_deduplicated_events_total`
- `vacciguard_stream_breach_events_total`
- `vacciguard_stream_latest_batch_avg_latency_seconds`
- `vacciguard_stream_latest_batch_p95_latency_seconds`

Current replay metrics:

- `vacciguard_replay_loaded_events`
- `vacciguard_replay_sent_events_total`
- `vacciguard_replay_configured_rate_events_per_second`
- `vacciguard_replay_duration_seconds`
- `vacciguard_replay_completion_status`
- `vacciguard_replay_run_started_timestamp_seconds`
- `vacciguard_replay_completion_timestamp_seconds`

The baseline Grafana dashboard `VacciGuard Baseline Overview` currently shows:

- replay sent events
- replay completion status
- stream processed, invalid, deduplicated, and breach totals
- latest batch average latency
- latest batch P95 latency

Deploy and inspect:

```bash
kubectl apply -k infra/monitoring/prometheus
kubectl apply -k infra/monitoring/grafana
kubectl port-forward -n monitoring svc/prometheus 9090:9090
kubectl port-forward -n monitoring svc/grafana 3000:3000
```

Render checks:

```bash
kubectl kustomize infra/monitoring/prometheus > /tmp/vacciguard-prometheus.yaml
kubectl kustomize infra/monitoring/grafana > /tmp/vacciguard-grafana.yaml
```

Important caveat: the replay producer is a short-lived Kubernetes Job, so its metrics are only scrapeable while the job pod still exists. The replay image keeps a short post-run drain window through `REPLAY_METRICS_DRAIN_SECONDS` so the final completion status is still visible to Prometheus and Grafana.

Prometheus and Grafana remain the live observability layer during AWS-native evaluation runs.
The evaluation controller does not replace monitoring. It executes the run inside EKS and writes the formal report to S3 after the run completes.

When the AWS-managed path is enabled, keep the local stack for development but
send evaluation runs through the managed bundle under `infra/monitoring/aws-managed`
and `infra/kubernetes/aws-observability`.
