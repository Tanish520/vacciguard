# Monitoring

This directory contains the baseline observability stack for VacciGuard.

- `prometheus/`: in-cluster metrics collection
- `grafana/`: baseline dashboard presentation

The baseline monitoring split is:

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
- `vacciguard_stream_latest_batch_p99_latency_seconds`
- `vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds`
- `vacciguard_stream_observed_throughput_eps`
- `vacciguard_stream_hot_batch_duration_seconds`
- `vacciguard_stream_cold_batch_duration_seconds`
- `vacciguard_stream_pod_restart_count`
- `vacciguard_stream_queries_active`
- `vacciguard_stream_cumulative_processed_events`
- `vacciguard_stream_latest_batch_event_time_lag_p95_seconds`
- `vacciguard_stream_watermark_delay_seconds`
- `vacciguard_stream_consumer_lag_records`

Current replay metrics:

- `vacciguard_replay_loaded_events`
- `vacciguard_replay_sent_events_total`
- `vacciguard_replay_configured_rate_events_per_second`
- `vacciguard_replay_duration_seconds`
- `vacciguard_replay_completion_status`
- `vacciguard_replay_run_started_timestamp_seconds`
- `vacciguard_replay_completion_timestamp_seconds`

The baseline Grafana dashboard `VacciGuard Baseline Overview` now uses a KPI-first layout:

- top-row KPI cards for avg/P95/P99 latency, throughput, consumer lag, and run-level alerts fired
- a second KPI row for estimated cost per GB, ingest-to-Redis P95, processed volume, and data-quality rates
- trend panels for latency, throughput vs lag, and quality breakdown
- a live alerts table so SLA violations are visible directly in Grafana

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

Prometheus also evaluates a minimal VacciGuard SLA alert rule set for live detection of latency and consumer-lag violations:

- `HighLatency` fires when `vacciguard_stream_latest_batch_avg_latency_seconds > 5`
- `ConsumerLagBuilding` fires when `vacciguard_stream_consumer_lag_records > 1000`

These rules are intentionally small. They are enough to prove live SLA violation detection without adding a separate notification stack.

The dashboard panels are organized around latency, consumer lag, throughput,
and data-quality signals so the live behavior of the pipeline is visible during
local and cluster-based runs.
