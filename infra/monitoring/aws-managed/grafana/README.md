# Managed Grafana Dashboard Guidance

This folder contains Grafana dashboards for the AWS-managed observability
stack.

The baseline-vs-optimized dashboard should be imported into Amazon Managed
Grafana after the AMP workspace and Grafana workspace are provisioned.

Expected data sources:

- Amazon Managed Service for Prometheus (AMP) for application metrics
- CloudWatch for infrastructure and log context

The dashboard expects the scraped application metrics to include a
`pipeline_target` label so the baseline and optimized series can appear on the
same chart.

The dashboard should focus on the same metrics produced by the stream processor
and replay producer so baseline and optimized runs can be compared on one page.

Recommended panels:

- avg, P95, and P99 latency
- ingest-to-Redis P95
- observed throughput and consumer lag
- processed, invalid, dedup, and breach rates
- pod restarts, active queries, hot/cold batch duration, and cumulative processed events

Open the Grafana workspace in `ap-southeast-1`. AWS Managed Grafana is not
available in the Mumbai region, so the console must be switched to Singapore
before you can view or edit the VacciGuard dashboard.
