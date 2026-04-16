# Managed Grafana Dashboard Guidance

This folder contains Grafana dashboards for the AWS-managed observability
stack.

The baseline-vs-optimized dashboard should be imported into Amazon Managed
Grafana after the AMP workspace and Grafana workspace are provisioned.

Expected data sources:

- Amazon Managed Service for Prometheus (AMP) for application metrics
- CloudWatch for infrastructure and log context

The dashboard should focus on the same metrics produced by the stream processor
and replay producer so baseline and optimized runs can be compared on one page.
