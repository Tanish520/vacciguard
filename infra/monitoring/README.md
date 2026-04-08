# Monitoring

This directory contains the baseline observability stack for VacciGuard.

- `cloudwatch/`: AWS and EKS runtime visibility guidance
- `prometheus/`: in-cluster metrics collection
- `grafana/`: baseline dashboard presentation

The baseline monitoring split is:

- CloudWatch plus Container Insights for AWS and EKS health
- Prometheus plus Grafana for in-cluster metrics and dashboards
