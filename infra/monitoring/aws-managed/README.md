# AWS-Managed Observability

This directory documents the AWS-managed observability path for VacciGuard.

It is the live evaluation stack for EKS runs:

- Amazon Managed Service for Prometheus (AMP) stores and queries metrics
- Amazon Managed Grafana presents the dashboards
- CloudWatch and Container Insights provide AWS and cluster health

The local developer stack under `infra/monitoring/` stays in place for smoke
tests and rapid iteration. The AWS-managed stack is what baseline and optimized
evaluation runs should use when measuring the production-shaped pipeline.
