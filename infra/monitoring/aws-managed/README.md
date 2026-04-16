# AWS-Managed Observability

This directory documents the AWS-managed observability path for VacciGuard.

It is the live evaluation stack for EKS runs:

- Amazon Managed Service for Prometheus (AMP) stores and queries metrics
- Amazon Managed Grafana presents the dashboards
- CloudWatch and Container Insights provide AWS and cluster health

The comparison dashboard plots baseline and optimized runs on the same panels
by grouping series on the `pipeline_target` label.

Amazon Managed Grafana is provisioned in `ap-southeast-1` for this project.
The EKS cluster and AMP workspace stay in `ap-south-1`, but you must open the
Grafana console in the supported Singapore region to view the dashboard.

The local developer stack under `infra/monitoring/` stays in place for smoke
tests and rapid iteration. The AWS-managed stack is what baseline and optimized
evaluation runs should use when measuring the production-shaped pipeline.
