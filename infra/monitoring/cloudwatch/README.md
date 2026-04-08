# CloudWatch Baseline Notes

CloudWatch and Container Insights are the AWS and EKS health layer for the VacciGuard baseline.

They should be used to inspect:

- cluster and node health
- pod and container runtime state
- workload logs for Kafka, replay producer, and stream processor

This document captures the expected baseline signals and activation direction. It does not yet fully automate Container Insights enablement.
