# Quick AWS Smoke Tests

1-minute smoke tests that run on the EKS cluster to validate the Vacciguard pipeline at two throughput levels.

## What These Tests Do

- **`run-100eps-1min.sh`** -- Replays 6,000 events at 100 eps for 1 minute.
- **`run-1000eps-1min.sh`** -- Replays 60,000 events at 1,000 eps for 1 minute.
- **`check-telemetry.sh`** -- After each replay, queries Prometheus metrics and verifies Grafana panel availability.
- **`cleanup.sh`** -- Removes all test Kubernetes resources (Jobs, ConfigMaps).

## How It Works

1. The `generate-test-workload.py` script creates a realistic NDJSON workload file and stores it as a Kubernetes ConfigMap in the `vacciguard` namespace.
2. A replay-producer Job is launched that reads the ConfigMap and publishes events to the existing Kafka topic `vacciguard-telemetry` at the target EPS.
3. After the Job completes, logs are printed showing actual EPS achieved and stream processor batch summaries.
4. `check-telemetry.sh` verifies that telemetry flowed through to Prometheus and Grafana.

All resources are temporary and namespaced to `vacciguard`. No existing deployments are modified.

## Prerequisites

- `kubectl` configured with access to the EKS cluster `vacciguard-aayush-baseline-eks`
- AWS CLI configured (for ECR image access)
- `jq` installed (for JSON parsing)
- Access to the `vacciguard` namespace and service account `vacciguard-pipeline`

## Quick Start

```bash
# Run the 100 eps test
./tests/quick-aws-smoke-test/run-100eps-1min.sh

# Run the 1000 eps test
./tests/quick-aws-smoke-test/run-1000eps-1min.sh

# Check telemetry (runs automatically after each test, but can be called standalone)
./tests/quick-aws-smoke-test/check-telemetry.sh

# Clean up all test resources
./tests/quick-aws-smoke-test/cleanup.sh
```

## Resources Created (Temporary)

| Resource Type | Name Pattern | Namespace |
|---|---|---|
| ConfigMap | `test-workload-100eps` / `test-workload-1000eps` | `vacciguard` |
| Job | `test-replay-100eps` / `test-replay-1000eps` | `vacciguard` |

All resources are prefixed with `test-` and can be safely removed with `cleanup.sh`.

## Image Used

Replay producer image: `347038623570.dkr.ecr.ap-south-1.amazonaws.com/vacciguard-aayush-baseline-replay-producer:bootstrap-20260413`
