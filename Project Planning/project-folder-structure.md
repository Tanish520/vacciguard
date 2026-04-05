# VacciGuard Project Folder Structure

## Recommended Repository Structure

```text
vacciguard/
  README.md
  .gitignore

  Project Planning/
    01-foundation/
    02-phase-3/
    03-architecture/
    team-guides/
    project-folder-structure.md

  data/
    reference/
    batch/
    workloads/
      dev/
      main/
      heavy/

  services/
    replay-producer/
    stream-processor/
    batch-processor/

  orchestration/
    airflow/
      dags/
      configs/

  infra/
    terraform/
    kubernetes/
      base/
      baseline/
      optimized/
    monitoring/
      cloudwatch/
      prometheus/
      grafana/

  tests/
    smoke/
    workload/
    failure/

  scripts/

  results/
    baseline/
    optimized/
```

## Why This Structure Is Needed

The goal of this structure is to keep the project simple while still supporting the full
implementation, deployment, testing, and evaluation flow. Each top-level folder has one clear
purpose so the team does not mix planning docs, datasets, code, infrastructure, and results.

## Folder Explanations

### `Project Planning/`
This folder keeps all planning and professor-facing documentation in one place.

Why needed:
- stores the phase-wise design decisions
- keeps architecture and methodology separate from implementation code
- gives the team a shared planning reference before coding

### `data/`
This folder stores all non-code datasets used by the project.

Subfolders:
- `data/reference/`: lookup and enrichment data such as device-to-facility mapping and temperature thresholds
- `data/batch/`: daily operations or maintenance logs used for batch analytics
- `data/workloads/dev/`: small workload files for quick development checks
- `data/workloads/main/`: main workload files used for the baseline versus optimized comparison
- `data/workloads/heavy/`: stronger workload files used only if the main profile is not stressful enough

Why needed:
- separates datasets from source code
- supports repeatable experiments
- gives Alok a clear home for the workload system

### `services/`
This folder contains runnable project components.

Subfolders:
- `services/replay-producer/`: code for the Kafka replay producer that publishes the precomputed workloads
- `services/stream-processor/`: Spark Structured Streaming code for live processing
- `services/batch-processor/`: Spark batch jobs for scheduled reports and historical analytics

Why needed:
- gives each main runtime service its own isolated code area
- makes Dockerization easier
- makes ownership across teammates cleaner

### `orchestration/`
This folder keeps workflow orchestration separate from processing logic.

Subfolders:
- `orchestration/airflow/dags/`: Airflow DAG definitions
- `orchestration/airflow/configs/`: Airflow-specific configuration files

Why needed:
- keeps Airflow focused on batch orchestration and scheduled evaluation
- avoids mixing DAG logic with Spark job code

### `infra/`
This folder stores the cloud and deployment configuration.

Subfolders:
- `infra/terraform/`: AWS infrastructure as code
- `infra/kubernetes/base/`: common manifests shared by both pipeline versions
- `infra/kubernetes/baseline/`: fixed-capacity configuration for the baseline pipeline
- `infra/kubernetes/optimized/`: autoscaling and optimization configuration for the optimized pipeline
- `infra/monitoring/cloudwatch/`: AWS monitoring setup notes or manifests
- `infra/monitoring/prometheus/`: Prometheus setup
- `infra/monitoring/grafana/`: Grafana dashboards and configuration

Why needed:
- keeps deployment and cloud setup reproducible
- makes baseline versus optimized differences explicit
- gives Monty a clear ownership area for AWS, EKS, monitoring, and scaling

### `tests/`
This folder stores validation and evaluation checks.

Subfolders:
- `tests/smoke/`: quick checks that the basic pipeline works
- `tests/workload/`: workload and replay validation tests
- `tests/failure/`: restart, recovery, and resilience checks

Why needed:
- separates implementation from verification
- supports the evaluation phases of the project
- prevents test scripts from getting lost in random folders

### `scripts/`
This folder stores helper scripts that do not belong to one service only.

Examples:
- dataset-generation helpers
- one-off setup commands
- utility scripts for collecting or cleaning result artifacts

Why needed:
- keeps miscellaneous helpers out of the main service directories
- reduces clutter in the implementation folders

### `results/`
This folder stores experiment outputs.

Subfolders:
- `results/baseline/`: graphs, logs, summaries, and measurements from the baseline pipeline
- `results/optimized/`: graphs, logs, summaries, and measurements from the optimized pipeline

Why needed:
- preserves evaluation outputs in an organized way
- makes the final comparison easier to write and present
- gives Tanish a clean place to assemble the final analysis

## Ownership Mapping

- **Alok**: `data/`, `services/replay-producer/`, parts of `tests/workload/`
- **Aayush**: `services/stream-processor/`, `services/batch-processor/`
- **Monty**: `infra/`, `orchestration/`, parts of `tests/failure/`
- **Tanish**: integration across all folders, `results/`, and structure consistency

## Design Rule

This structure should remain simple.

The team should avoid adding deep nested folders unless a folder becomes crowded enough to justify
more separation. The current structure is intentionally small but complete enough to finish the
whole project.
