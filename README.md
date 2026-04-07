# VacciGuard

VacciGuard is a cloud data pipeline case study for vaccine cold-chain monitoring. The project focuses on how to ingest, process, store, and analyze mixed live and batch data so unsafe storage conditions can be detected quickly and long-term compliance trends can still be studied reliably.

## Current Scope

- Simulated vaccine cold-chain telemetry
- A small device and facility lookup dataset for enrichment
- Daily operations or maintenance logs for batch processing
- Fast breach detection and live status monitoring
- Historical analysis, reporting, and baseline vs optimized evaluation

## Project Goals

1. Define a clear and realistic cloud pipeline problem.
2. Lock a simple hybrid architecture before choosing tools.
3. Keep the dataset design minimal and evaluation-friendly.
4. Build the repo from a clean starting point and add implementation in later phases.

## Success Criteria

- End-to-end delay for live readings stays below 5 seconds under normal workload.
- The pipeline tolerates a 10x traffic spike and recovers from a single component failure within 2 minutes without intentional data loss.
- The optimized design lowers cost per GB processed while still meeting the latency target.

## Repository Layout

- [Project Planning/01-foundation/phase-1-problem-statement.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/phase-1-problem-statement.md): project definition, scope, and success metrics
- [Project Planning/01-foundation/phase-2-conceptual-pipeline.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/phase-2-conceptual-pipeline.md): conceptual hybrid architecture and data flow
- [Project Planning/01-foundation/dataset-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/dataset-plan.md): the three-dataset plan for stream, lookup, and batch inputs
- [Project Planning/02-phase-3/phase-3-technology-stack.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/02-phase-3/phase-3-technology-stack.md): locked technology stack and rationale
- [Project Planning/02-phase-3/phase-3-objective-mapping.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/02-phase-3/phase-3-objective-mapping.md): how the stack covers the professor's objectives
- [Project Planning/02-phase-3/non-functional-requirements/README.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/02-phase-3/non-functional-requirements/README.md): separate non-functional requirement notes
- [Project Planning/03-architecture/vacciguard-architecture-overview.html](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/03-architecture/vacciguard-architecture-overview.html): colorful visual overview of the architecture and system working flow
- [Project Planning/project-folder-structure.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-folder-structure.md): implementation folder structure and the purpose of each folder
- [data/](/Users/tanishgupta/Desktop/vacciguard/data): datasets for lookup, batch, and replay workloads
- [services/](/Users/tanishgupta/Desktop/vacciguard/services): code for the replay producer and Spark processors
- [orchestration/](/Users/tanishgupta/Desktop/vacciguard/orchestration): Airflow DAGs and related orchestration config
- [infra/](/Users/tanishgupta/Desktop/vacciguard/infra): Terraform, Kubernetes, and monitoring setup
- [tests/](/Users/tanishgupta/Desktop/vacciguard/tests): smoke, workload, and failure validation areas
- [results/](/Users/tanishgupta/Desktop/vacciguard/results): baseline and optimized experiment outputs

## Current Status

This repository now has the planning foundation and the initial implementation folder structure in place. The next steps are to build the replay producer, processing services, deployment configuration, and evaluation workflow inside the new folders.

## Phase 4 Local Runbook

1. Generate the replay workload:

```bash
python3 scripts/generate-dev-workload.py
```

2. Start the local services:

```bash
docker compose up -d kafka redis stream-processor
```

3. Replay the workload:

```bash
docker compose run --rm replay-producer
```

4. Inspect Redis:

```bash
docker compose exec redis redis-cli GET device:status:FR-0102
docker compose exec redis redis-cli ZRANGE active_breaches 0 -1 WITHSCORES
```

5. Inspect cold outputs:

```bash
find data/output/processed -maxdepth 1 -name '*.parquet' -type f | sort
find data/output/invalid -maxdepth 1 -name '*.json' -type f | sort
```

6. Run smoke verification:

```bash
python3 tests/smoke/verify_phase4.py
```

7. Use the one-command local helper:

```bash
bash scripts/run-phase4-local.sh
```

8. Inspect stream batch summaries:

```bash
docker compose logs stream-processor | grep "Batch .* summary"
```
