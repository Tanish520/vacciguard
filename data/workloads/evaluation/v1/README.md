# Evaluation Workload Family v1

This directory stores the official `evaluation-workload-v1` scenarios.

- `normal.events.ndjson`: steady-state workload for baseline vs optimized comparison
- `spike.events.ndjson`: 10x replay-rate stress workload
- `failure-recovery.events.ndjson`: normal-rate workload used with a scripted stream-processor restart

Each `.manifest.json` file records:

- workload family version
- scenario name
- device count
- duration
- target replay rate
- target mix percentages
- fault model when applicable
