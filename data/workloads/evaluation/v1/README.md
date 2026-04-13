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

## Regeneration Policy

The `.events.ndjson` files in this directory are generated artifacts and should be regenerated locally instead of being treated as the canonical source of truth in Git.

Use:

```bash
python3 scripts/generate-evaluation-workloads.py
```

The canonical shared inputs for teammates are:

- `scripts/generate-evaluation-workloads.py`
- the committed `.manifest.json` files
- this README

Current shared target rates are:

- `normal`: `100 eps`
- `spike`: `1000 eps`
- `failure-recovery`: `100 eps`

Current shared fault model is:

- `failure-recovery`: restart the stream processor `6 minutes` into the run
