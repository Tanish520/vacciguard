# Evaluation Workload Family Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first official `evaluation-workload-v1` family, generate deterministic `normal`, `spike`, and `failure-recovery` workloads, and wire the AWS evaluation flow to record workload version and scenario in the report.

**Architecture:** Add one dedicated generator script for evaluation workloads instead of overloading the existing dev smoke generator. Store workload metadata alongside generated NDJSON artifacts so each scenario stays reproducible and versioned. Extend the AWS evaluation script to accept scenario-aware inputs and emit workload-family metadata in reports without breaking the current baseline run flow.

**Tech Stack:** Python 3.9, NDJSON, Bash, unittest, existing VacciGuard scripts and AWS evaluation tooling

---

## File Structure

- Create: `scripts/generate-evaluation-workloads.py`
  Responsibility: deterministic generator for `evaluation-workload-v1` scenarios and metadata.
- Create: `tests/workload/test_generate_evaluation_workloads.py`
  Responsibility: generator behavior tests, workload counts, deterministic output assertions.
- Create: `data/workloads/evaluation/v1/.gitkeep`
  Responsibility: repository anchor for generated evaluation workload outputs.
- Create: `data/workloads/evaluation/v1/README.md`
  Responsibility: human-readable explanation of scenario files and versioning rules.
- Modify: `scripts/run-aws-baseline-evaluation.sh`
  Responsibility: accept scenario/workload arguments, inject the chosen workload into the replay job, and record workload family metadata in the report.
- Modify: `docs/aws-baseline-foundation.md`
  Responsibility: document how to generate and run the official evaluation scenarios.
- Modify: `tests/evaluation/test_aws_baseline_metrics.py`
  Responsibility: verify the report path preserves workload metadata and scenario-specific fields if helper code changes.

### Task 1: Build The Deterministic Evaluation Workload Generator

**Files:**
- Create: `tests/workload/test_generate_evaluation_workloads.py`
- Create: `scripts/generate-evaluation-workloads.py`
- Create: `data/workloads/evaluation/v1/.gitkeep`

- [ ] **Step 1: Write the failing tests for generator metadata and scenario sizing**

```python
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "generate-evaluation-workloads.py"
)
SPEC = importlib.util.spec_from_file_location("generate_evaluation_workloads", MODULE_PATH)
generator = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(generator)


class EvaluationWorkloadGeneratorTests(unittest.TestCase):
    def test_build_workload_family_metadata_locks_v1_defaults(self):
        metadata = generator.build_workload_family_metadata()

        self.assertEqual(metadata["workload_family_version"], "evaluation-workload-v1")
        self.assertEqual(metadata["devices"], 30)
        self.assertEqual(metadata["duration_minutes"], 12)
        self.assertEqual(metadata["normal_eps"], 6.0)
        self.assertEqual(metadata["spike_eps"], 60.0)

    def test_generate_normal_scenario_creates_expected_base_volume(self):
        metadata = generator.build_workload_family_metadata()
        events = generator.generate_scenario("normal", metadata)

        self.assertGreaterEqual(len(events), 4320)
        self.assertLess(len(events), 5000)
        self.assertTrue(all("event_id" in event for event in events))

    def test_write_outputs_creates_ndjson_and_manifest(self):
        metadata = generator.build_workload_family_metadata()
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            generator.write_scenario_outputs(output_dir, "normal", metadata)

            manifest = json.loads((output_dir / "normal.manifest.json").read_text())
            lines = (output_dir / "normal.events.ndjson").read_text().strip().splitlines()

        self.assertEqual(manifest["scenario"], "normal")
        self.assertEqual(manifest["workload_family_version"], "evaluation-workload-v1")
        self.assertGreater(len(lines), 4000)
```

- [ ] **Step 2: Run the new workload tests to verify they fail**

Run: `python3 -m unittest tests.workload.test_generate_evaluation_workloads -v`

Expected: FAIL with `FileNotFoundError` for `scripts/generate-evaluation-workloads.py`

- [ ] **Step 3: Write the minimal generator implementation**

```python
#!/usr/bin/env python3
import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


DEFAULT_METADATA = {
    "workload_family_version": "evaluation-workload-v1",
    "devices": 30,
    "duration_minutes": 12,
    "normal_eps": 6.0,
    "spike_eps": 60.0,
    "seed": 20260408,
}


def build_workload_family_metadata():
    return dict(DEFAULT_METADATA)


def generate_scenario(scenario, metadata):
    random.seed(metadata["seed"])
    total_seconds = metadata["duration_minutes"] * 60
    eps = metadata["normal_eps"]
    if scenario == "spike":
        eps = metadata["spike_eps"]

    total_events = int(total_seconds * eps)
    start_time = datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)
    events = []
    for index in range(total_events):
        event_time = start_time + timedelta(seconds=index / eps)
        device_number = (index % metadata["devices"]) + 1
        events.append(
            {
                "event_id": f"{scenario}-evt-{index + 1:06d}",
                "device_id": f"FR-{device_number:04d}",
                "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temperature_c": 5.0,
                "door_open": False,
                "battery_pct": 80,
                "location_lat": 24.0 + device_number / 1000,
                "location_lon": 73.0 + device_number / 1000,
            }
        )
    return events


def write_scenario_outputs(output_dir, scenario, metadata):
    output_dir.mkdir(parents=True, exist_ok=True)
    events = generate_scenario(scenario, metadata)
    (output_dir / f"{scenario}.events.ndjson").write_text(
        "\n".join(json.dumps(event) for event in events) + "\n",
        encoding="utf-8",
    )
    (output_dir / f"{scenario}.manifest.json").write_text(
        json.dumps(
            {
                "scenario": scenario,
                "workload_family_version": metadata["workload_family_version"],
                "event_count": len(events),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/workloads/evaluation/v1")
    args = parser.parse_args()

    metadata = build_workload_family_metadata()
    output_dir = Path(args.output_dir)
    for scenario in ("normal", "spike", "failure-recovery"):
        write_scenario_outputs(output_dir, scenario, metadata)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the workload tests to verify they pass**

Run: `python3 -m unittest tests.workload.test_generate_evaluation_workloads -v`

Expected: PASS

- [ ] **Step 5: Commit the generator foundation**

```bash
git add tests/workload/test_generate_evaluation_workloads.py scripts/generate-evaluation-workloads.py data/workloads/evaluation/v1/.gitkeep
git commit -m "feat: add evaluation workload generator foundation"
```

### Task 2: Encode The Official V1 Scenario Mix

**Files:**
- Modify: `scripts/generate-evaluation-workloads.py`
- Modify: `tests/workload/test_generate_evaluation_workloads.py`
- Create: `data/workloads/evaluation/v1/README.md`

- [ ] **Step 1: Write the failing tests for scenario-specific mix and manifests**

```python
class EvaluationScenarioMixTests(unittest.TestCase):
    def test_normal_manifest_records_mix_targets(self):
        metadata = generator.build_workload_family_metadata()
        manifest = generator.build_scenario_manifest("normal", metadata, event_count=4500)

        self.assertEqual(manifest["scenario"], "normal")
        self.assertEqual(manifest["mix_targets"]["duplicates_pct"], 0.05)
        self.assertEqual(manifest["mix_targets"]["late_pct"], 0.03)
        self.assertEqual(manifest["mix_targets"]["invalid_pct"], 0.02)

    def test_spike_manifest_uses_10x_rate(self):
        metadata = generator.build_workload_family_metadata()
        manifest = generator.build_scenario_manifest("spike", metadata, event_count=43200)

        self.assertEqual(manifest["target_eps"], 60.0)

    def test_failure_recovery_manifest_records_fault_model(self):
        metadata = generator.build_workload_family_metadata()
        manifest = generator.build_scenario_manifest("failure-recovery", metadata, event_count=4500)

        self.assertEqual(manifest["fault_model"]["type"], "stream-processor-restart")
```

- [ ] **Step 2: Run the specific workload tests to verify they fail**

Run: `python3 -m unittest tests.workload.test_generate_evaluation_workloads.EvaluationScenarioMixTests -v`

Expected: FAIL with `AttributeError` for missing `build_scenario_manifest`

- [ ] **Step 3: Implement the official scenario metadata and controlled event shaping**

```python
SCENARIO_DEFAULTS = {
    "normal": {
        "target_eps": 6.0,
        "mix_targets": {"duplicates_pct": 0.05, "late_pct": 0.03, "invalid_pct": 0.02},
    },
    "spike": {
        "target_eps": 60.0,
        "mix_targets": {"duplicates_pct": 0.05, "late_pct": 0.03, "invalid_pct": 0.02},
    },
    "failure-recovery": {
        "target_eps": 6.0,
        "mix_targets": {"duplicates_pct": 0.05, "late_pct": 0.03, "invalid_pct": 0.02},
        "fault_model": {"type": "stream-processor-restart", "offset_minutes": 6},
    },
}


def build_scenario_manifest(scenario, metadata, event_count):
    scenario_defaults = SCENARIO_DEFAULTS[scenario]
    manifest = {
        "workload_family_version": metadata["workload_family_version"],
        "scenario": scenario,
        "devices": metadata["devices"],
        "duration_minutes": metadata["duration_minutes"],
        "target_eps": scenario_defaults["target_eps"],
        "mix_targets": scenario_defaults["mix_targets"],
        "event_count": event_count,
    }
    if "fault_model" in scenario_defaults:
        manifest["fault_model"] = scenario_defaults["fault_model"]
    return manifest
```

- [ ] **Step 4: Add the workload-family README**

```markdown
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
```

- [ ] **Step 5: Run the workload tests again**

Run: `python3 -m unittest tests.workload.test_generate_evaluation_workloads -v`

Expected: PASS

- [ ] **Step 6: Commit the workload mix definition**

```bash
git add scripts/generate-evaluation-workloads.py tests/workload/test_generate_evaluation_workloads.py data/workloads/evaluation/v1/README.md
git commit -m "feat: define evaluation workload v1 scenarios"
```

### Task 3: Make The AWS Evaluation Script Scenario-Aware

**Files:**
- Modify: `scripts/run-aws-baseline-evaluation.sh`
- Modify: `tests/evaluation/test_aws_baseline_metrics.py`

- [ ] **Step 1: Write the failing tests for workload metadata rendering**

```python
    def test_render_markdown_table_preserves_workload_metadata(self):
        table = aws_baseline_metrics.render_markdown_table(
            {
                "scenario": "normal",
                "workload_family_version": "evaluation-workload-v1",
                "avg_end_to_end_latency_seconds": 1.25,
                "p95_end_to_end_latency_seconds": 2.5,
            }
        )

        self.assertIn("evaluation-workload-v1", table)
        self.assertIn("normal", table)
```

- [ ] **Step 2: Run the targeted evaluation tests to verify they fail**

Run: `python3 -m unittest tests.evaluation.test_aws_baseline_metrics.AwsBaselineMetricsTests.test_render_markdown_table_preserves_workload_metadata -v`

Expected: FAIL because the current markdown renderer does not include workload metadata

- [ ] **Step 3: Extend the AWS evaluation script to accept scenario inputs and pass workload metadata**

```bash
SCENARIO="${SCENARIO:-${2:-normal}}"
WORKLOAD_FAMILY_VERSION="${WORKLOAD_FAMILY_VERSION:-evaluation-workload-v1}"
WORKLOAD_BASE_DIR="${WORKLOAD_BASE_DIR:-data/workloads/evaluation/v1}"
WORKLOAD_FILE="${WORKLOAD_BASE_DIR}/${SCENARIO}.events.ndjson"
WORKLOAD_MANIFEST="${WORKLOAD_BASE_DIR}/${SCENARIO}.manifest.json"
```

```bash
if [[ ! -f "$WORKLOAD_FILE" ]]; then
  echo "Missing workload file: $WORKLOAD_FILE" >&2
  exit 1
fi
if [[ ! -f "$WORKLOAD_MANIFEST" ]]; then
  echo "Missing workload manifest: $WORKLOAD_MANIFEST" >&2
  exit 1
fi
```

```bash
WORKLOAD_EVENT_COUNT="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["event_count"])' "$WORKLOAD_MANIFEST")"
```

```bash
- Scenario: \`${SCENARIO}\`
- Workload family version: \`${WORKLOAD_FAMILY_VERSION}\`
- Workload file: \`${WORKLOAD_FILE}\`
- Declared input events: \`${WORKLOAD_EVENT_COUNT}\`
```

- [ ] **Step 4: Update the markdown helper test expectations and, if needed, helper code**

```python
        self.assertIn("| Workload family version | evaluation-workload-v1 |", table)
        self.assertIn("| Scenario | normal |", table)
```

- [ ] **Step 5: Verify the script and tests**

Run: `python3 -m unittest tests.evaluation.test_aws_baseline_metrics -v`

Expected: PASS

Run: `bash -n scripts/run-aws-baseline-evaluation.sh`

Expected: no output, exit code 0

- [ ] **Step 6: Commit the scenario-aware evaluation flow**

```bash
git add scripts/run-aws-baseline-evaluation.sh tests/evaluation/test_aws_baseline_metrics.py
git commit -m "feat: add scenario-aware aws evaluation flow"
```

### Task 4: Generate The Official V1 Workloads And Document Operator Flow

**Files:**
- Modify: `scripts/generate-evaluation-workloads.py`
- Modify: `docs/aws-baseline-foundation.md`
- Modify: `README.md`
- Create: `data/workloads/evaluation/v1/normal.events.ndjson`
- Create: `data/workloads/evaluation/v1/normal.manifest.json`
- Create: `data/workloads/evaluation/v1/spike.events.ndjson`
- Create: `data/workloads/evaluation/v1/spike.manifest.json`
- Create: `data/workloads/evaluation/v1/failure-recovery.events.ndjson`
- Create: `data/workloads/evaluation/v1/failure-recovery.manifest.json`

- [ ] **Step 1: Add a failing test that the generated workload directory contains all v1 outputs**

```python
    def test_main_writes_all_v1_scenarios(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            generator.main(["--output-dir", temp_dir])

            names = sorted(path.name for path in Path(temp_dir).iterdir())

        self.assertEqual(
            names,
            [
                "failure-recovery.events.ndjson",
                "failure-recovery.manifest.json",
                "normal.events.ndjson",
                "normal.manifest.json",
                "spike.events.ndjson",
                "spike.manifest.json",
            ],
        )
```

- [ ] **Step 2: Run the workload test to verify it fails**

Run: `python3 -m unittest tests.workload.test_generate_evaluation_workloads.EvaluationWorkloadGeneratorTests.test_main_writes_all_v1_scenarios -v`

Expected: FAIL because `main()` does not yet accept argv-style injection

- [ ] **Step 3: Make the generator CLI testable and generate the official artifacts**

```python
def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/workloads/evaluation/v1")
    args = parser.parse_args(argv)
    ...
```

Run: `python3 scripts/generate-evaluation-workloads.py`

Expected: creates six files under `data/workloads/evaluation/v1/`

- [ ] **Step 4: Document the operator workflow**

Add to `docs/aws-baseline-foundation.md`:

```markdown
## Official Evaluation Workloads

Generate the official workload family:

```bash
python3 scripts/generate-evaluation-workloads.py
```

Run the normal scenario:

```bash
bash scripts/run-aws-baseline-evaluation.sh "$(date -u +%Y%m%dT%H%M%SZ)" normal
```

Run the spike scenario:

```bash
bash scripts/run-aws-baseline-evaluation.sh "$(date -u +%Y%m%dT%H%M%SZ)" spike
```
```

Add to `README.md`:

```markdown
The official professor-facing evaluation workload is no longer `data/workloads/dev/events.ndjson`.
Use the versioned workload family under `data/workloads/evaluation/v1/`.
```

- [ ] **Step 5: Verify the complete workload toolchain**

Run: `python3 -m unittest tests/workload/test_generate_evaluation_workloads.py tests/evaluation/test_aws_baseline_metrics.py -v`

Expected: PASS

Run: `python3 scripts/generate-evaluation-workloads.py`

Expected: workload files regenerated successfully

Run: `bash -n scripts/run-aws-baseline-evaluation.sh`

Expected: no output, exit code 0

- [ ] **Step 6: Commit the official v1 artifacts and docs**

```bash
git add data/workloads/evaluation/v1 scripts/generate-evaluation-workloads.py docs/aws-baseline-foundation.md README.md
git commit -m "feat: add evaluation workload family v1"
```

## Self-Review

- Spec coverage:
  - workload family vs single file: covered by Tasks 1 and 2
  - versioned `evaluation-workload-v1`: covered by Tasks 1, 2, and 4
  - `normal`, `spike`, `failure-recovery` scenarios: covered by Tasks 2 and 4
  - 30 devices / 12 minutes / 6 eps / 60 eps: covered by Tasks 1 and 2
  - scenario-aware evaluation reporting: covered by Task 3
  - reproducibility metadata: covered by Tasks 2, 3, and 4
- Placeholder scan:
  - no `TODO`, `TBD`, or missing file references remain
- Type consistency:
  - generator entrypoints are consistently named `build_workload_family_metadata`, `generate_scenario`, `build_scenario_manifest`, `write_scenario_outputs`, and `main`
