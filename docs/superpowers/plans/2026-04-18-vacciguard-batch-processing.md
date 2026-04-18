# VacciGuard Batch Processing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manually triggered Airflow workflow that runs a true storage-first batch analytics job over archived VacciGuard S3 outputs and writes compliance and audit summaries back to S3.

**Architecture:** The existing optimized hot/cold streaming services remain unchanged. Airflow acts only as the orchestrator. A new batch analytics script reads archived `processed/`, `invalid/`, and `breach_windows/` outputs, computes two historical summary datasets, writes them to dedicated S3 prefixes, and is launched by a manual-trigger Airflow DAG.

**Tech Stack:** Apache Airflow, Python, pandas or PySpark-compatible batch logic, S3 via AWS CLI / Spark-compatible readers, pytest

---

## File Structure

- Create: `orchestration/airflow/dags/vacciguard_batch_analytics_dag.py`
- Create: `services/batch-analytics/job.py`
- Create: `tests/orchestration/test_vacciguard_batch_analytics_dag.py`
- Create: `tests/batch/test_batch_analytics_job.py`
- Modify: `README.md`
- Modify: `Project Planning/project-folder-structure.md`
- Modify: `orchestration/airflow/configs/README.md`

The Airflow DAG should stay thin and orchestration-only. The batch analytics logic lives in one focused script so it can be tested without Airflow. Tests are split the same way: DAG structure tests in `tests/orchestration`, data transformation tests in `tests/batch`.

---

### Task 1: Create the batch analytics summary job

**Files:**
- Create: `services/batch-analytics/job.py`
- Test: `tests/batch/test_batch_analytics_job.py`

- [ ] **Step 1: Write the failing test**

Create `tests/batch/test_batch_analytics_job.py` with a focused transformation test that verifies the summary builders produce the expected compliance and audit fields from representative archived rows.

```python
import importlib.util
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "services" / "batch-analytics" / "job.py"
SPEC = importlib.util.spec_from_file_location("batch_analytics_job", MODULE_PATH)
batch_job = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(batch_job)


def test_build_daily_compliance_summary():
    processed = pd.DataFrame(
        [
            {
                "event_date": "2026-04-17",
                "facility_id": "fac-1",
                "facility_name": "Clinic A",
                "device_id": "dev-1",
                "temperature_c": 4.0,
                "breach_status": "safe",
            },
            {
                "event_date": "2026-04-17",
                "facility_id": "fac-1",
                "facility_name": "Clinic A",
                "device_id": "dev-2",
                "temperature_c": 9.2,
                "breach_status": "breach",
            },
        ]
    )

    summary = batch_job.build_daily_compliance_summary(processed)

    assert list(summary.columns) == [
        "event_date",
        "facility_id",
        "facility_name",
        "total_processed_events",
        "safe_events",
        "breach_events",
        "breach_rate_pct",
        "avg_temperature_c",
        "min_temperature_c",
        "max_temperature_c",
        "unique_devices_seen",
    ]
    assert summary.iloc[0]["breach_events"] == 1
    assert summary.iloc[0]["safe_events"] == 1
    assert summary.iloc[0]["unique_devices_seen"] == 2


def test_build_daily_audit_summary():
    invalid = pd.DataFrame(
        [
            {"event_date": "2026-04-17", "invalid_reason": "unknown_device"},
            {"event_date": "2026-04-17", "invalid_reason": "missing_fields"},
        ]
    )
    breach_windows = pd.DataFrame(
        [
            {"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"},
            {"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"},
            {"event_date": "2026-04-17", "facility_id": "fac-2", "device_id": "dev-2"},
        ]
    )

    summary = batch_job.build_daily_audit_summary(invalid, breach_windows)

    assert list(summary.columns) == [
        "event_date",
        "invalid_events_total",
        "invalid_unknown_device",
        "invalid_corrupt_payload",
        "invalid_missing_fields",
        "breach_window_count",
        "facilities_with_breaches",
        "devices_with_repeated_breaches",
    ]
    assert summary.iloc[0]["invalid_events_total"] == 2
    assert summary.iloc[0]["facilities_with_breaches"] == 2
    assert summary.iloc[0]["devices_with_repeated_breaches"] == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pytest -q tests/batch/test_batch_analytics_job.py
```

Expected: FAIL because `services/batch-analytics/job.py` does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Create `services/batch-analytics/job.py` with pure transformation helpers first, plus a small CLI entrypoint for later Airflow use.

```python
#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_daily_compliance_summary(processed: pd.DataFrame) -> pd.DataFrame:
    if processed.empty:
        return pd.DataFrame(
            columns=[
                "event_date",
                "facility_id",
                "facility_name",
                "total_processed_events",
                "safe_events",
                "breach_events",
                "breach_rate_pct",
                "avg_temperature_c",
                "min_temperature_c",
                "max_temperature_c",
                "unique_devices_seen",
            ]
        )

    grouped = processed.groupby(["event_date", "facility_id", "facility_name"], dropna=False)
    summary = grouped.agg(
        total_processed_events=("device_id", "size"),
        safe_events=("breach_status", lambda s: int((s == "safe").sum())),
        breach_events=("breach_status", lambda s: int((s == "breach").sum())),
        avg_temperature_c=("temperature_c", "mean"),
        min_temperature_c=("temperature_c", "min"),
        max_temperature_c=("temperature_c", "max"),
        unique_devices_seen=("device_id", "nunique"),
    ).reset_index()
    summary["breach_rate_pct"] = (
        summary["breach_events"] / summary["total_processed_events"] * 100.0
    ).round(2)
    return summary[
        [
            "event_date",
            "facility_id",
            "facility_name",
            "total_processed_events",
            "safe_events",
            "breach_events",
            "breach_rate_pct",
            "avg_temperature_c",
            "min_temperature_c",
            "max_temperature_c",
            "unique_devices_seen",
        ]
    ]


def build_daily_audit_summary(
    invalid: pd.DataFrame, breach_windows: pd.DataFrame
) -> pd.DataFrame:
    invalid_summary = (
        invalid.groupby("event_date", dropna=False)["invalid_reason"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for reason in ("unknown_device", "corrupt_payload", "missing_fields"):
        if reason not in invalid_summary.columns:
            invalid_summary[reason] = 0
    invalid_summary["invalid_events_total"] = (
        invalid_summary[["unknown_device", "corrupt_payload", "missing_fields"]].sum(axis=1)
    )

    if breach_windows.empty:
        breach_summary = pd.DataFrame(
            columns=[
                "event_date",
                "breach_window_count",
                "facilities_with_breaches",
                "devices_with_repeated_breaches",
            ]
        )
    else:
        repeated = (
            breach_windows.groupby(["event_date", "device_id"], dropna=False)
            .size()
            .reset_index(name="breach_occurrences")
        )
        repeated = repeated.groupby("event_date", dropna=False).agg(
            devices_with_repeated_breaches=("breach_occurrences", lambda s: int((s > 1).sum()))
        ).reset_index()

        breach_summary = breach_windows.groupby("event_date", dropna=False).agg(
            breach_window_count=("device_id", "size"),
            facilities_with_breaches=("facility_id", "nunique"),
        ).reset_index()
        breach_summary = breach_summary.merge(repeated, on="event_date", how="left")

    merged = invalid_summary.merge(breach_summary, on="event_date", how="outer").fillna(0)
    return merged.rename(
        columns={
            "unknown_device": "invalid_unknown_device",
            "corrupt_payload": "invalid_corrupt_payload",
            "missing_fields": "invalid_missing_fields",
        }
    )[
        [
            "event_date",
            "invalid_events_total",
            "invalid_unknown_device",
            "invalid_corrupt_payload",
            "invalid_missing_fields",
            "breach_window_count",
            "facilities_with_breaches",
            "devices_with_repeated_breaches",
        ]
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-input", required=True)
    parser.add_argument("--invalid-input", required=True)
    parser.add_argument("--breach-windows-input", required=True)
    parser.add_argument("--compliance-output", required=True)
    parser.add_argument("--audit-output", required=True)
    args = parser.parse_args()
    raise SystemExit(
        "CLI wiring is added in a later task after the summary helpers are verified."
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
pytest -q tests/batch/test_batch_analytics_job.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/batch-analytics/job.py tests/batch/test_batch_analytics_job.py
git commit -m "feat: add batch analytics summary builders"
```

### Task 2: Add archived-input loading and summary output writing

**Files:**
- Modify: `services/batch-analytics/job.py`
- Test: `tests/batch/test_batch_analytics_job.py`

- [ ] **Step 1: Write the failing test**

Extend `tests/batch/test_batch_analytics_job.py` with a CLI-level test that verifies the job writes both summaries when given local sample files.

```python
def test_run_batch_job_writes_summary_outputs(tmp_path):
    processed = pd.DataFrame(
        [
            {
                "event_date": "2026-04-17",
                "facility_id": "fac-1",
                "facility_name": "Clinic A",
                "device_id": "dev-1",
                "temperature_c": 4.0,
                "breach_status": "safe",
            }
        ]
    )
    invalid = pd.DataFrame(
        [{"event_date": "2026-04-17", "invalid_reason": "unknown_device"}]
    )
    breach_windows = pd.DataFrame(
        [{"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"}]
    )

    processed_path = tmp_path / "processed.parquet"
    invalid_path = tmp_path / "invalid.json"
    breach_path = tmp_path / "breach.json"
    compliance_output = tmp_path / "compliance"
    audit_output = tmp_path / "audit"

    processed.to_parquet(processed_path, index=False)
    invalid.to_json(invalid_path, orient="records", lines=True)
    breach_windows.to_json(breach_path, orient="records", lines=True)

    batch_job.run_batch_job(
        processed_input=str(processed_path),
        invalid_input=str(invalid_path),
        breach_windows_input=str(breach_path),
        compliance_output=str(compliance_output),
        audit_output=str(audit_output),
    )

    assert (compliance_output / "summary.parquet").exists()
    assert (audit_output / "summary.parquet").exists()
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pytest -q tests/batch/test_batch_analytics_job.py::test_run_batch_job_writes_summary_outputs
```

Expected: FAIL because `run_batch_job` is not defined.

- [ ] **Step 3: Write the minimal implementation**

Update `services/batch-analytics/job.py` to add input readers, a single orchestration function, and a real CLI.

```python
def read_processed_input(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def read_json_lines_input(path: str) -> pd.DataFrame:
    input_path = Path(path)
    if input_path.is_dir():
        frames = [pd.read_json(file_path, lines=True) for file_path in sorted(input_path.glob("*.json"))]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return pd.read_json(input_path, lines=True)


def write_summary_output(frame: pd.DataFrame, output_path: str) -> None:
    destination = Path(output_path)
    destination.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(destination / "summary.parquet", index=False)


def run_batch_job(
    *,
    processed_input: str,
    invalid_input: str,
    breach_windows_input: str,
    compliance_output: str,
    audit_output: str,
) -> None:
    processed = read_processed_input(processed_input)
    invalid = read_json_lines_input(invalid_input)
    breach_windows = read_json_lines_input(breach_windows_input)

    compliance = build_daily_compliance_summary(processed)
    audit = build_daily_audit_summary(invalid, breach_windows)

    write_summary_output(compliance, compliance_output)
    write_summary_output(audit, audit_output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-input", required=True)
    parser.add_argument("--invalid-input", required=True)
    parser.add_argument("--breach-windows-input", required=True)
    parser.add_argument("--compliance-output", required=True)
    parser.add_argument("--audit-output", required=True)
    args = parser.parse_args()
    run_batch_job(
        processed_input=args.processed_input,
        invalid_input=args.invalid_input,
        breach_windows_input=args.breach_windows_input,
        compliance_output=args.compliance_output,
        audit_output=args.audit_output,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
pytest -q tests/batch/test_batch_analytics_job.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/batch-analytics/job.py tests/batch/test_batch_analytics_job.py
git commit -m "feat: wire batch job io and outputs"
```

### Task 3: Add the manual-trigger Airflow DAG

**Files:**
- Create: `orchestration/airflow/dags/vacciguard_batch_analytics_dag.py`
- Create: `tests/orchestration/test_vacciguard_batch_analytics_dag.py`
- Modify: `orchestration/airflow/configs/README.md`

- [ ] **Step 1: Write the failing test**

Create `tests/orchestration/test_vacciguard_batch_analytics_dag.py` with a DAG-structure test.

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "orchestration" / "airflow" / "dags" / "vacciguard_batch_analytics_dag.py"
SPEC = importlib.util.spec_from_file_location("vacciguard_batch_analytics_dag", MODULE_PATH)
dag_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dag_module)


def test_vacciguard_batch_analytics_dag_structure():
    dag = dag_module.dag
    assert dag.dag_id == "vacciguard_batch_analytics"
    assert dag.schedule is None
    assert [task.task_id for task in dag.tasks] == [
        "run_batch_analytics",
        "verify_batch_outputs",
    ]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pytest -q tests/orchestration/test_vacciguard_batch_analytics_dag.py
```

Expected: FAIL because the DAG file does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Create `orchestration/airflow/dags/vacciguard_batch_analytics_dag.py`.

```python
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "vacciguard",
    "retries": 1,
}

with DAG(
    dag_id="vacciguard_batch_analytics",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 18, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["vacciguard", "batch", "airflow"],
    params={
        "processed_input": "",
        "invalid_input": "",
        "breach_windows_input": "",
        "compliance_output": "",
        "audit_output": "",
    },
) as dag:
    run_batch_analytics = BashOperator(
        task_id="run_batch_analytics",
        bash_command=(
            "cd /workspace/vacciguard && "
            "python3 services/batch-analytics/job.py "
            "--processed-input '{{ params.processed_input }}' "
            "--invalid-input '{{ params.invalid_input }}' "
            "--breach-windows-input '{{ params.breach_windows_input }}' "
            "--compliance-output '{{ params.compliance_output }}' "
            "--audit-output '{{ params.audit_output }}'"
        ),
    )

    verify_batch_outputs = BashOperator(
        task_id="verify_batch_outputs",
        bash_command=(
            "test -n '{{ params.compliance_output }}' && "
            "test -n '{{ params.audit_output }}'"
        ),
    )

    run_batch_analytics >> verify_batch_outputs
```

Update `orchestration/airflow/configs/README.md` so it explains that this DAG is manual-trigger only, expects the repo at `/workspace/vacciguard`, and requires AWS/S3 access for archived inputs and outputs.

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
pytest -q tests/orchestration/test_vacciguard_batch_analytics_dag.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add orchestration/airflow/dags/vacciguard_batch_analytics_dag.py orchestration/airflow/configs/README.md tests/orchestration/test_vacciguard_batch_analytics_dag.py
git commit -m "feat: add airflow batch analytics dag"
```

### Task 4: Make the batch job friendlier to S3-style prefixes and no-data cases

**Files:**
- Modify: `services/batch-analytics/job.py`
- Test: `tests/batch/test_batch_analytics_job.py`

- [ ] **Step 1: Write the failing test**

Add a no-data test and a directory-input test.

```python
def test_builders_handle_empty_inputs():
    compliance = batch_job.build_daily_compliance_summary(pd.DataFrame())
    audit = batch_job.build_daily_audit_summary(pd.DataFrame(), pd.DataFrame())

    assert compliance.empty
    assert audit.empty


def test_read_json_lines_input_reads_directory(tmp_path):
    first = tmp_path / "part-00000.json"
    second = tmp_path / "part-00001.json"
    pd.DataFrame([{"event_date": "2026-04-17", "invalid_reason": "unknown_device"}]).to_json(
        first, orient="records", lines=True
    )
    pd.DataFrame([{"event_date": "2026-04-17", "invalid_reason": "missing_fields"}]).to_json(
        second, orient="records", lines=True
    )

    frame = batch_job.read_json_lines_input(str(tmp_path))

    assert len(frame) == 2
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pytest -q tests/batch/test_batch_analytics_job.py::test_builders_handle_empty_inputs tests/batch/test_batch_analytics_job.py::test_read_json_lines_input_reads_directory
```

Expected: at least one FAIL because the current builder path does not safely handle missing columns in completely empty frames.

- [ ] **Step 3: Write the minimal implementation**

Update `services/batch-analytics/job.py` so empty frames are normalized before grouping.

```python
COMPLIANCE_COLUMNS = [
    "event_date",
    "facility_id",
    "facility_name",
    "device_id",
    "temperature_c",
    "breach_status",
]
AUDIT_INVALID_COLUMNS = ["event_date", "invalid_reason"]
AUDIT_BREACH_COLUMNS = ["event_date", "facility_id", "device_id"]


def ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.Series(dtype="object")
    return frame[columns]


def build_daily_compliance_summary(processed: pd.DataFrame) -> pd.DataFrame:
    processed = ensure_columns(processed.copy(), COMPLIANCE_COLUMNS)
    ...


def build_daily_audit_summary(invalid: pd.DataFrame, breach_windows: pd.DataFrame) -> pd.DataFrame:
    invalid = ensure_columns(invalid.copy(), AUDIT_INVALID_COLUMNS)
    breach_windows = ensure_columns(breach_windows.copy(), AUDIT_BREACH_COLUMNS)
    ...
```

- [ ] **Step 4: Run the full batch tests to verify they pass**

Run:

```bash
pytest -q tests/batch/test_batch_analytics_job.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/batch-analytics/job.py tests/batch/test_batch_analytics_job.py
git commit -m "fix: harden batch analytics for archived inputs"
```

### Task 5: Document the new hybrid stream-plus-batch architecture

**Files:**
- Modify: `README.md`
- Modify: `Project Planning/project-folder-structure.md`
- Test: `tests/evaluation/test_repo_docs.py`

- [ ] **Step 1: Write the failing test**

Create or extend `tests/evaluation/test_repo_docs.py` with checks for the new batch workflow paths.

```python
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_readme_mentions_batch_analytics_dag():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "vacciguard_batch_analytics" in readme
    assert "services/batch-analytics/" in readme


def test_project_structure_mentions_airflow_batch_workflow():
    structure = (ROOT / "Project Planning" / "project-folder-structure.md").read_text(
        encoding="utf-8"
    )
    assert "orchestration/airflow/dags/" in structure
    assert "future batch workflows" in structure
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pytest -q tests/evaluation/test_repo_docs.py
```

Expected: FAIL because the docs do not mention the new batch workflow yet.

- [ ] **Step 3: Write the minimal implementation**

Update `README.md` so the repository layout and architecture section mention:

```text
- `orchestration/airflow/dags/`: Airflow DAG definitions for evaluation runs and manual batch analytics workflows
- `services/batch-analytics/`: Storage-first batch analytics jobs that summarize archived S3 outputs into compliance and audit reports
```

Update `Project Planning/project-folder-structure.md` with matching language for the Airflow and batch-analytics folders.

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
pytest -q tests/evaluation/test_repo_docs.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add README.md 'Project Planning/project-folder-structure.md' tests/evaluation/test_repo_docs.py
git commit -m "docs: describe batch analytics workflow"
```

### Task 6: Manual verification for the demo path

**Files:**
- Modify: `orchestration/airflow/configs/README.md`

- [ ] **Step 1: Add an operator runbook snippet**

Update `orchestration/airflow/configs/README.md` with a short manual-trigger example that points the DAG at a completed optimized evaluation prefix, for example:

```text
Example manual trigger parameters:
- processed_input: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/processed/
- invalid_input: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/invalid/
- breach_windows_input: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/breach_windows/
- compliance_output: s3://vacciguard-tanish-baseline-ap-south-1-data/batch-analytics/demo/daily_compliance_summary/
- audit_output: s3://vacciguard-tanish-baseline-ap-south-1-data/batch-analytics/demo/daily_audit_summary/
```

- [ ] **Step 2: Run the targeted doc test**

Run:

```bash
pytest -q tests/evaluation/test_repo_docs.py
```

Expected: PASS and no doc regressions.

- [ ] **Step 3: Commit**

```bash
git add orchestration/airflow/configs/README.md
git commit -m "docs: add batch analytics airflow runbook"
```

---

## Self-Review

- Spec coverage:
  - manual Airflow workflow: covered in Task 3
  - storage-first batch job over archived S3 data: covered in Tasks 1, 2, and 4
  - compliance and audit summaries: covered in Tasks 1 and 2
  - docs and demo support: covered in Tasks 5 and 6
  - no changes to streaming hot/cold path: enforced by scope and file list
- Placeholder scan:
  - no `TODO`, `TBD`, or unresolved task references remain
- Type consistency:
  - batch functions consistently use `pd.DataFrame`
  - output names match the approved spec and DAG params

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-18-vacciguard-batch-processing.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
