# Airflow Evaluation DAG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first VacciGuard Airflow DAG so a manual trigger can launch a normal or spike evaluation run, wait for it to finish, and point the user at the generated report.

**Architecture:** Airflow will act as the orchestrator only. The DAG will shell out to the existing AWS evaluation script instead of reimplementing Spark, Kafka, or Kubernetes logic. The first version stays intentionally small: one DAG, one run command, one verification step, and a short test suite that checks the DAG structure and command contract.

**Tech Stack:** Apache Airflow, Python, BashOperator, existing AWS evaluation shell script, pytest

---

### Task 1: Create the evaluation DAG and Airflow helper docs

**Files:**
- Create: `orchestration/airflow/dags/vacciguard_evaluation_dag.py`
- Create: `orchestration/airflow/configs/README.md`

- [ ] **Step 1: Write the failing test**

Create `tests/orchestration/test_vacciguard_evaluation_dag.py` with a test that imports the DAG module and asserts the DAG exists with the expected task ids and dependencies.

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "orchestration" / "airflow" / "dags" / "vacciguard_evaluation_dag.py"
SPEC = importlib.util.spec_from_file_location("vacciguard_evaluation_dag", MODULE_PATH)
dag_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dag_module)


def test_vacciguard_evaluation_dag_structure():
    dag = dag_module.dag
    assert dag.dag_id == "vacciguard_evaluation"
    assert [task.task_id for task in dag.tasks] == [
        "run_evaluation",
        "verify_report",
    ]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pytest -q tests/orchestration/test_vacciguard_evaluation_dag.py
```

Expected: FAIL because the DAG file does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Create a DAG module that:

```python
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "vacciguard",
    "retries": 1,
}

with DAG(
    dag_id="vacciguard_evaluation",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 17, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["vacciguard", "evaluation"],
    params={
        "scenario": "normal",
        "duration_minutes": 5,
    },
) as dag:
    run_evaluation = BashOperator(
        task_id="run_evaluation",
        bash_command=(
            "cd /workspace/vacciguard && "
            "WORKLOAD_DURATION_MINUTES={{ params.duration_minutes }} "
            "bash scripts/run-aws-baseline-evaluation.sh "
            "{{ params.scenario }}-{{ ts_nodash }} "
            "{{ params.scenario }}"
        ),
    )

    verify_report = BashOperator(
        task_id="verify_report",
        bash_command=(
            "test -f "
            "artifacts/aws-baseline-evaluations/{{ params.scenario }}-{{ ts_nodash }}.json"
        ),
    )

    run_evaluation >> verify_report
```

Add `orchestration/airflow/configs/README.md` with a short note that the DAG expects the repo to be mounted at `/workspace/vacciguard` inside the Airflow worker image and that the worker needs AWS and kubectl access because the existing evaluation script uses both.

The README text should say:

```text
This folder holds Airflow runtime notes for the VacciGuard evaluation DAG.
The Airflow worker image must mount the repo at /workspace/vacciguard so the DAG can run scripts/run-aws-baseline-evaluation.sh.
The worker also needs AWS credentials and kubectl access because the evaluation script provisions Kubernetes jobs and writes reports to S3.
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
pytest -q tests/orchestration/test_vacciguard_evaluation_dag.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add orchestration/airflow/dags/vacciguard_evaluation_dag.py orchestration/airflow/configs/README.md tests/orchestration/test_vacciguard_evaluation_dag.py
git commit -m "feat: add initial airflow evaluation dag"
```

### Task 2: Wire the Airflow folder into the repo docs

**Files:**
- Modify: `README.md`
- Modify: `Project Planning/project-folder-structure.md`
- Create: `tests/evaluation/test_repo_docs.py`

- [ ] **Step 1: Write the failing test**

Add a lightweight documentation check in `tests/evaluation/test_repo_docs.py` that asserts the README mentions the new Airflow evaluation DAG path.

```python
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_readme_mentions_orchestration_airflow():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "orchestration/airflow/dags/" in readme
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
pytest -q tests/evaluation/test_repo_docs.py
```

Expected: FAIL because the documentation check does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Update `README.md` and `Project Planning/project-folder-structure.md` so they say the Airflow folder contains the evaluation DAG used to launch and verify benchmark runs. Add this exact README sentence under the repository layout section:

```text
- [orchestration/airflow/dags/](/Users/tanishgupta/Documents/GitHub/vacciguard/orchestration/airflow/dags): Airflow DAG definitions, including the VacciGuard evaluation DAG that launches benchmark runs
```

And add this exact folder-structure sentence:

```text
- `orchestration/airflow/dags/`: Airflow DAG definitions for evaluation runs and future batch workflows
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
pytest -q tests/evaluation/test_repo_docs.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add README.md Project\ Planning/project-folder-structure.md tests/evaluation/test_repo_docs.py
git commit -m "docs: describe airflow evaluation orchestration"
```
