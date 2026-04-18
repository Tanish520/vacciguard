from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

try:
    import pendulum
except ModuleNotFoundError:
    class _PendulumFallback:
        @staticmethod
        def datetime(year: int, month: int, day: int, tz: str = "UTC") -> datetime:
            if tz != "UTC":
                raise ValueError("Pendulum fallback only supports UTC")
            return datetime(year, month, day, tzinfo=timezone.utc)

    pendulum = _PendulumFallback()

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ModuleNotFoundError:
    _CURRENT_DAG = None

    class _Task:
        def __init__(self, *, task_id: str, bash_command: str, dag: Any = None) -> None:
            self.task_id = task_id
            self.bash_command = bash_command
            self.dag = dag
            self.downstream_task_ids: set[str] = set()

        def __rshift__(self, other: Any) -> Any:
            other_task_id = getattr(other, "task_id", None)
            if other_task_id is not None:
                self.downstream_task_ids.add(other_task_id)
            return other

    class DAG:
        def __init__(
            self,
            *,
            dag_id: str,
            schedule: Any = None,
            start_date: Any = None,
            catchup: bool = False,
            default_args: dict[str, Any] | None = None,
            tags: list[str] | None = None,
            params: dict[str, Any] | None = None,
        ) -> None:
            self.dag_id = dag_id
            self.schedule = schedule
            self.start_date = start_date
            self.catchup = catchup
            self.default_args = default_args or {}
            self.tags = tags or []
            self.params = params or {}
            self.tasks: list[Any] = []

        def __enter__(self) -> "DAG":
            global _CURRENT_DAG
            _CURRENT_DAG = self
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            global _CURRENT_DAG
            _CURRENT_DAG = None

    class BashOperator(_Task):
        def __init__(self, *, task_id: str, bash_command: str) -> None:
            current_dag = _CURRENT_DAG
            super().__init__(task_id=task_id, bash_command=bash_command, dag=current_dag)
            if current_dag is not None:
                current_dag.tasks.append(self)


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
    tags=["vacciguard", "batch", "manual"],
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
