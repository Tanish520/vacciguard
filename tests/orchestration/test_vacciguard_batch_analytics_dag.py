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
    assert dag.params["device_compliance_output"] == ""
    tasks = {task.task_id: task for task in dag.tasks}
    assert list(tasks) == ["run_batch_analytics", "verify_batch_outputs"]

    run_task = tasks["run_batch_analytics"]
    verify_task = tasks["verify_batch_outputs"]

    assert "cd /workspace/vacciguard" in run_task.bash_command
    assert "python3 services/batch-analytics/job.py" in run_task.bash_command
    assert "--processed-input '{{ params.processed_input }}'" in run_task.bash_command
    assert "--invalid-input '{{ params.invalid_input }}'" in run_task.bash_command
    assert "--breach-windows-input '{{ params.breach_windows_input }}'" in run_task.bash_command
    assert "--compliance-output '{{ params.compliance_output }}'" in run_task.bash_command
    assert (
        "--device-compliance-output '{{ params.device_compliance_output }}'"
        in run_task.bash_command
    )
    assert "--audit-output '{{ params.audit_output }}'" in run_task.bash_command
    assert "test -n '{{ params.compliance_output }}'" in verify_task.bash_command
    assert (
        "test -n '{{ params.device_compliance_output }}'" in verify_task.bash_command
    )
    assert "test -n '{{ params.audit_output }}'" in verify_task.bash_command

    downstream = getattr(run_task, "downstream_task_ids", None)
    if downstream is not None:
        assert "verify_batch_outputs" in downstream
