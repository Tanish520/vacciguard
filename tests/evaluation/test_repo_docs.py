from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class RepoDocumentationTests(unittest.TestCase):
    def test_readme_documents_batch_analytics_workflow(self):
        raw = (ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("## Batch Analytics Workflow", raw)
        self.assertIn("services/batch-analytics/job.py", raw)
        self.assertIn(
            "orchestration/airflow/dags/vacciguard_batch_analytics_dag.py", raw
        )
        self.assertIn("orchestration/airflow/configs/README.md", raw)
        self.assertIn("daily_compliance_summary", raw)
        self.assertIn("daily_audit_summary", raw)

    def test_project_structure_documents_batch_service_and_eval_tests(self):
        raw = (
            ROOT / "Project Planning" / "project-folder-structure.md"
        ).read_text(encoding="utf-8")

        self.assertIn("batch-analytics/", raw)
        self.assertIn("vacciguard_batch_analytics_dag.py", raw)
        self.assertIn("tests/evaluation/", raw)
        self.assertIn("documentation regression tests", raw)

    def test_airflow_config_readme_includes_manual_batch_runbook(self):
        raw = (
            ROOT / "orchestration" / "airflow" / "configs" / "README.md"
        ).read_text(encoding="utf-8")

        self.assertIn("## Manual batch analytics runbook", raw)
        self.assertIn("vacciguard_batch_analytics", raw)
        self.assertIn("processed_input", raw)
        self.assertIn("invalid_input", raw)
        self.assertIn("breach_windows_input", raw)
        self.assertIn("compliance_output", raw)
        self.assertIn("audit_output", raw)
        self.assertIn("summary.parquet", raw)


if __name__ == "__main__":
    unittest.main()
