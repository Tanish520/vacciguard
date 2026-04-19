from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class AthenaDemoScriptTests(unittest.TestCase):
    def test_batch_demo_script_uses_query_execution_context(self):
        raw = (ROOT / "scripts" / "run-batch-demo.sh").read_text(encoding="utf-8")

        self.assertIn("--query-execution-context", raw)
        self.assertNotIn("--query-context", raw)

    def test_legacy_athena_demo_script_uses_query_execution_context(self):
        raw = (ROOT / "scripts" / "demo-batch-athena.sh").read_text(encoding="utf-8")

        self.assertIn("--query-execution-context", raw)
        self.assertNotIn("--query-context", raw)


if __name__ == "__main__":
    unittest.main()
