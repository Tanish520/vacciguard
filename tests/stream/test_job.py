import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).resolve().parents[2] / "services" / "stream-processor" / "job.py"
SPEC = importlib.util.spec_from_file_location("stream_job", MODULE_PATH)
stream_job = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(stream_job)


class BatchSummaryTests(unittest.TestCase):
    def test_summarize_batch_counts_sets_all_expected_fields(self):
        summary = stream_job.summarize_batch_counts(
            batch_id=7,
            valid_count=9,
            invalid_count=3,
            deduplicated_count=2,
            breach_count=4,
        )

        self.assertEqual(summary["batch_id"], 7)
        self.assertEqual(summary["valid_count"], 9)
        self.assertEqual(summary["invalid_count"], 3)
        self.assertEqual(summary["deduplicated_count"], 2)
        self.assertEqual(summary["breach_count"], 4)


if __name__ == "__main__":
    unittest.main()
