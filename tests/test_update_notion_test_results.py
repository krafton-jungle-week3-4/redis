import unittest

from scripts.run_qa_suite import QA_CASES
from scripts.update_notion_test_results import validate_report


def build_minimal_report(case_ids: list[str]) -> dict:
    return {
        "cases": [
            {
                "id": case_id,
                "title": case_id,
                "status_label": "✅ 통과",
                "detail": "ok",
                "priority": "P0",
            }
            for case_id in case_ids
        ]
    }


class UpdateNotionTestResultsTests(unittest.TestCase):
    def test_validate_report_accepts_full_expected_case_set(self) -> None:
        report = build_minimal_report([case["id"] for case in QA_CASES])

        validate_report(report)

    def test_validate_report_rejects_missing_cases(self) -> None:
        case_ids = [case["id"] for case in QA_CASES[:-1]]
        report = build_minimal_report(case_ids)

        with self.assertRaises(RuntimeError) as context:
            validate_report(report)

        self.assertIn("missing=", str(context.exception))
        self.assertIn(QA_CASES[-1]["id"], str(context.exception))
