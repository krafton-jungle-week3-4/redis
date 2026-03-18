#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import sys
import time
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
DEFAULT_COMMAND_LABEL = "python -m unittest discover -s tests -p 'test_*.py' -q"

STATUS_META = {
    "pass": {"label": "✅ 통과"},
    "fail": {"label": "❌ 실패"},
    "partial": {"label": "⚠️ 부분 반영"},
    "not_covered": {"label": "❌ 미반영"},
}


def derived_case(
    case_id: str,
    title: str,
    priority: str,
    tests: list[str],
    pass_detail: str,
    fail_detail: str | None = None,
) -> dict:
    return {
        "id": case_id,
        "title": title,
        "priority": priority,
        "mode": "derived",
        "tests": tests,
        "pass_detail": pass_detail,
        "fail_detail": fail_detail or "관련 테스트가 실패했습니다.",
    }


def static_case(case_id: str, title: str, priority: str, status: str, detail: str) -> dict:
    return {
        "id": case_id,
        "title": title,
        "priority": priority,
        "mode": "static",
        "status": status,
        "detail": detail,
    }


QA_CASES = [
    derived_case(
        "TC-INV-01",
        "삭제 후 조회 반영",
        "P2",
        [
            "test_invalidation_layer.InvalidationLayerTests.test_delete_invalidates_cached_get_result",
            "test_invalidation_layer.InvalidationLayerTests.test_type_read_is_refreshed_after_type_change",
        ],
        "별도 invalidation 레이어와 삭제 후 조회 반영 테스트를 통과했습니다.",
    ),
    derived_case(
        "TC-VER-01",
        "버전 전환",
        "P2",
        [
            "test_version_namespace.VersionNamespaceTests.test_switchver_isolates_keyspace_and_restores_previous_namespace",
            "test_version_namespace.VersionNamespaceTests.test_switchver_separates_non_string_types_too",
        ],
        "Namespace 버전 전환과 버전별 데이터 격리 테스트를 통과했습니다.",
    ),
    derived_case(
        "TC-REST-03",
        "Restore 정책",
        "P2",
        [
            "test_restore_behavior.RestoreBehaviorTests.test_restore_replace_policy_replaces_existing_data",
            "test_restore_behavior.RestoreBehaviorTests.test_restore_merge_policy_merges_snapshot_into_existing_data",
        ],
        "Restore Merge/Replace 정책 분기 테스트를 통과했습니다.",
    ),
    static_case(
        "TC-DUR-01",
        "장애 후 복구 (AOF)",
        "P2",
        "not_covered",
        "AOF 기반 내구성 기능과 테스트가 아직 없습니다.",
    ),
]


class CollectingTestResult(unittest.TextTestResult):
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.status_by_test_id: dict[str, str] = {}

    def addSuccess(self, test) -> None:  # noqa: N802
        super().addSuccess(test)
        self.status_by_test_id[test.id()] = "pass"

    def addFailure(self, test, err) -> None:  # noqa: N802
        super().addFailure(test, err)
        self.status_by_test_id[test.id()] = "fail"

    def addError(self, test, err) -> None:  # noqa: N802
        super().addError(test, err)
        self.status_by_test_id[test.id()] = "fail"

    def addSkip(self, test, reason) -> None:  # noqa: N802
        super().addSkip(test, reason)
        self.status_by_test_id[test.id()] = "skip"


def summarize_result(result: CollectingTestResult) -> str:
    if result.wasSuccessful():
        return f"Ran {result.testsRun} tests / OK"

    parts: list[str] = []
    if result.failures:
        parts.append(f"failures={len(result.failures)}")
    if result.errors:
        parts.append(f"errors={len(result.errors)}")
    if result.skipped:
        parts.append(f"skipped={len(result.skipped)}")
    detail = ", ".join(parts) if parts else "failed"
    return f"Ran {result.testsRun} tests / FAILED ({detail})"


def failed_modules(status_by_test_id: dict[str, str]) -> set[str]:
    modules: set[str] = set()
    prefix = "unittest.loader._FailedTest."
    for test_id, status in status_by_test_id.items():
        if status == "fail" and test_id.startswith(prefix):
            modules.add(test_id[len(prefix):])
    return modules


def evaluate_case(case: dict, status_by_test_id: dict[str, str], broken_modules: set[str]) -> dict:
    if case["mode"] == "static":
        status = case["status"]
        return {
            "id": case["id"],
            "title": case["title"],
            "priority": case["priority"],
            "status": status,
            "status_label": STATUS_META[status]["label"],
            "detail": case["detail"],
        }

    related_tests = case["tests"]
    discovered = [test_id for test_id in related_tests if test_id in status_by_test_id]
    related_modules = {test_id.rsplit(".", 1)[0] for test_id in related_tests}

    if broken_modules & related_modules:
        status = "fail"
        detail = "관련 테스트 모듈 import 또는 초기화에 실패했습니다."
    elif not discovered:
        status = "not_covered"
        detail = "관련 자동 테스트를 찾지 못했습니다."
    elif any(status_by_test_id[test_id] == "fail" for test_id in discovered):
        status = "fail"
        detail = case["fail_detail"]
    elif any(status_by_test_id[test_id] == "skip" for test_id in discovered):
        status = "partial"
        detail = "관련 테스트 일부가 skip 처리되었습니다."
    elif all(status_by_test_id[test_id] == "pass" for test_id in discovered):
        status = "pass"
        detail = case["pass_detail"]
    else:
        status = "partial"
        detail = "관련 테스트 결과를 완전히 판정하지 못했습니다."

    return {
        "id": case["id"],
        "title": case["title"],
        "priority": case["priority"],
        "status": status,
        "status_label": STATUS_META[status]["label"],
        "detail": detail,
    }


def build_report(result: CollectingTestResult, duration_sec: float, command_label: str) -> dict:
    generated_at_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    broken_modules = failed_modules(result.status_by_test_id)
    cases = [evaluate_case(case, result.status_by_test_id, broken_modules) for case in QA_CASES]
    counts = {key: 0 for key in STATUS_META}
    for case in cases:
        counts[case["status"]] += 1

    return {
        "title": f"Redis QA Status ({generated_at_kst[:10]} 업데이트)",
        "generated_at": generated_at_kst,
        "command": command_label,
        "tests_run": result.testsRun,
        "duration_sec": round(duration_sec, 3),
        "suite_status": "OK" if result.wasSuccessful() else "FAILED",
        "summary_text": summarize_result(result),
        "counts": counts,
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run tests and build a QA status report.")
    parser.add_argument("--start-dir", default="tests")
    parser.add_argument("--pattern", default="test_*.py")
    parser.add_argument("--results-file", default="testresult.txt")
    parser.add_argument("--report-file", default="qa_report.json")
    parser.add_argument("--command-label", default=DEFAULT_COMMAND_LABEL)
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    suite = unittest.defaultTestLoader.discover(args.start_dir, pattern=args.pattern)
    stream = io.StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=1, resultclass=CollectingTestResult)

    started = time.perf_counter()
    result = runner.run(suite)
    duration_sec = time.perf_counter() - started

    output = stream.getvalue()
    Path(args.results_file).write_text(output, encoding="utf-8")
    report = build_report(result, duration_sec, args.command_label)
    Path(args.report_file).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(output, end="")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
