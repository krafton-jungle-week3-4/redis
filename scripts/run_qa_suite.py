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
        "TC-ZSET-01",
        "동일 멤버 동시 증가",
        "P0",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zincrby_is_atomic_with_large_concurrent_same_member_updates",
            "test_single_writer_concurrency.SingleWriterConcurrencyTests.test_zincrby_is_serialized_across_many_threads",
        ],
        "동시성 제어 및 점수 합산 검증 완료",
    ),
    derived_case(
        "TC-ZSET-02",
        "다중 멤버 갱신",
        "P0",
        [
            "zsets.test_zset_api.ZSetApiTests.test_concurrent_multi_member_updates_keep_expected_totals",
            "test_single_writer_concurrency.SingleWriterConcurrencyTests.test_random_multi_member_updates_preserve_total_sum",
        ],
        "여러 멤버 랜덤 갱신 및 합계 정합성 확인",
    ),
    derived_case(
        "TC-ZSET-03",
        "쓰기 후 읽기",
        "P0",
        [
            "zsets.test_zset_api.ZSetApiTests.test_write_then_read_never_returns_stale_score",
        ],
        "갱신 직후 읽기에서 stale score가 없음을 확인",
    ),
    derived_case(
        "TC-ZSET-04",
        "실시간 반영",
        "P0",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zincrby_updates_score_and_zrem_removes_member",
            "zsets.test_zset_api.ZSetApiTests.test_zrank_zrevrank_and_zrange_ordering",
        ],
        "점수 갱신 후 즉시 score/rank 결과 반영 확인",
    ),
    derived_case(
        "TC-ZSET-05",
        "동점 처리",
        "P0",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zset_uses_earlier_update_order_for_score_ties",
        ],
        "업데이트 순서를 기준으로 한 동점 정렬 로직 검증 완료",
    ),
    derived_case(
        "TC-ZSET-06",
        "완전 동점",
        "P0",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zset_tie_order_is_deterministic_across_repeated_reads",
        ],
        "완전 동점 상황에서도 결과가 결정적으로 유지됨을 확인",
    ),
    derived_case(
        "TC-TTL-01",
        "Lazy expiration",
        "P0",
        [
            "strings.test_string_state_commands.StringStateCommandTests.test_expired_string_key_behaves_like_missing_key",
        ],
        "대기 후 조회 시 null, ttl=-2, exists=0 확인",
    ),
    derived_case(
        "TC-TTL-02",
        "Background cleanup",
        "P0",
        [
            "strings.test_string_state_commands.StringStateCommandTests.test_background_cleanup_removes_expired_key_without_read",
            "zsets.test_zset_api.ZSetApiTests.test_cleanup_expired_keys_removes_expired_zset_and_auxiliary_ordering_state",
        ],
        "주기적 만료 정리와 보조 정렬 상태 제거까지 검증 완료",
    ),
    derived_case(
        "TC-TTL-03",
        "PERSIST",
        "P0",
        [
            "strings.test_string_state_commands.StringStateCommandTests.test_expire_ttl_and_persist_for_string_key",
            "strings.test_string_state_commands.StringStateCommandTests.test_persist_missing_key_returns_zero",
        ],
        "TTL 제거 및 영구 저장 전환 확인",
    ),
    derived_case(
        "TC-TTL-04",
        "TTL 값",
        "P0",
        [
            "strings.test_string_state_commands.StringStateCommandTests.test_ttl_missing_key_returns_negative_two",
            "strings.test_string_state_commands.StringStateCommandTests.test_set_clears_existing_ttl",
        ],
        "-1(무한), -2(만료/없음) 상태 값 정확성 확인",
    ),
    derived_case(
        "TC-TYPE-01",
        "타입 충돌",
        "P0",
        [
            "lists.test_list_api.ListApiTests.test_list_wrong_type_returns_error",
            "sets.test_set_api.SetApiTests.test_set_wrong_type_returns_error",
            "hashes.test_hash_api.HashApiTests.test_hash_wrong_type_returns_error",
            "zsets.test_zset_api.ZSetApiTests.test_zset_wrong_type_returns_error",
        ],
        "자료형 간 교차 호출 시 wrong type 에러 처리 확인",
    ),
    derived_case(
        "TC-RESP-01",
        "RESP 오류",
        "P0",
        [
            "protocol.test_server_protocol.ServerProtocolTests.test_malformed_resp_returns_error_and_connection_stays_alive",
        ],
        "잘못된 프로토콜 입력 시 서버 생존 및 에러 반환 확인",
    ),
    derived_case(
        "TC-SNAP-01",
        "Snapshot (Dump)",
        "P0",
        [
            "snapshot.test_snapshot_core.SnapshotCoreTests.test_snapshot_dump_writes_file",
            "snapshot.test_snapshot_core.SnapshotCoreTests.test_snapshot_includes_closed_season_state",
        ],
        "Snapshot 파일 생성과 dump 내용 검증 완료",
    ),
    derived_case(
        "TC-SNAP-02",
        "Snapshot 중 쓰기",
        "P0",
        [
            "snapshot.test_snapshot_core.SnapshotCoreTests.test_snapshot_during_writes_produces_valid_dump",
            "snapshot.test_snapshot_core.SnapshotCoreTests.test_snapshot_is_stable_after_following_writes",
        ],
        "쓰기 중 snapshot 유효성 및 dump 시점 고정성 검증 완료",
    ),
    derived_case(
        "TC-REST-01",
        "복구 (Restore)",
        "P0",
        [
            "test_restore_behavior.RestoreBehaviorTests.test_restore_recovers_data_after_cleared_state",
        ],
        "재시작 가정 후 데이터 복구 검증 완료",
    ),
    derived_case(
        "TC-REST-02",
        "복구 중 요청 처리",
        "P0",
        [
            "test_restore_behavior.RestoreBehaviorTests.test_write_requests_wait_until_restore_completes",
        ],
        "복구 중 쓰기 요청 대기 후 정상 처리 확인",
    ),
    derived_case(
        "TC-CONC-01",
        "종료 경쟁",
        "P0",
        [
            "snapshot.test_season_close_core.SeasonCloseCoreTests.test_close_season_blocks_later_writes_and_keeps_final_ranking",
            "snapshot.test_season_close_core.SeasonCloseCoreTests.test_close_season_serializes_with_concurrent_member_updates",
        ],
        "시즌 종료 시점 경쟁 처리와 종료 이후 쓰기 차단 검증 완료",
    ),
    derived_case(
        "TC-STR-01",
        "SET / GET / DEL",
        "P1",
        [
            "strings.test_string_crud_commands.StringCrudCommandTests.test_set_and_get_string_value",
            "strings.test_string_crud_commands.StringCrudCommandTests.test_delete_existing_and_missing_key",
            "strings.test_string_crud_commands.StringCrudCommandTests.test_get_missing_key_returns_none",
        ],
        "기본 String 입출력 및 삭제 확인",
    ),
    derived_case(
        "TC-STR-02",
        "INCR / INCR 에러",
        "P1",
        [
            "strings.test_string_numeric_commands.StringNumericCommandTests.test_increment_missing_key_creates_integer_string",
            "strings.test_string_numeric_commands.StringNumericCommandTests.test_increment_existing_integer_string",
            "strings.test_string_numeric_commands.StringNumericCommandTests.test_increment_non_integer_value_returns_http_400",
            "strings.test_string_numeric_commands.StringNumericCommandTests.test_decrement_missing_key_creates_negative_integer_string",
            "strings.test_string_numeric_commands.StringNumericCommandTests.test_decrement_existing_integer_string",
            "strings.test_string_numeric_commands.StringNumericCommandTests.test_decrement_non_integer_value_returns_http_400",
        ],
        "INCR/DECR 정수 연산 및 예외 처리 확인",
    ),
    derived_case(
        "TC-STR-03",
        "MSET / MGET",
        "P1",
        [
            "strings.test_string_batch_commands.StringBatchCommandTests.test_mset_and_mget_cover_existing_and_missing_keys",
            "strings.test_string_batch_commands.StringBatchCommandTests.test_mset_rejects_empty_key_before_writing_anything",
        ],
        "다중 키 동시 처리와 전체 롤백 동작 확인",
    ),
    derived_case(
        "TC-TTL-05",
        "TTL 경계",
        "P1",
        [
            "strings.test_string_state_commands.StringStateCommandTests.test_expire_zero_removes_key_immediately",
            "strings.test_string_state_commands.StringStateCommandTests.test_expire_negative_ttl_returns_http_400",
        ],
        "0 또는 음수 TTL 입력 경계 조건 처리 확인",
    ),
    derived_case(
        "TC-TTL-06",
        "TTL 갱신",
        "P1",
        [
            "strings.test_string_state_commands.StringStateCommandTests.test_last_expire_call_wins",
            "strings.test_string_batch_commands.StringBatchCommandTests.test_mset_clears_existing_ttl_for_updated_keys",
        ],
        "반복 호출 시 최신 TTL 덮어쓰기와 값 갱신 시 TTL 제거 확인",
    ),
    derived_case(
        "TC-LIST-01",
        "Push / Pop",
        "P1",
        [
            "lists.test_list_api.ListApiTests.test_push_preserves_order_and_type",
            "lists.test_list_api.ListApiTests.test_lpop_and_rpop_remove_values_from_both_sides",
            "lists.test_list_api.ListApiTests.test_pop_on_single_item_list_leaves_length_zero",
        ],
        "List 좌우 푸시 및 팝 동작 확인",
    ),
    derived_case(
        "TC-LIST-02",
        "Index / Range",
        "P1",
        [
            "lists.test_list_api.ListApiTests.test_lrange_supports_negative_indices",
            "lists.test_list_api.ListApiTests.test_lrange_returns_partial_slice_when_stop_exceeds_length",
            "lists.test_list_api.ListApiTests.test_lindex_returns_null_when_index_is_out_of_range",
        ],
        "음수 인덱스와 범위 초과 처리 확인",
    ),
    derived_case(
        "TC-RANK-01",
        "Rank / Range",
        "P1",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zrank_zrevrank_and_zrange_ordering",
            "zsets.test_zset_api.ZSetApiTests.test_zadd_zscore_zcard_and_type",
        ],
        "Sorted Set 순위 및 범위 조회 확인",
    ),
    derived_case(
        "TC-RANK-02",
        "Pagination",
        "P1",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zrange_supports_page_and_limit_pagination",
            "zsets.test_zset_api.ZSetApiTests.test_zrange_rejects_invalid_pagination_inputs",
        ],
        "page/limit 기반 pagination 시나리오 확인",
    ),
    derived_case(
        "TC-RANK-03",
        "Around (내 주변)",
        "P1",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zaround_returns_neighbors_without_boundary_errors",
            "zsets.test_zset_api.ZSetApiTests.test_zaround_rejects_negative_radius",
        ],
        "특정 멤버 기준 주변 랭킹 조회 확인",
    ),
    derived_case(
        "TC-RANK-04",
        "없는 Member",
        "P1",
        [
            "zsets.test_zset_api.ZSetApiTests.test_zaround_returns_empty_for_missing_member",
            "zsets.test_zset_api.ZSetApiTests.test_zscore_and_rank_return_null_for_missing_member",
            "zsets.test_zset_api.ZSetApiTests.test_missing_leaderboard_queries_return_empty_results",
        ],
        "존재하지 않는 멤버/리더보드 조회 시 예외 처리 확인",
    ),
    derived_case(
        "TC-INV-01",
        "삭제 후 조회 반영",
        "P2",
        [
            "test_invalidation_layer.InvalidationLayerTests.test_delete_invalidates_cached_get_result",
            "test_invalidation_layer.InvalidationLayerTests.test_type_read_is_refreshed_after_type_change",
        ],
        "삭제/타입 변경 후 캐시 무효화와 재조회 반영 검증 완료",
    ),
    derived_case(
        "TC-VER-01",
        "버전 전환",
        "P2",
        [
            "test_version_namespace.VersionNamespaceTests.test_switchver_isolates_keyspace_and_restores_previous_namespace",
            "test_version_namespace.VersionNamespaceTests.test_switchver_separates_non_string_types_too",
        ],
        "Namespace 전환 시 keyspace 분리와 재전환 복원 확인",
    ),
    derived_case(
        "TC-REST-03",
        "Restore 정책",
        "P2",
        [
            "test_restore_behavior.RestoreBehaviorTests.test_restore_replace_policy_replaces_existing_data",
            "test_restore_behavior.RestoreBehaviorTests.test_restore_merge_policy_merges_snapshot_into_existing_data",
        ],
        "replace/merge restore 정책 분기 동작 검증 완료",
    ),
    derived_case(
        "TC-DUR-01",
        "장애 후 복구 (AOF)",
        "P2",
        [
            "test_aof_durability.AofDurabilityTests.test_aof_replay_recovers_data_after_cleared_state",
            "test_aof_durability.AofDurabilityTests.test_write_requests_wait_until_aof_replay_completes",
        ],
        "AOF replay 기반 상태 복구와 복구 중 요청 대기 처리 확인",
    ),
    derived_case(
        "TC-PERF-01",
        "성능 벤치마크",
        "P2",
        [
            "test_benchmark_reports.RunBenchmarksTests.test_core_profile_generates_only_core_execute",
            "test_benchmark_reports.RunBenchmarksTests.test_network_profile_generates_only_network_e2e",
            "test_benchmark_reports.RunBenchmarksTests.test_avg_ms_over_ping_is_shared_by_report_csv_and_plot",
        ],
        "벤치마크 리포트/CSV/플롯 산출물과 Mongo 비교 경로 검증 완료",
    ),
    derived_case(
        "TC-PERF-02",
        "Hot key 부하",
        "P2",
        [
            "test_hot_key_load.HotKeyLoadTests.test_hot_key_increments_keep_exact_total_under_heavy_contention",
            "test_hot_key_load.HotKeyLoadTests.test_hot_key_reads_never_move_backward_while_writes_continue",
        ],
        "집중 부하 상황에서 최종 합계와 읽기 일관성 유지 확인",
    ),
    derived_case(
        "TC-ERR-01",
        "내부 예외 처리",
        "P2",
        [
            "protocol.test_server_protocol.ServerProtocolTests.test_internal_execute_exception_returns_error_and_next_command_still_works",
        ],
        "내부 예외 발생 시 서버 유지 및 후속 명령 처리 확인",
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
        detail = "관련 테스트 모듈 import 또는 초기화에 실패했습니다"
    elif not discovered:
        status = "not_covered"
        detail = "연결된 자동 테스트를 찾지 못했습니다"
    elif any(status_by_test_id[test_id] == "fail" for test_id in discovered):
        status = "fail"
        detail = case["fail_detail"]
    elif any(status_by_test_id[test_id] == "skip" for test_id in discovered):
        status = "partial"
        detail = "관련 테스트 일부가 skip 처리되었습니다"
    elif all(status_by_test_id[test_id] == "pass" for test_id in discovered):
        status = "pass"
        detail = case["pass_detail"]
    else:
        status = "partial"
        detail = "관련 테스트 결과를 완전히 판정하지 못했습니다"

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
        "title": f"🚀 Redis QA Status Database ({generated_at_kst[:10]} 업데이트)",
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
