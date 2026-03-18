import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path

from core.core_state import clear_all_stores
from managers.aof_manager import get_aof_path, replay_aof, reset_aof, set_aof_path
from managers.invalidation_manager import debug_cache_snapshot, invalidate_all
from managers.restore_manager import restore_snapshot, save_snapshot
from redis import execute
from server import handle_client_connection


class FakeSocket:
    def __init__(self, payload: bytes) -> None:
        self._reader = BytesIO(payload)
        self.written = bytearray()
        self.closed = False

    def makefile(self, mode: str) -> BytesIO:
        return self._reader

    def sendall(self, data: bytes) -> None:
        self.written.extend(data)

    def close(self) -> None:
        self.closed = True


class DemoScenarioTests(unittest.TestCase):
    """발표 시연용 핵심 테스트 6개를 한 파일에 모아둔 테스트 모음입니다."""

    def setUp(self) -> None:
        clear_all_stores()
        invalidate_all()
        self._previous_aof_path = get_aof_path()

    def tearDown(self) -> None:
        set_aof_path(self._previous_aof_path)

    def test_01_single_writer_queue_keeps_zincrby_consistent(self) -> None:
        """동시성 제어: 같은 key에 요청이 몰려도 최종 점수가 정확해야 합니다."""

        request_count = 1200

        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [
                executor.submit(execute, ["ZINCRBY", "leaderboard", "1", "alice"])
                for _ in range(request_count)
            ]
            for future in futures:
                future.result()

        self.assertEqual(
            execute(["ZSCORE", "leaderboard", "alice"]),
            {"type": "bulk_string", "value": str(request_count)},
        )

    def test_02_expired_value_behaves_like_missing_key(self) -> None:
        """TTL 처리: 만료된 값은 조회 시점에 없는 값처럼 동작해야 합니다."""

        execute(["SET", "token", "xyz"])
        execute(["EXPIRE", "token", "1"])

        time.sleep(1.05)

        self.assertEqual(execute(["GET", "token"]), {"type": "null", "value": None})
        self.assertEqual(execute(["TTL", "token"]), {"type": "integer", "value": -2})
        self.assertEqual(execute(["EXISTS", "token"]), {"type": "integer", "value": 0})

    def test_03_malformed_resp_returns_error_and_keeps_processing(self) -> None:
        """외부 사용 구조: 잘못된 RESP 입력에도 서버가 오류를 반환하고 다음 명령을 처리합니다."""

        fake_socket = FakeSocket(b"*1\r\n$-1\r\nPING\n")

        def fake_execute(command: list[str]) -> dict:
            if command == ["PING"]:
                return {"type": "simple_string", "value": "PONG"}
            return {"type": "error", "value": "unexpected command"}

        handle_client_connection(fake_socket, fake_execute)

        self.assertEqual(
            fake_socket.written.decode("utf-8"),
            "-RESP bulk string length must be non-negative\r\n+PONG\r\n",
        )
        self.assertTrue(fake_socket.closed)

    def test_04_delete_invalidates_cached_get_result(self) -> None:
        """무효화: 삭제 후에는 캐시에 남은 예전 조회 결과가 보이면 안 됩니다."""

        execute(["SET", "name", "redis"])
        self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
        self.assertIn("name", debug_cache_snapshot())

        self.assertEqual(execute(["DEL", "name"]), {"type": "integer", "value": 1})
        self.assertNotIn("name", debug_cache_snapshot())
        self.assertEqual(execute(["GET", "name"]), {"type": "null", "value": None})

    def test_05_aof_replay_recovers_data_after_cleared_state(self) -> None:
        """내구성: 메모리를 비운 뒤에도 AOF replay로 상태를 복구할 수 있어야 합니다."""

        with tempfile.TemporaryDirectory() as tmp_dir:
            aof_path = Path(tmp_dir) / "appendonly.aof"
            set_aof_path(aof_path)
            reset_aof(aof_path)

            execute(["SET", "name", "redis"])
            execute(["HSET", "user:1", "score", "10"])
            execute(["ZADD", "leaderboard", "30", "alice"])
            execute(["EXPIRE", "name", "30"])

            clear_all_stores()
            self.assertEqual(execute(["GET", "name"]), {"type": "null", "value": None})

            replay_aof(aof_path)

            self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
            self.assertEqual(execute(["HGET", "user:1", "score"]), {"type": "bulk_string", "value": "10"})
            self.assertEqual(execute(["ZSCORE", "leaderboard", "alice"]), {"type": "bulk_string", "value": "30"})

    def test_06_write_requests_wait_until_restore_completes(self) -> None:
        """추가 데모: snapshot 복구 중 들어온 요청은 복구가 끝날 때까지 대기해야 합니다."""

        execute(["SET", "name", "redis"])

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot.json"
            save_snapshot(snapshot_path)
            clear_all_stores()

            restore_thread = threading.Thread(
                target=restore_snapshot,
                args=(snapshot_path,),
                kwargs={"delay_sec": 0.25},
            )
            restore_thread.start()
            time.sleep(0.05)

            started = time.perf_counter()
            response = execute(["SET", "pending", "ready"])
            elapsed = time.perf_counter() - started
            restore_thread.join()

            self.assertGreaterEqual(elapsed, 0.15)
            self.assertEqual(response, {"type": "simple_string", "value": "OK"})
            self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
            self.assertEqual(execute(["GET", "pending"]), {"type": "bulk_string", "value": "ready"})


class DemoTextResult(unittest.TextTestResult):
    def startTest(self, test: unittest.case.TestCase) -> None:
        super().startTest(test)
        title = test.id().split(".")[-1]
        description = test.shortDescription() or "설명 없음"
        self.stream.writeln("\n" + "=" * 72)
        self.stream.writeln(f"[DEMO] {title}")
        self.stream.writeln(f"설명: {description}")
        self.stream.writeln("-" * 72)

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self.stream.writeln("결과: PASS")

    def addFailure(self, test: unittest.case.TestCase, err) -> None:
        super().addFailure(test, err)
        self.stream.writeln("결과: FAIL")

    def addError(self, test: unittest.case.TestCase, err) -> None:
        super().addError(test, err)
        self.stream.writeln("결과: ERROR")


class DemoTextRunner(unittest.TextTestRunner):
    resultclass = DemoTextResult


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(DemoScenarioTests)
    runner = DemoTextRunner(verbosity=0)
    runner.run(suite)
