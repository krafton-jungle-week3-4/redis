import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from core_state import archived_zset_store, clear_all_stores, closed_zset_keys, zset_store
from redis import execute


class SeasonCloseCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()

    def test_close_season_blocks_later_writes_and_keeps_final_ranking(self) -> None:
        execute(["ZADD", "leaderboard", "10", "alice"])

        request_count = 400
        commands = [["ZINCRBY", "leaderboard", "1", "alice"] for _ in range(request_count)]
        commands.insert(request_count // 2, ["CLOSESEASON", "leaderboard"])

        with ThreadPoolExecutor(max_workers=32) as executor:
            # 종료 명령과 점수 갱신 명령을 실제로 동시에 던져서,
            # single-writer queue가 종료 시점을 경계로 순서를 보장하는지 확인합니다.
            futures = [executor.submit(execute, command) for command in commands]
            results = [future.result() for future in futures]

        close_results = [result for result in results if result == {"type": "simple_string", "value": "OK"}]
        self.assertEqual(len(close_results), 1)

        successful_increments = [
            result for result in results if result.get("type") == "bulk_string"
        ]
        rejected_increments = [
            result for result in results if result.get("type") == "error"
        ]

        self.assertGreater(len(successful_increments), 0)
        self.assertGreater(len(rejected_increments), 0)
        self.assertIn("leaderboard", closed_zset_keys)
        self.assertNotIn("leaderboard", zset_store)

        final_score = 10 + len(successful_increments)
        self.assertEqual(
            execute(["ZSCORE", "leaderboard", "alice"]),
            {"type": "bulk_string", "value": str(final_score)},
        )
        self.assertEqual(
            execute(["ZRANGE", "leaderboard", "0", "-1"]),
            {"type": "array", "value": ["alice"]},
        )
        self.assertEqual(archived_zset_store["leaderboard"], {"alice": float(final_score)})

        after_close = execute(["ZINCRBY", "leaderboard", "1", "alice"])
        self.assertEqual(after_close["type"], "error")

    def test_close_season_serializes_with_concurrent_member_updates(self) -> None:
        execute(["ZADD", "leaderboard", "0", "alice"])
        execute(["ZADD", "leaderboard", "0", "bob"])

        stop = threading.Event()
        results: list[dict] = []

        def spam(member: str) -> None:
            while not stop.is_set():
                results.append(execute(["ZINCRBY", "leaderboard", "1", member]))

        workers = [
            threading.Thread(target=spam, args=("alice",), daemon=True),
            threading.Thread(target=spam, args=("bob",), daemon=True),
        ]
        for worker in workers:
            worker.start()

        close_result = execute(["CLOSESEASON", "leaderboard"])
        stop.set()
        for worker in workers:
            worker.join(timeout=1)

        self.assertEqual(close_result, {"type": "simple_string", "value": "OK"})
        self.assertIn("leaderboard", archived_zset_store)
        archived = archived_zset_store["leaderboard"]
        self.assertEqual(set(archived), {"alice", "bob"})
        self.assertTrue(all(score >= 0 for score in archived.values()))
        self.assertEqual(execute(["ZCARD", "leaderboard"])["value"], 2)


if __name__ == "__main__":
    unittest.main()

