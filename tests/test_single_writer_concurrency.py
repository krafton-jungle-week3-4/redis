import random
import unittest
from concurrent.futures import ThreadPoolExecutor

from redis import execute, hash_store, list_store, set_store, string_store, zset_store


class SingleWriterConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        string_store.clear()
        set_store.clear()
        list_store.clear()
        hash_store.clear()
        zset_store.clear()

    def test_incr_is_serialized_across_many_threads(self) -> None:
        request_count = 2000

        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [executor.submit(execute, ["INCR", "counter"]) for _ in range(request_count)]
            for future in futures:
                future.result()

        self.assertEqual(execute(["GET", "counter"]), {"type": "bulk_string", "value": str(request_count)})

    def test_hincrby_is_serialized_across_many_threads(self) -> None:
        request_count = 1500

        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [
                executor.submit(execute, ["HINCRBY", "user:1", "score", "1"])
                for _ in range(request_count)
            ]
            for future in futures:
                future.result()

        self.assertEqual(
            execute(["HGET", "user:1", "score"]),
            {"type": "bulk_string", "value": str(request_count)},
        )

    def test_zincrby_is_serialized_across_many_threads(self) -> None:
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

    def test_random_multi_member_updates_preserve_total_sum(self) -> None:
        members = ["alice", "bob", "carol", "dave"]
        random.seed(7)
        commands: list[list[str]] = []
        expected_totals = {member: 0 for member in members}

        for _ in range(2000):
            member = random.choice(members)
            increment = random.randint(1, 3)
            commands.append(["ZINCRBY", "leaderboard", str(increment), member])
            expected_totals[member] += increment

        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [executor.submit(execute, command) for command in commands]
            for future in futures:
                future.result()

        actual_total = 0
        for member, expected in expected_totals.items():
            response = execute(["ZSCORE", "leaderboard", member])
            self.assertEqual(response, {"type": "bulk_string", "value": str(expected)})
            actual_total += expected

        ranked_members = execute(["ZRANGE", "leaderboard", "0", "-1"])
        self.assertEqual(ranked_members["type"], "array")
        self.assertEqual(sorted(ranked_members["value"]), sorted(members))
        self.assertEqual(sum(expected_totals.values()), actual_total)


if __name__ == "__main__":
    unittest.main()
