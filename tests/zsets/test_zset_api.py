from concurrent.futures import ThreadPoolExecutor

from fastapi import HTTPException

from main import HashSetRequest, ZAddRequest, ZIncrByRequest, hset_value, type_value, zadd_value, zcard_value, zincrby_value, zrange_value, zrank_value, zrem_value, zrevrank_value, zscore_value
from tests.base import StoreIsolationTestCase


class ZSetApiTests(StoreIsolationTestCase):
    def test_zadd_zscore_zcard_and_type(self) -> None:
        self.assertEqual(
            zadd_value("leaderboard", "alice", ZAddRequest(score=10)).model_dump(),
            {"key": "leaderboard", "member": "alice", "added": 1, "score": 10.0},
        )
        self.assertEqual(
            zadd_value("leaderboard", "alice", ZAddRequest(score=15)).model_dump(),
            {"key": "leaderboard", "member": "alice", "added": 0, "score": 15.0},
        )

        self.assertEqual(type_value("leaderboard").model_dump(), {"key": "leaderboard", "type": "zset"})
        self.assertEqual(
            zscore_value("leaderboard", "alice").model_dump(),
            {"key": "leaderboard", "member": "alice", "score": 15.0},
        )
        self.assertEqual(zcard_value("leaderboard").model_dump(), {"key": "leaderboard", "count": 1})

    def test_zrank_zrevrank_and_zrange_ordering(self) -> None:
        zadd_value("leaderboard", "carol", ZAddRequest(score=30))
        zadd_value("leaderboard", "alice", ZAddRequest(score=10))
        zadd_value("leaderboard", "bob", ZAddRequest(score=20))

        self.assertEqual(
            zrank_value("leaderboard", "alice").model_dump(),
            {"key": "leaderboard", "member": "alice", "rank": 0},
        )
        self.assertEqual(
            zrank_value("leaderboard", "carol").model_dump(),
            {"key": "leaderboard", "member": "carol", "rank": 2},
        )
        self.assertEqual(
            zrevrank_value("leaderboard", "carol").model_dump(),
            {"key": "leaderboard", "member": "carol", "rank": 0},
        )
        self.assertEqual(
            zrange_value("leaderboard", start=0, stop=-1, order="asc").model_dump(),
            {"key": "leaderboard", "members": ["alice", "bob", "carol"]},
        )
        self.assertEqual(
            zrange_value("leaderboard", start=0, stop=1, order="desc").model_dump(),
            {"key": "leaderboard", "members": ["carol", "bob"]},
        )

    def test_zincrby_updates_score_and_zrem_removes_member(self) -> None:
        response1 = zincrby_value("leaderboard", "alice", ZIncrByRequest(increment=5))
        response2 = zincrby_value("leaderboard", "alice", ZIncrByRequest(increment=2.5))

        self.assertEqual(response1.model_dump(), {"key": "leaderboard", "member": "alice", "score": 5.0})
        self.assertEqual(response2.model_dump(), {"key": "leaderboard", "member": "alice", "score": 7.5})
        self.assertEqual(
            zscore_value("leaderboard", "alice").model_dump(),
            {"key": "leaderboard", "member": "alice", "score": 7.5},
        )
        self.assertEqual(
            zrem_value("leaderboard", "alice").model_dump(),
            {"key": "leaderboard", "member": "alice", "removed": 1},
        )
        self.assertEqual(
            zscore_value("leaderboard", "alice").model_dump(),
            {"key": "leaderboard", "member": "alice", "score": None},
        )

    def test_zscore_and_rank_return_null_for_missing_member(self) -> None:
        self.assertEqual(
            zscore_value("leaderboard", "missing").model_dump(),
            {"key": "leaderboard", "member": "missing", "score": None},
        )
        self.assertEqual(
            zrank_value("leaderboard", "missing").model_dump(),
            {"key": "leaderboard", "member": "missing", "rank": None},
        )

    def test_zincrby_is_atomic_with_concurrent_updates(self) -> None:
        request_count = 100

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [
                executor.submit(zincrby_value, "leaderboard", "alice", ZIncrByRequest(increment=1))
                for _ in range(request_count)
            ]
            results = [future.result() for future in futures]

        self.assertEqual(len(results), request_count)
        self.assertEqual(
            zscore_value("leaderboard", "alice").model_dump(),
            {"key": "leaderboard", "member": "alice", "score": 100.0},
        )

    def test_zset_wrong_type_returns_error(self) -> None:
        hset_value("name", "x", HashSetRequest(value="1"))

        with self.assertRaises(HTTPException) as context:
            zadd_value("name", "alice", ZAddRequest(score=1))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)
