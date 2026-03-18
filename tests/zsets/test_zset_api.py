from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import random

from fastapi import HTTPException

from main import (
    HashSetRequest,
    ZAddRequest,
    ZIncrByRequest,
    cleanup_expired_keys,
    expiry_store,
    hset_value,
    type_value,
    zadd_value,
    zaround_value,
    zcard_value,
    zincrby_value,
    zrange_value,
    zrank_value,
    zrem_value,
    zrevrank_value,
    zscore_value,
    zset_order_store,
)
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

    def test_zset_uses_earlier_update_order_for_score_ties(self) -> None:
        zadd_value("leaderboard", "alice", ZAddRequest(score=100))
        zadd_value("leaderboard", "bob", ZAddRequest(score=100))
        zadd_value("leaderboard", "carol", ZAddRequest(score=90))

        self.assertEqual(
            zrange_value("leaderboard", start=0, stop=-1, order="desc").model_dump(),
            {"key": "leaderboard", "members": ["alice", "bob", "carol"]},
        )

    def test_zset_tie_order_is_deterministic_across_repeated_reads(self) -> None:
        zadd_value("leaderboard", "alice", ZAddRequest(score=100))
        zadd_value("leaderboard", "bob", ZAddRequest(score=100))
        zadd_value("leaderboard", "carol", ZAddRequest(score=100))

        expected = {"key": "leaderboard", "members": ["alice", "bob", "carol"]}
        for _ in range(5):
            self.assertEqual(zrange_value("leaderboard", start=0, stop=-1, order="desc").model_dump(), expected)

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

    def test_zrange_supports_page_and_limit_pagination(self) -> None:
        for member, score in [
            ("erin", 50),
            ("dave", 40),
            ("carol", 30),
            ("bob", 20),
            ("alice", 10),
        ]:
            zadd_value("leaderboard", member, ZAddRequest(score=score))

        self.assertEqual(
            zrange_value("leaderboard", order="desc", page=2, limit=2).model_dump(),
            {"key": "leaderboard", "members": ["carol", "bob"]},
        )

    def test_zaround_returns_neighbors_without_boundary_errors(self) -> None:
        for member, score in [
            ("erin", 50),
            ("dave", 40),
            ("carol", 30),
            ("bob", 20),
            ("alice", 10),
        ]:
            zadd_value("leaderboard", member, ZAddRequest(score=score))

        self.assertEqual(
            zaround_value("leaderboard", "carol", radius=1, order="desc").model_dump(),
            {"key": "leaderboard", "member": "carol", "members": ["dave", "carol", "bob"]},
        )
        self.assertEqual(
            zaround_value("leaderboard", "erin", radius=2, order="desc").model_dump(),
            {"key": "leaderboard", "member": "erin", "members": ["erin", "dave", "carol"]},
        )

    def test_zaround_returns_empty_for_missing_member(self) -> None:
        zadd_value("leaderboard", "alice", ZAddRequest(score=10))

        self.assertEqual(
            zaround_value("leaderboard", "missing", radius=2).model_dump(),
            {"key": "leaderboard", "member": "missing", "members": []},
        )

    def test_zaround_rejects_negative_radius(self) -> None:
        with self.assertRaises(HTTPException) as context:
            zaround_value("leaderboard", "alice", radius=-1)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "radius must be non-negative")

    def test_zrange_rejects_invalid_pagination_inputs(self) -> None:
        zadd_value("leaderboard", "alice", ZAddRequest(score=10))

        with self.assertRaises(HTTPException) as missing_limit:
            zrange_value("leaderboard", page=1)

        self.assertEqual(missing_limit.exception.status_code, 400)
        self.assertEqual(missing_limit.exception.detail, "page and limit must be used together")

        with self.assertRaises(HTTPException) as non_positive:
            zrange_value("leaderboard", page=0, limit=10)

        self.assertEqual(non_positive.exception.status_code, 400)
        self.assertEqual(non_positive.exception.detail, "page and limit must be positive")

    def test_missing_leaderboard_queries_return_empty_results(self) -> None:
        self.assertEqual(
            zrange_value("missing", order="desc", page=1, limit=3).model_dump(),
            {"key": "missing", "members": []},
        )
        self.assertEqual(
            zaround_value("missing", "alice", radius=2).model_dump(),
            {"key": "missing", "member": "alice", "members": []},
        )
        self.assertEqual(zcard_value("missing").model_dump(), {"key": "missing", "count": 0})

    def test_cleanup_expired_keys_removes_expired_zset_and_auxiliary_ordering_state(self) -> None:
        zadd_value("leaderboard", "alice", ZAddRequest(score=10))
        expiry_store["leaderboard"] = 0

        removed = cleanup_expired_keys()

        self.assertEqual(removed, 1)
        self.assertEqual(zcard_value("leaderboard").model_dump(), {"key": "leaderboard", "count": 0})
        self.assertNotIn("leaderboard", zset_order_store)

    def test_zincrby_is_atomic_with_large_concurrent_same_member_updates(self) -> None:
        worker_count = 100
        increments_per_worker = 100

        def worker() -> None:
            for _ in range(increments_per_worker):
                zincrby_value("leaderboard", "alice", ZIncrByRequest(increment=1))

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(worker) for _ in range(worker_count)]
            results = [future.result() for future in futures]

        self.assertEqual(len(results), worker_count)
        self.assertEqual(
            zscore_value("leaderboard", "alice").model_dump(),
            {"key": "leaderboard", "member": "alice", "score": 10000.0},
        )

    def test_concurrent_multi_member_updates_keep_expected_totals(self) -> None:
        members = ["alice", "bob", "carol", "dave", "erin"]
        worker_count = 50
        increments_per_worker = 100

        def worker(seed: int) -> Counter[str]:
            rng = random.Random(seed)
            local_counts: Counter[str] = Counter()
            for _ in range(increments_per_worker):
                member = rng.choice(members)
                zincrby_value("leaderboard", member, ZIncrByRequest(increment=1))
                local_counts[member] += 1
            return local_counts

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(worker, index) for index in range(worker_count)]
            totals = Counter()
            for future in futures:
                totals.update(future.result())

        self.assertEqual(sum(totals.values()), worker_count * increments_per_worker)
        observed_total = 0
        for member in members:
            score = zscore_value("leaderboard", member).score
            if score is not None:
                self.assertEqual(score, float(totals[member]))
                observed_total += int(score)
        self.assertEqual(observed_total, worker_count * increments_per_worker)

    def test_write_then_read_never_returns_stale_score(self) -> None:
        request_count = 100

        def worker() -> tuple[float, float]:
            written = zincrby_value("leaderboard", "alice", ZIncrByRequest(increment=1)).score
            read_back = zscore_value("leaderboard", "alice").score
            return written, read_back if read_back is not None else -1.0

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = [future.result() for future in [executor.submit(worker) for _ in range(request_count)]]

        for written, read_back in results:
            self.assertGreaterEqual(read_back, written)

    def test_zset_wrong_type_returns_error(self) -> None:
        hset_value("name", "x", HashSetRequest(value="1"))

        with self.assertRaises(HTTPException) as context:
            zadd_value("name", "alice", ZAddRequest(score=1))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)
