from concurrent.futures import ThreadPoolExecutor
import time
import unittest

from fastapi import HTTPException

from main import (
    GetResponse,
    LIndexResponse,
    LSetRequest,
    LLenResponse,
    LRangeResponse,
    ListPushRequest,
    MGetResponse,
    MSetItem,
    MSetRequest,
    SetRequest,
    TtlResponse,
    decrement_value,
    delete_value,
    exists_value,
    expire_value,
    expiry_store,
    get_value,
    increment_value,
    lindex_value,
    llen_value,
    lpop_value,
    lpush_value,
    lrange_value,
    lset_value,
    mget_values,
    mset_values,
    persist_value,
    redis_store,
    rpop_value,
    rpush_value,
    set_value,
    sadd_value,
    scard_value,
    sinter_value,
    sismember_value,
    smembers_value,
    srem_value,
    sunion_value,
    SetMemberRequest,
    ttl_value,
    type_value,
    HashSetRequest,
    HashIncrementRequest,
    hset_value,
    hget_value,
    hdel_value,
    hgetall_value,
    hexists_value,
    hincrby_value,
    hlen_value,
    ZAddRequest,
    ZIncrByRequest,
    zadd_value,
    zscore_value,
    zrank_value,
    zrevrank_value,
    zrange_value,
    zrevrange_value,
    zincrby_value,
    zrem_value,
    zcard_value,
)


class MiniRedisTests(unittest.TestCase):
    def setUp(self) -> None:
        redis_store.clear()
        expiry_store.clear()

    def test_increment_existing_integer_value(self) -> None:
        set_value(SetRequest(key="count", value="1"))

        response = increment_value("count")

        self.assertEqual(response.value, 2)
        self.assertEqual(get_value("count"), GetResponse(key="count", value="2"))

    def test_increment_missing_key_creates_one(self) -> None:
        response = increment_value("count")

        self.assertEqual(response.value, 1)
        self.assertEqual(get_value("count"), GetResponse(key="count", value="1"))

    def test_increment_non_integer_value_returns_error(self) -> None:
        set_value(SetRequest(key="name", value="redis"))

        with self.assertRaises(HTTPException) as context:
            increment_value("name")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "value is not an integer")
        self.assertEqual(get_value("name"), GetResponse(key="name", value="redis"))

    def test_increment_is_thread_safe_within_single_process(self) -> None:
        request_count = 100

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(increment_value, "count") for _ in range(request_count)]
            results = [future.result() for future in futures]

        self.assertEqual(len(results), request_count)
        self.assertEqual(get_value("count"), GetResponse(key="count", value="100"))

    def test_delete_existing_key_returns_one(self) -> None:
        set_value(SetRequest(key="name", value="redis"))

        response = delete_value("name")

        self.assertEqual(response.deleted, 1)
        self.assertEqual(get_value("name"), GetResponse(key="name", value=None))

    def test_decrement_existing_integer_value(self) -> None:
        set_value(SetRequest(key="count", value="3"))

        response = decrement_value("count")

        self.assertEqual(response.value, 2)
        self.assertEqual(get_value("count"), GetResponse(key="count", value="2"))

    def test_mset_and_mget(self) -> None:
        mset_values(
            MSetRequest(
                items=[
                    MSetItem(key="name", value="redis"),
                    MSetItem(key="count", value="2"),
                ]
            )
        )

        response = mget_values(["name", "count", "missing"])

        self.assertEqual(response, MGetResponse(values=["redis", "2", None]))

    def test_exists_and_type_for_string(self) -> None:
        set_value(SetRequest(key="name", value="redis"))

        self.assertEqual(exists_value("name").exists, 1)
        self.assertEqual(exists_value("missing").exists, 0)
        self.assertEqual(type_value("name").type, "string")
        self.assertEqual(type_value("missing").type, "none")

    def test_expire_ttl_and_persist(self) -> None:
        set_value(SetRequest(key="session", value="abc"))

        expire_response = expire_value("session", 5)
        ttl_response = ttl_value("session")
        persist_response = persist_value("session")
        ttl_after_persist = ttl_value("session")

        self.assertEqual(expire_response.updated, 1)
        self.assertTrue(0 <= ttl_response.ttl <= 5)
        self.assertEqual(persist_response.removed, 1)
        self.assertEqual(ttl_after_persist, TtlResponse(ttl=-1))

    def test_expired_key_behaves_like_missing_key(self) -> None:
        set_value(SetRequest(key="token", value="xyz"))
        expire_value("token", 0)
        time.sleep(0.01)

        self.assertEqual(get_value("token"), GetResponse(key="token", value=None))
        self.assertEqual(ttl_value("token"), TtlResponse(ttl=-2))
        self.assertEqual(exists_value("token").exists, 0)

    def test_list_push_and_type(self) -> None:
        lpush_value("numbers", ListPushRequest(value="2"))
        lpush_value("numbers", ListPushRequest(value="1"))
        rpush_value("numbers", ListPushRequest(value="3"))

        self.assertEqual(type_value("numbers").type, "list")
        self.assertEqual(llen_value("numbers"), LLenResponse(key="numbers", length=3))
        self.assertEqual(lrange_value("numbers", 0, -1), LRangeResponse(key="numbers", values=["1", "2", "3"]))

    def test_lpop_and_rpop(self) -> None:
        rpush_value("letters", ListPushRequest(value="a"))
        rpush_value("letters", ListPushRequest(value="b"))
        rpush_value("letters", ListPushRequest(value="c"))

        self.assertEqual(lpop_value("letters").value, "a")
        self.assertEqual(rpop_value("letters").value, "c")
        self.assertEqual(lrange_value("letters", 0, -1), LRangeResponse(key="letters", values=["b"]))

    def test_lindex_and_lset(self) -> None:
        rpush_value("letters", ListPushRequest(value="a"))
        rpush_value("letters", ListPushRequest(value="b"))
        rpush_value("letters", ListPushRequest(value="c"))

        self.assertEqual(lindex_value("letters", 1), LIndexResponse(key="letters", index=1, value="b"))
        self.assertEqual(lindex_value("letters", -1), LIndexResponse(key="letters", index=-1, value="c"))

        response = lset_value("letters", 1, LSetRequest(value="B"))

        self.assertEqual(response.value, "B")
        self.assertEqual(lrange_value("letters", 0, -1), LRangeResponse(key="letters", values=["a", "B", "c"]))

    def test_list_wrong_type_raises_error(self) -> None:
        set_value(SetRequest(key="name", value="redis"))

        with self.assertRaises(HTTPException) as context:
            lpush_value("name", ListPushRequest(value="x"))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)

    def test_set_add_members_and_type(self) -> None:
        self.assertEqual(sadd_value("tags", SetMemberRequest(member="redis")).added, 1)
        self.assertEqual(sadd_value("tags", SetMemberRequest(member="python")).added, 1)
        self.assertEqual(sadd_value("tags", SetMemberRequest(member="redis")).added, 0)

        self.assertEqual(type_value("tags").type, "set")
        self.assertEqual(scard_value("tags").count, 2)
        self.assertEqual(smembers_value("tags").members, ["python", "redis"])

    def test_set_remove_and_membership(self) -> None:
        sadd_value("tags", SetMemberRequest(member="redis"))
        sadd_value("tags", SetMemberRequest(member="python"))

        self.assertEqual(sismember_value("tags", "redis").exists, 1)
        self.assertEqual(srem_value("tags", SetMemberRequest(member="redis")).removed, 1)
        self.assertEqual(sismember_value("tags", "redis").exists, 0)
        self.assertEqual(srem_value("tags", SetMemberRequest(member="missing")).removed, 0)

    def test_set_intersection_and_union(self) -> None:
        sadd_value("set1", SetMemberRequest(member="a"))
        sadd_value("set1", SetMemberRequest(member="b"))
        sadd_value("set2", SetMemberRequest(member="b"))
        sadd_value("set2", SetMemberRequest(member="c"))

        self.assertEqual(sinter_value(["set1", "set2"]).members, ["b"])
        self.assertEqual(sunion_value(["set1", "set2"]).members, ["a", "b", "c"])

    def test_set_wrong_type_raises_error(self) -> None:
        rpush_value("letters", ListPushRequest(value="a"))

        with self.assertRaises(HTTPException) as context:
            sadd_value("letters", SetMemberRequest(member="x"))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)

    def test_hash_set_get_and_type(self) -> None:
        self.assertEqual(hset_value("user:1", HashSetRequest(field="name", value="redis")).added, 1)
        self.assertEqual(hset_value("user:1", HashSetRequest(field="name", value="mini-redis")).added, 0)

        self.assertEqual(type_value("user:1").type, "hash")
        self.assertEqual(hget_value("user:1", "name").value, "mini-redis")
        self.assertEqual(hlen_value("user:1").count, 1)

    def test_hash_delete_exists_and_getall(self) -> None:
        hset_value("user:1", HashSetRequest(field="name", value="redis"))
        hset_value("user:1", HashSetRequest(field="role", value="cache"))

        self.assertEqual(hexists_value("user:1", "name").exists, 1)
        self.assertEqual(hgetall_value("user:1").values, {"name": "redis", "role": "cache"})
        self.assertEqual(hdel_value("user:1", "name").removed, 1)
        self.assertEqual(hexists_value("user:1", "name").exists, 0)
        self.assertEqual(hdel_value("user:1", "missing").removed, 0)

    def test_hash_increment(self) -> None:
        response1 = hincrby_value("user:1", "score", HashIncrementRequest(increment=5))
        response2 = hincrby_value("user:1", "score", HashIncrementRequest(increment=3))

        self.assertEqual(response1.value, 5)
        self.assertEqual(response2.value, 8)
        self.assertEqual(hget_value("user:1", "score").value, "8")

    def test_hash_wrong_type_raises_error(self) -> None:
        set_value(SetRequest(key="name", value="redis"))

        with self.assertRaises(HTTPException) as context:
            hset_value("name", HashSetRequest(field="x", value="1"))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)

    def test_zset_add_score_and_type(self) -> None:
        self.assertEqual(zadd_value("leaderboard", ZAddRequest(score=10, member="alice")).added, 1)
        self.assertEqual(zadd_value("leaderboard", ZAddRequest(score=15, member="alice")).added, 0)

        self.assertEqual(type_value("leaderboard").type, "zset")
        self.assertEqual(zscore_value("leaderboard", "alice").score, 15)
        self.assertEqual(zcard_value("leaderboard").count, 1)

    def test_zset_rank_and_range(self) -> None:
        zadd_value("leaderboard", ZAddRequest(score=30, member="carol"))
        zadd_value("leaderboard", ZAddRequest(score=10, member="alice"))
        zadd_value("leaderboard", ZAddRequest(score=20, member="bob"))

        self.assertEqual(zrank_value("leaderboard", "alice").rank, 0)
        self.assertEqual(zrank_value("leaderboard", "carol").rank, 2)
        self.assertEqual(zrevrank_value("leaderboard", "carol").rank, 0)
        self.assertEqual(zrange_value("leaderboard", 0, -1).members, ["alice", "bob", "carol"])
        self.assertEqual(zrevrange_value("leaderboard", 0, 1).members, ["carol", "bob"])

    def test_zset_increment_and_remove(self) -> None:
        response1 = zincrby_value("leaderboard", ZIncrByRequest(increment=5, member="alice"))
        response2 = zincrby_value("leaderboard", ZIncrByRequest(increment=2.5, member="alice"))

        self.assertEqual(response1.score, 5)
        self.assertEqual(response2.score, 7.5)
        self.assertEqual(zscore_value("leaderboard", "alice").score, 7.5)
        self.assertEqual(zrem_value("leaderboard", "alice").removed, 1)
        self.assertEqual(zscore_value("leaderboard", "alice").score, None)

    def test_zset_wrong_type_raises_error(self) -> None:
        hset_value("name", HashSetRequest(field="x", value="1"))

        with self.assertRaises(HTTPException) as context:
            zadd_value("name", ZAddRequest(score=1, member="alice"))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)


if __name__ == "__main__":
    unittest.main()
