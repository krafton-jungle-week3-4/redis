from concurrent.futures import ThreadPoolExecutor
import time
import unittest

from fastapi.testclient import TestClient

from main import app, expiry_store, redis_store


class MiniRedisTests(unittest.TestCase):
    def setUp(self) -> None:
        redis_store.clear()
        expiry_store.clear()
        self.client = TestClient(app)

    def test_increment_existing_integer_value(self) -> None:
        self.client.put("/keys/count", json={"value": "1"})

        response = self.client.post("/keys/count/increment")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["value"], 2)
        self.assertEqual(self.client.get("/keys/count").json(), {"key": "count", "value": "2"})

    def test_increment_missing_key_creates_one(self) -> None:
        response = self.client.post("/keys/count/increment")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["value"], 1)
        self.assertEqual(self.client.get("/keys/count").json(), {"key": "count", "value": "1"})

    def test_increment_non_integer_value_returns_error(self) -> None:
        self.client.put("/keys/name", json={"value": "redis"})

        response = self.client.post("/keys/name/increment")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "value is not an integer")
        self.assertEqual(self.client.get("/keys/name").json(), {"key": "name", "value": "redis"})

    def test_increment_is_thread_safe_within_single_process(self) -> None:
        request_count = 100

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(self.client.post, "/keys/count/increment") for _ in range(request_count)]
            results = [future.result() for future in futures]

        self.assertEqual(len(results), request_count)
        self.assertEqual(self.client.get("/keys/count").json(), {"key": "count", "value": "100"})

    def test_delete_existing_key_returns_one(self) -> None:
        self.client.put("/keys/name", json={"value": "redis"})

        response = self.client.delete("/keys/name")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"deleted": 1})
        self.assertEqual(self.client.get("/keys/name").json(), {"key": "name", "value": None})

    def test_decrement_existing_integer_value(self) -> None:
        self.client.put("/keys/count", json={"value": "3"})

        response = self.client.post("/keys/count/decrement")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["value"], 2)
        self.assertEqual(self.client.get("/keys/count").json(), {"key": "count", "value": "2"})

    def test_mset_and_mget(self) -> None:
        self.client.put(
            "/keys",
            json={"items": [{"key": "name", "value": "redis"}, {"key": "count", "value": "2"}]},
        )

        response = self.client.post("/keys/read", json={"keys": ["name", "count", "missing"]})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"values": ["redis", "2", None]})

    def test_exists_and_type_for_string(self) -> None:
        self.client.put("/keys/name", json={"value": "redis"})

        self.assertEqual(self.client.get("/keys/name/exists").json(), {"exists": 1})
        self.assertEqual(self.client.get("/keys/missing/exists").json(), {"exists": 0})
        self.assertEqual(self.client.get("/keys/name/type").json()["type"], "string")
        self.assertEqual(self.client.get("/keys/missing/type").json()["type"], "none")

    def test_expire_ttl_and_persist(self) -> None:
        self.client.put("/keys/session", json={"value": "abc"})

        expire_response = self.client.put("/keys/session/expiry", json={"ttl": 5})
        ttl_response = self.client.get("/keys/session/ttl")
        persist_response = self.client.delete("/keys/session/expiry")
        ttl_after_persist = self.client.get("/keys/session/ttl")

        self.assertEqual(expire_response.json(), {"updated": 1})
        self.assertTrue(0 <= ttl_response.json()["ttl"] <= 5)
        self.assertEqual(persist_response.json(), {"removed": 1})
        self.assertEqual(ttl_after_persist.json(), {"ttl": -1})

    def test_expired_key_behaves_like_missing_key(self) -> None:
        self.client.put("/keys/token", json={"value": "xyz"})
        self.client.put("/keys/token/expiry", json={"ttl": 0})
        time.sleep(0.01)

        self.assertEqual(self.client.get("/keys/token").json(), {"key": "token", "value": None})
        self.assertEqual(self.client.get("/keys/token/ttl").json(), {"ttl": -2})
        self.assertEqual(self.client.get("/keys/token/exists").json(), {"exists": 0})

    def test_list_push_and_type(self) -> None:
        self.client.post("/lists/numbers/items/left", json={"value": "2"})
        self.client.post("/lists/numbers/items/left", json={"value": "1"})
        self.client.post("/lists/numbers/items/right", json={"value": "3"})

        self.assertEqual(self.client.get("/keys/numbers/type").json()["type"], "list")
        self.assertEqual(self.client.get("/lists/numbers").json(), {"key": "numbers", "length": 3})
        self.assertEqual(
            self.client.get("/lists/numbers/items?start=0&stop=-1").json(),
            {"key": "numbers", "values": ["1", "2", "3"]},
        )

    def test_lpop_and_rpop(self) -> None:
        self.client.post("/lists/letters/items/right", json={"value": "a"})
        self.client.post("/lists/letters/items/right", json={"value": "b"})
        self.client.post("/lists/letters/items/right", json={"value": "c"})

        self.assertEqual(self.client.delete("/lists/letters/items/left").json()["value"], "a")
        self.assertEqual(self.client.delete("/lists/letters/items/right").json()["value"], "c")
        self.assertEqual(
            self.client.get("/lists/letters/items?start=0&stop=-1").json(),
            {"key": "letters", "values": ["b"]},
        )

    def test_lindex_and_lset(self) -> None:
        self.client.post("/lists/letters/items/right", json={"value": "a"})
        self.client.post("/lists/letters/items/right", json={"value": "b"})
        self.client.post("/lists/letters/items/right", json={"value": "c"})

        self.assertEqual(self.client.get("/lists/letters/items/1").json(), {"key": "letters", "index": 1, "value": "b"})
        self.assertEqual(self.client.get("/lists/letters/items/-1").json(), {"key": "letters", "index": -1, "value": "c"})

        response = self.client.put("/lists/letters/items/1", json={"value": "B"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["value"], "B")
        self.assertEqual(
            self.client.get("/lists/letters/items?start=0&stop=-1").json(),
            {"key": "letters", "values": ["a", "B", "c"]},
        )

    def test_list_wrong_type_raises_error(self) -> None:
        self.client.put("/keys/name", json={"value": "redis"})

        response = self.client.post("/lists/name/items/left", json={"value": "x"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("wrong type", response.json()["detail"])

    def test_set_add_members_and_type(self) -> None:
        self.assertEqual(self.client.put("/sets/tags/members/redis").json()["added"], 1)
        self.assertEqual(self.client.put("/sets/tags/members/python").json()["added"], 1)
        self.assertEqual(self.client.put("/sets/tags/members/redis").json()["added"], 0)

        self.assertEqual(self.client.get("/keys/tags/type").json()["type"], "set")
        self.assertEqual(self.client.get("/sets/tags").json()["count"], 2)
        self.assertEqual(self.client.get("/sets/tags/members").json()["members"], ["python", "redis"])

    def test_set_remove_and_membership(self) -> None:
        self.client.put("/sets/tags/members/redis")
        self.client.put("/sets/tags/members/python")

        self.assertEqual(self.client.get("/sets/tags/members/redis").json()["exists"], 1)
        self.assertEqual(self.client.delete("/sets/tags/members/redis").json()["removed"], 1)
        self.assertEqual(self.client.get("/sets/tags/members/redis").json()["exists"], 0)
        self.assertEqual(self.client.delete("/sets/tags/members/missing").json()["removed"], 0)

    def test_set_intersection_and_union(self) -> None:
        self.client.put("/sets/set1/members/a")
        self.client.put("/sets/set1/members/b")
        self.client.put("/sets/set2/members/b")
        self.client.put("/sets/set2/members/c")

        self.assertEqual(self.client.post("/sets/intersection", json={"keys": ["set1", "set2"]}).json()["members"], ["b"])
        self.assertEqual(self.client.post("/sets/union", json={"keys": ["set1", "set2"]}).json()["members"], ["a", "b", "c"])

    def test_set_wrong_type_raises_error(self) -> None:
        self.client.post("/lists/letters/items/right", json={"value": "a"})

        response = self.client.put("/sets/letters/members/x")

        self.assertEqual(response.status_code, 400)
        self.assertIn("wrong type", response.json()["detail"])

    def test_hash_set_get_and_type(self) -> None:
        self.assertEqual(self.client.put("/hashes/user:1/fields/name", json={"value": "redis"}).json()["added"], 1)
        self.assertEqual(self.client.put("/hashes/user:1/fields/name", json={"value": "mini-redis"}).json()["added"], 0)

        self.assertEqual(self.client.get("/keys/user:1/type").json()["type"], "hash")
        self.assertEqual(self.client.get("/hashes/user:1/fields/name").json()["value"], "mini-redis")
        self.assertEqual(self.client.get("/hashes/user:1").json()["count"], 1)

    def test_hash_delete_exists_and_getall(self) -> None:
        self.client.put("/hashes/user:1/fields/name", json={"value": "redis"})
        self.client.put("/hashes/user:1/fields/role", json={"value": "cache"})

        self.assertEqual(self.client.get("/hashes/user:1/fields/name/exists").json()["exists"], 1)
        self.assertEqual(self.client.get("/hashes/user:1/fields").json()["values"], {"name": "redis", "role": "cache"})
        self.assertEqual(self.client.delete("/hashes/user:1/fields/name").json()["removed"], 1)
        self.assertEqual(self.client.get("/hashes/user:1/fields/name/exists").json()["exists"], 0)
        self.assertEqual(self.client.delete("/hashes/user:1/fields/missing").json()["removed"], 0)

    def test_hash_increment(self) -> None:
        response1 = self.client.post("/hashes/user:1/fields/score/increment", json={"increment": 5})
        response2 = self.client.post("/hashes/user:1/fields/score/increment", json={"increment": 3})

        self.assertEqual(response1.json()["value"], 5)
        self.assertEqual(response2.json()["value"], 8)
        self.assertEqual(self.client.get("/hashes/user:1/fields/score").json()["value"], "8")

    def test_hash_wrong_type_raises_error(self) -> None:
        self.client.put("/keys/name", json={"value": "redis"})

        response = self.client.put("/hashes/name/fields/x", json={"value": "1"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("wrong type", response.json()["detail"])

    def test_zset_add_score_and_type(self) -> None:
        self.assertEqual(self.client.put("/zsets/leaderboard/members/alice", json={"score": 10}).json()["added"], 1)
        self.assertEqual(self.client.put("/zsets/leaderboard/members/alice", json={"score": 15}).json()["added"], 0)

        self.assertEqual(self.client.get("/keys/leaderboard/type").json()["type"], "zset")
        self.assertEqual(self.client.get("/zsets/leaderboard/members/alice").json()["score"], 15)
        self.assertEqual(self.client.get("/zsets/leaderboard").json()["count"], 1)

    def test_zset_rank_and_range(self) -> None:
        self.client.put("/zsets/leaderboard/members/carol", json={"score": 30})
        self.client.put("/zsets/leaderboard/members/alice", json={"score": 10})
        self.client.put("/zsets/leaderboard/members/bob", json={"score": 20})

        self.assertEqual(self.client.get("/zsets/leaderboard/members/alice/rank").json()["rank"], 0)
        self.assertEqual(self.client.get("/zsets/leaderboard/members/carol/rank").json()["rank"], 2)
        self.assertEqual(self.client.get("/zsets/leaderboard/members/carol/reverse-rank").json()["rank"], 0)
        self.assertEqual(
            self.client.get("/zsets/leaderboard/members?start=0&stop=-1&order=asc").json()["members"],
            ["alice", "bob", "carol"],
        )
        self.assertEqual(
            self.client.get("/zsets/leaderboard/members?start=0&stop=1&order=desc").json()["members"],
            ["carol", "bob"],
        )

    def test_zset_increment_and_remove(self) -> None:
        response1 = self.client.post("/zsets/leaderboard/members/alice/increment", json={"increment": 5})
        response2 = self.client.post("/zsets/leaderboard/members/alice/increment", json={"increment": 2.5})

        self.assertEqual(response1.json()["score"], 5)
        self.assertEqual(response2.json()["score"], 7.5)
        self.assertEqual(self.client.get("/zsets/leaderboard/members/alice").json()["score"], 7.5)
        self.assertEqual(self.client.delete("/zsets/leaderboard/members/alice").json()["removed"], 1)
        self.assertEqual(self.client.get("/zsets/leaderboard/members/alice").json()["score"], None)

    def test_zset_wrong_type_raises_error(self) -> None:
        self.client.put("/hashes/name/fields/x", json={"value": "1"})

        response = self.client.put("/zsets/name/members/alice", json={"score": 1})

        self.assertEqual(response.status_code, 400)
        self.assertIn("wrong type", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
