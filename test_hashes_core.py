import unittest

from core_commands.hashes import execute_hash_command


class HashCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.string_store: dict[str, str] = {}
        self.set_store: dict[str, set[str]] = {}
        self.list_store: dict[str, list[str]] = {}
        self.hash_store: dict[str, dict[str, str]] = {}

    def test_hset_and_hget(self) -> None:
        created = execute_hash_command(
            "HSET",
            ["HSET", "user:1", "name", "redis"],
            self.string_store,
            self.set_store,
            self.list_store,
            self.hash_store,
        )
        fetched = execute_hash_command(
            "HGET",
            ["HGET", "user:1", "name"],
            self.string_store,
            self.set_store,
            self.list_store,
            self.hash_store,
        )

        self.assertEqual(created, {"type": "integer", "value": 1})
        self.assertEqual(fetched, {"type": "bulk_string", "value": "redis"})

    def test_hdel_hexists_hlen(self) -> None:
        execute_hash_command("HSET", ["HSET", "user:1", "name", "redis"], self.string_store, self.set_store, self.list_store, self.hash_store)
        execute_hash_command("HSET", ["HSET", "user:1", "score", "10"], self.string_store, self.set_store, self.list_store, self.hash_store)

        exists_before = execute_hash_command("HEXISTS", ["HEXISTS", "user:1", "name"], self.string_store, self.set_store, self.list_store, self.hash_store)
        length_before = execute_hash_command("HLEN", ["HLEN", "user:1"], self.string_store, self.set_store, self.list_store, self.hash_store)
        deleted = execute_hash_command("HDEL", ["HDEL", "user:1", "name"], self.string_store, self.set_store, self.list_store, self.hash_store)
        exists_after = execute_hash_command("HEXISTS", ["HEXISTS", "user:1", "name"], self.string_store, self.set_store, self.list_store, self.hash_store)

        self.assertEqual(exists_before, {"type": "integer", "value": 1})
        self.assertEqual(length_before, {"type": "integer", "value": 2})
        self.assertEqual(deleted, {"type": "integer", "value": 1})
        self.assertEqual(exists_after, {"type": "integer", "value": 0})

    def test_hgetall_and_hincrby(self) -> None:
        execute_hash_command("HSET", ["HSET", "user:1", "name", "redis"], self.string_store, self.set_store, self.list_store, self.hash_store)
        incremented = execute_hash_command(
            "HINCRBY",
            ["HINCRBY", "user:1", "score", "5"],
            self.string_store,
            self.set_store,
            self.list_store,
            self.hash_store,
        )
        all_fields = execute_hash_command(
            "HGETALL",
            ["HGETALL", "user:1"],
            self.string_store,
            self.set_store,
            self.list_store,
            self.hash_store,
        )

        self.assertEqual(incremented, {"type": "integer", "value": 5})
        self.assertEqual(all_fields, {"type": "array", "value": ["name", "redis", "score", "5"]})


if __name__ == "__main__":
    unittest.main()
