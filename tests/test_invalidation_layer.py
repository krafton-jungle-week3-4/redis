import unittest

from invalidation_manager import debug_cache_snapshot, invalidate_all
from redis import execute, expiry_store, hash_store, list_store, set_store, string_store, zset_store


class InvalidationLayerTests(unittest.TestCase):
    def setUp(self) -> None:
        string_store.clear()
        set_store.clear()
        list_store.clear()
        hash_store.clear()
        zset_store.clear()
        expiry_store.clear()
        invalidate_all()

    def test_delete_invalidates_cached_get_result(self) -> None:
        execute(["SET", "name", "redis"])
        self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
        self.assertIn("name", debug_cache_snapshot())

        self.assertEqual(execute(["DEL", "name"]), {"type": "integer", "value": 1})
        self.assertNotIn("name", debug_cache_snapshot())
        self.assertEqual(execute(["GET", "name"]), {"type": "null", "value": None})

    def test_type_read_is_refreshed_after_type_change(self) -> None:
        execute(["SET", "k", "v"])
        self.assertEqual(execute(["TYPE", "k"]), {"type": "bulk_string", "value": "string"})

        execute(["DEL", "k"])
        execute(["SADD", "k", "member"])

        self.assertEqual(execute(["TYPE", "k"]), {"type": "bulk_string", "value": "set"})


if __name__ == "__main__":
    unittest.main()
