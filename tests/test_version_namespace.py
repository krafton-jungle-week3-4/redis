import unittest

from core.core_state import clear_all_stores
from redis import execute


class VersionNamespaceTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()

    def test_switchver_isolates_keyspace_and_restores_previous_namespace(self) -> None:
        self.assertEqual(execute(["CURRENTVER"]), {"type": "bulk_string", "value": "default"})

        execute(["SET", "name", "alice"])
        self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "alice"})

        self.assertEqual(execute(["SWITCHVER", "v2"]), {"type": "simple_string", "value": "OK"})
        self.assertEqual(execute(["CURRENTVER"]), {"type": "bulk_string", "value": "v2"})
        self.assertEqual(execute(["GET", "name"]), {"type": "null", "value": None})

        execute(["SET", "name", "bob"])
        self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "bob"})

        self.assertEqual(execute(["SWITCHVER", "default"]), {"type": "simple_string", "value": "OK"})
        self.assertEqual(execute(["CURRENTVER"]), {"type": "bulk_string", "value": "default"})
        self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "alice"})

    def test_switchver_separates_non_string_types_too(self) -> None:
        execute(["SADD", "tags", "python"])
        execute(["ZADD", "leaderboard", "10", "alice"])

        execute(["SWITCHVER", "v2"])
        self.assertEqual(execute(["SMEMBERS", "tags"]), {"type": "array", "value": []})
        self.assertEqual(execute(["ZSCORE", "leaderboard", "alice"]), {"type": "null", "value": None})

        execute(["SADD", "tags", "redis"])
        execute(["ZADD", "leaderboard", "20", "bob"])

        execute(["SWITCHVER", "default"])
        self.assertEqual(execute(["SMEMBERS", "tags"]), {"type": "array", "value": ["python"]})
        self.assertEqual(execute(["ZSCORE", "leaderboard", "alice"]), {"type": "bulk_string", "value": "10"})


if __name__ == "__main__":
    unittest.main()
