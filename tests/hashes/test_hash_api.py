from fastapi import HTTPException

from main import (
    HashIncrementRequest,
    HashSetRequest,
    SetRequest,
    hdel_value,
    hexists_value,
    hget_value,
    hgetall_value,
    hlen_value,
    hincrby_value,
    hset_value,
    set_value,
    type_value,
)
from tests.base import StoreIsolationTestCase


class HashApiTests(StoreIsolationTestCase):
    def test_hset_hget_hlen_and_type(self) -> None:
        self.assertEqual(
            hset_value("user:1", "name", HashSetRequest(value="redis")).model_dump(),
            {"key": "user:1", "field": "name", "added": 1},
        )
        self.assertEqual(
            hset_value("user:1", "name", HashSetRequest(value="mini-redis")).model_dump(),
            {"key": "user:1", "field": "name", "added": 0},
        )

        self.assertEqual(type_value("user:1").model_dump(), {"key": "user:1", "type": "hash"})
        self.assertEqual(
            hget_value("user:1", "name").model_dump(),
            {"key": "user:1", "field": "name", "value": "mini-redis"},
        )
        self.assertEqual(hlen_value("user:1").model_dump(), {"key": "user:1", "count": 1})

    def test_hdel_hexists_and_hgetall_cover_existing_and_missing_fields(self) -> None:
        hset_value("user:1", "name", HashSetRequest(value="redis"))
        hset_value("user:1", "role", HashSetRequest(value="cache"))

        self.assertEqual(
            hexists_value("user:1", "name").model_dump(),
            {"key": "user:1", "field": "name", "exists": 1},
        )
        self.assertEqual(
            hgetall_value("user:1").model_dump(),
            {"key": "user:1", "values": {"name": "redis", "role": "cache"}},
        )
        self.assertEqual(
            hdel_value("user:1", "name").model_dump(),
            {"key": "user:1", "field": "name", "removed": 1},
        )
        self.assertEqual(
            hexists_value("user:1", "name").model_dump(),
            {"key": "user:1", "field": "name", "exists": 0},
        )
        self.assertEqual(
            hdel_value("user:1", "missing").model_dump(),
            {"key": "user:1", "field": "missing", "removed": 0},
        )

    def test_hincrby_creates_and_updates_numeric_field(self) -> None:
        response1 = hincrby_value("user:1", "score", HashIncrementRequest(increment=5))
        response2 = hincrby_value("user:1", "score", HashIncrementRequest(increment=3))

        self.assertEqual(response1.model_dump(), {"key": "user:1", "field": "score", "value": 5})
        self.assertEqual(response2.model_dump(), {"key": "user:1", "field": "score", "value": 8})
        self.assertEqual(
            hget_value("user:1", "score").model_dump(),
            {"key": "user:1", "field": "score", "value": "8"},
        )

    def test_hincrby_rejects_non_integer_field_value(self) -> None:
        hset_value("user:1", "score", HashSetRequest(value="redis"))

        with self.assertRaises(HTTPException) as context:
            hincrby_value("user:1", "score", HashIncrementRequest(increment=1))

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "value is not an integer")

    def test_hash_wrong_type_returns_error(self) -> None:
        set_value("name", SetRequest(value="redis"))

        with self.assertRaises(HTTPException) as context:
            hset_value("name", "x", HashSetRequest(value="1"))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)
