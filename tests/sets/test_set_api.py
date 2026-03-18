from fastapi import HTTPException

from main import ListPushRequest, SetKeysRequest, sadd_value, scard_value, set_value, sinter_value, sismember_value, smembers_value, srem_value, sunion_value, type_value, rpush_value, SetRequest
from tests.base import StoreIsolationTestCase


class SetApiTests(StoreIsolationTestCase):
    def test_sadd_smembers_scard_and_type(self) -> None:
        self.assertEqual(sadd_value("tags", "redis").model_dump(), {"added": 1, "key": "tags"})
        self.assertEqual(sadd_value("tags", "python").model_dump(), {"added": 1, "key": "tags"})
        self.assertEqual(sadd_value("tags", "redis").model_dump(), {"added": 0, "key": "tags"})

        self.assertEqual(type_value("tags").model_dump(), {"key": "tags", "type": "set"})
        self.assertEqual(scard_value("tags").model_dump(), {"key": "tags", "count": 2})
        self.assertEqual(smembers_value("tags").model_dump(), {"key": "tags", "members": ["python", "redis"]})

    def test_srem_and_sismember_handle_existing_and_missing_members(self) -> None:
        sadd_value("tags", "redis")
        sadd_value("tags", "python")

        self.assertEqual(
            sismember_value("tags", "redis").model_dump(),
            {"key": "tags", "member": "redis", "exists": 1},
        )
        self.assertEqual(srem_value("tags", "redis").model_dump(), {"removed": 1, "key": "tags"})
        self.assertEqual(
            sismember_value("tags", "redis").model_dump(),
            {"key": "tags", "member": "redis", "exists": 0},
        )
        self.assertEqual(srem_value("tags", "missing").model_dump(), {"removed": 0, "key": "tags"})

    def test_sinter_and_sunion_return_expected_members(self) -> None:
        sadd_value("set1", "a")
        sadd_value("set1", "b")
        sadd_value("set2", "b")
        sadd_value("set2", "c")

        self.assertEqual(
            sinter_value(SetKeysRequest(keys=["set1", "set2"])).model_dump(),
            {"keys": ["set1", "set2"], "members": ["b"]},
        )
        self.assertEqual(
            sunion_value(SetKeysRequest(keys=["set1", "set2"])).model_dump(),
            {"keys": ["set1", "set2"], "members": ["a", "b", "c"]},
        )

    def test_set_wrong_type_returns_error(self) -> None:
        rpush_value("letters", ListPushRequest(value="a"))

        with self.assertRaises(HTTPException) as context:
            sadd_value("letters", "x")

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)
