from fastapi import HTTPException

from main import MGetRequest, MSetItem, MSetRequest, get_value, mget_values, mset_values
from tests.base import StoreIsolationTestCase


class StringBatchCommandTests(StoreIsolationTestCase):
    def test_mset_and_mget_cover_existing_and_missing_keys(self) -> None:
        response = mset_values(
            MSetRequest(
                items=[
                    MSetItem(key="name", value="redis"),
                    MSetItem(key="count", value="2"),
                ]
            )
        )

        self.assertEqual(response.model_dump(), {"result": "OK", "count": 2})
        self.assertEqual(
            mget_values(MGetRequest(keys=["name", "count", "missing"])).model_dump(),
            {"values": ["redis", "2", None]},
        )

    def test_mset_rejects_empty_key_before_writing_anything(self) -> None:
        with self.assertRaises(HTTPException) as context:
            mset_values(
                MSetRequest(
                    items=[
                        MSetItem(key="name", value="redis"),
                        MSetItem(key="", value="invalid"),
                    ]
                )
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "key must not be empty")
        self.assertEqual(get_value("name").model_dump(), {"key": "name", "value": None})
