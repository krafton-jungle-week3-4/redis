from fastapi import HTTPException

from main import ExpireRequest, MGetRequest, MSetItem, MSetRequest, expire_value, get_value, mget_values, mset_values, ttl_value
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

    def test_mset_clears_existing_ttl_for_updated_keys(self) -> None:
        mset_values(
            MSetRequest(
                items=[
                    MSetItem(key="name", value="redis"),
                    MSetItem(key="count", value="1"),
                ]
            )
        )
        expire_value("name", ExpireRequest(ttl=5))
        expire_value("count", ExpireRequest(ttl=5))

        response = mset_values(
            MSetRequest(
                items=[
                    MSetItem(key="name", value="mini-redis"),
                    MSetItem(key="count", value="2"),
                ]
            )
        )

        self.assertEqual(response.model_dump(), {"result": "OK", "count": 2})
        self.assertEqual(ttl_value("name").model_dump(), {"ttl": -1})
        self.assertEqual(ttl_value("count").model_dump(), {"ttl": -1})
        self.assertEqual(get_value("name").model_dump(), {"key": "name", "value": "mini-redis"})
        self.assertEqual(get_value("count").model_dump(), {"key": "count", "value": "2"})
