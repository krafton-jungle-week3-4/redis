from fastapi import HTTPException

from main import (
    LSetRequest,
    ListPushRequest,
    SetRequest,
    lindex_value,
    llen_value,
    lpop_value,
    lpush_value,
    lrange_value,
    lset_value,
    rpop_value,
    rpush_value,
    set_value,
    type_value,
)
from tests.base import StoreIsolationTestCase


class ListApiTests(StoreIsolationTestCase):
    def test_push_preserves_order_and_type(self) -> None:
        lpush_value("numbers", ListPushRequest(value="2"))
        lpush_value("numbers", ListPushRequest(value="1"))
        rpush_value("numbers", ListPushRequest(value="3"))

        self.assertEqual(type_value("numbers").model_dump(), {"key": "numbers", "type": "list"})
        self.assertEqual(llen_value("numbers").model_dump(), {"key": "numbers", "length": 3})
        self.assertEqual(
            lrange_value("numbers", start=0, stop=-1).model_dump(),
            {"key": "numbers", "values": ["1", "2", "3"]},
        )

    def test_lpop_and_rpop_remove_values_from_both_sides(self) -> None:
        rpush_value("letters", ListPushRequest(value="a"))
        rpush_value("letters", ListPushRequest(value="b"))
        rpush_value("letters", ListPushRequest(value="c"))

        self.assertEqual(lpop_value("letters").model_dump(), {"key": "letters", "value": "a"})
        self.assertEqual(rpop_value("letters").model_dump(), {"key": "letters", "value": "c"})
        self.assertEqual(
            lrange_value("letters", start=0, stop=-1).model_dump(),
            {"key": "letters", "values": ["b"]},
        )

    def test_pop_on_single_item_list_leaves_length_zero(self) -> None:
        rpush_value("solo", ListPushRequest(value="only"))

        self.assertEqual(lpop_value("solo").model_dump(), {"key": "solo", "value": "only"})
        self.assertEqual(llen_value("solo").model_dump(), {"key": "solo", "length": 0})
        self.assertEqual(
            lrange_value("solo", start=0, stop=-1).model_dump(),
            {"key": "solo", "values": []},
        )

    def test_lrange_supports_negative_indices(self) -> None:
        for value in ["a", "b", "c", "d"]:
            rpush_value("letters", ListPushRequest(value=value))

        self.assertEqual(
            lrange_value("letters", start=-2, stop=-1).model_dump(),
            {"key": "letters", "values": ["c", "d"]},
        )

    def test_lrange_returns_partial_slice_when_stop_exceeds_length(self) -> None:
        for value in ["a", "b", "c"]:
            rpush_value("numbers", ListPushRequest(value=value))

        self.assertEqual(
            lrange_value("numbers", start=1, stop=10).model_dump(),
            {"key": "numbers", "values": ["b", "c"]},
        )

    def test_lindex_returns_null_when_index_is_out_of_range(self) -> None:
        rpush_value("letters", ListPushRequest(value="a"))

        self.assertEqual(
            lindex_value("letters", 5).model_dump(),
            {"key": "letters", "index": 5, "value": None},
        )

    def test_lset_updates_existing_index(self) -> None:
        for value in ["a", "b", "c"]:
            rpush_value("letters", ListPushRequest(value=value))

        response = lset_value("letters", 1, LSetRequest(value="B"))

        self.assertEqual(response.model_dump(), {"result": "OK", "key": "letters", "index": 1, "value": "B"})
        self.assertEqual(
            lrange_value("letters", start=0, stop=-1).model_dump(),
            {"key": "letters", "values": ["a", "B", "c"]},
        )

    def test_lset_returns_error_for_missing_key(self) -> None:
        with self.assertRaises(HTTPException) as context:
            lset_value("missing", 0, LSetRequest(value="x"))

        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.detail, "no such key")

    def test_lset_returns_error_for_out_of_range_index(self) -> None:
        rpush_value("letters", ListPushRequest(value="a"))

        with self.assertRaises(HTTPException) as context:
            lset_value("letters", 3, LSetRequest(value="x"))

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "index out of range")

    def test_list_wrong_type_returns_error(self) -> None:
        set_value("name", SetRequest(value="redis"))

        with self.assertRaises(HTTPException) as context:
            lpush_value("name", ListPushRequest(value="x"))

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("wrong type", context.exception.detail)
