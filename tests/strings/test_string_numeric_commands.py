from fastapi import HTTPException

from main import decrement_value, get_value, increment_value, set_value, SetRequest
from tests.base import StoreIsolationTestCase


class StringNumericCommandTests(StoreIsolationTestCase):
    def test_increment_missing_key_creates_integer_string(self) -> None:
        response = increment_value("count")

        self.assertEqual(response.model_dump(), {"result": "OK", "key": "count", "value": 1})
        self.assertEqual(get_value("count").model_dump(), {"key": "count", "value": "1"})

    def test_increment_existing_integer_string(self) -> None:
        set_value("count", SetRequest(value="10"))

        self.assertEqual(increment_value("count").model_dump(), {"result": "OK", "key": "count", "value": 11})
        self.assertEqual(get_value("count").model_dump(), {"key": "count", "value": "11"})

    def test_increment_non_integer_value_returns_http_400(self) -> None:
        set_value("name", SetRequest(value="redis"))

        with self.assertRaises(HTTPException) as context:
            increment_value("name")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "value is not an integer")

    def test_decrement_missing_key_creates_negative_integer_string(self) -> None:
        response = decrement_value("count")

        self.assertEqual(response.model_dump(), {"result": "OK", "key": "count", "value": -1})
        self.assertEqual(get_value("count").model_dump(), {"key": "count", "value": "-1"})

    def test_decrement_existing_integer_string(self) -> None:
        set_value("count", SetRequest(value="10"))

        self.assertEqual(decrement_value("count").model_dump(), {"result": "OK", "key": "count", "value": 9})
        self.assertEqual(get_value("count").model_dump(), {"key": "count", "value": "9"})

    def test_decrement_non_integer_value_returns_http_400(self) -> None:
        set_value("name", SetRequest(value="redis"))

        with self.assertRaises(HTTPException) as context:
            decrement_value("name")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "value is not an integer")
