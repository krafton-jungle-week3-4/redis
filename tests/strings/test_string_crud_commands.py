from main import delete_value, get_value, set_value, SetRequest
from tests.base import StoreIsolationTestCase


class StringCrudCommandTests(StoreIsolationTestCase):
    def test_set_and_get_string_value(self) -> None:
        set_response = set_value("name", SetRequest(value="redis"))
        get_response = get_value("name")

        self.assertEqual(set_response.model_dump(), {"result": "OK", "key": "name", "value": "redis"})
        self.assertEqual(get_response.model_dump(), {"key": "name", "value": "redis"})

    def test_set_overwrites_existing_string_value(self) -> None:
        set_value("name", SetRequest(value="redis"))
        set_value("name", SetRequest(value="mini-redis"))

        self.assertEqual(get_value("name").model_dump(), {"key": "name", "value": "mini-redis"})

    def test_set_allows_empty_string_value(self) -> None:
        set_value("empty", SetRequest(value=""))

        self.assertEqual(get_value("empty").model_dump(), {"key": "empty", "value": ""})

    def test_get_missing_key_returns_none(self) -> None:
        self.assertEqual(get_value("missing").model_dump(), {"key": "missing", "value": None})

    def test_delete_existing_and_missing_key(self) -> None:
        set_value("name", SetRequest(value="redis"))

        self.assertEqual(delete_value("name").model_dump(), {"deleted": 1})
        self.assertEqual(delete_value("name").model_dump(), {"deleted": 0})
        self.assertEqual(get_value("name").model_dump(), {"key": "name", "value": None})
