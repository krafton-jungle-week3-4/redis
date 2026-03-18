import time

from main import (
    ExpireRequest,
    SetRequest,
    exists_value,
    expire_value,
    get_value,
    persist_value,
    set_value,
    ttl_value,
    type_value,
)
from tests.base import StoreIsolationTestCase


class StringStateCommandTests(StoreIsolationTestCase):
    def test_exists_and_type_for_string_and_missing_key(self) -> None:
        set_value("name", SetRequest(value="redis"))

        self.assertEqual(exists_value("name").model_dump(), {"exists": 1})
        self.assertEqual(exists_value("missing").model_dump(), {"exists": 0})
        self.assertEqual(type_value("name").model_dump(), {"key": "name", "type": "string"})
        self.assertEqual(type_value("missing").model_dump(), {"key": "missing", "type": "none"})

    def test_expire_ttl_and_persist_for_string_key(self) -> None:
        set_value("session", SetRequest(value="abc"))

        expire_response = expire_value("session", ExpireRequest(ttl=5))
        ttl_response = ttl_value("session")
        persist_response = persist_value("session")
        ttl_after_persist = ttl_value("session")

        self.assertEqual(expire_response.model_dump(), {"updated": 1})
        self.assertTrue(0 <= ttl_response.ttl <= 5)
        self.assertEqual(persist_response.model_dump(), {"removed": 1})
        self.assertEqual(ttl_after_persist.model_dump(), {"ttl": -1})

    def test_expire_missing_key_returns_zero(self) -> None:
        response = expire_value("missing", ExpireRequest(ttl=5))

        self.assertEqual(response.model_dump(), {"updated": 0})

    def test_ttl_missing_key_returns_negative_two(self) -> None:
        self.assertEqual(ttl_value("missing").model_dump(), {"ttl": -2})

    def test_expired_string_key_behaves_like_missing_key(self) -> None:
        set_value("token", SetRequest(value="xyz"))
        expire_value("token", ExpireRequest(ttl=1))

        time.sleep(1.05)

        self.assertEqual(get_value("token").model_dump(), {"key": "token", "value": None})
        self.assertEqual(ttl_value("token").model_dump(), {"ttl": -2})
        self.assertEqual(exists_value("token").model_dump(), {"exists": 0})

    def test_persist_missing_key_returns_zero(self) -> None:
        self.assertEqual(persist_value("missing").model_dump(), {"removed": 0})
