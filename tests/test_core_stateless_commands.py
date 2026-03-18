import unittest

from core_state import clear_all_stores
from redis import execute


class CoreStatelessCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()

    def test_ping_returns_pong(self) -> None:
        self.assertEqual(execute(["PING"]), {"type": "simple_string", "value": "PONG"})


if __name__ == "__main__":
    unittest.main()
