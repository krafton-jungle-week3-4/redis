import unittest

from core_commands.lists import execute_list_command


class ListCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store: dict[str, object] = {}

    def test_lpush_and_rpush(self) -> None:
        self.assertEqual(execute_list_command("LPUSH", ["LPUSH", "numbers", "2"], self.store), {"type": "integer", "value": 1})
        self.assertEqual(execute_list_command("LPUSH", ["LPUSH", "numbers", "1"], self.store), {"type": "integer", "value": 2})
        self.assertEqual(execute_list_command("RPUSH", ["RPUSH", "numbers", "3"], self.store), {"type": "integer", "value": 3})
        self.assertEqual(self.store["numbers"], ["1", "2", "3"])

    def test_lpop_and_rpop(self) -> None:
        execute_list_command("RPUSH", ["RPUSH", "letters", "a"], self.store)
        execute_list_command("RPUSH", ["RPUSH", "letters", "b"], self.store)
        execute_list_command("RPUSH", ["RPUSH", "letters", "c"], self.store)

        self.assertEqual(execute_list_command("LPOP", ["LPOP", "letters"], self.store), {"type": "bulk_string", "value": "a"})
        self.assertEqual(execute_list_command("RPOP", ["RPOP", "letters"], self.store), {"type": "bulk_string", "value": "c"})
        self.assertEqual(self.store["letters"], ["b"])

    def test_lrange(self) -> None:
        for value in ["a", "b", "c", "d"]:
            execute_list_command("RPUSH", ["RPUSH", "letters", value], self.store)

        self.assertEqual(
            execute_list_command("LRANGE", ["LRANGE", "letters", "0", "-1"], self.store),
            {"type": "array", "value": ["a", "b", "c", "d"]},
        )
        self.assertEqual(
            execute_list_command("LRANGE", ["LRANGE", "letters", "1", "2"], self.store),
            {"type": "array", "value": ["b", "c"]},
        )


if __name__ == "__main__":
    unittest.main()
