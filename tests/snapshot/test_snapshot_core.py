import json
import tempfile
import threading
import unittest
from pathlib import Path

from core_state import clear_all_stores
from redis import execute


class SnapshotCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()

    def test_snapshot_dump_writes_file(self) -> None:
        execute(["SET", "name", "redis"])
        execute(["SADD", "tags", "python"])

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir) / "dump.json"
            response = execute(["SNAPSHOT", str(snapshot_path)])
            self.assertEqual(response["type"], "bulk_string")
            self.assertEqual(Path(response["value"]), snapshot_path)
            self.assertTrue(snapshot_path.exists())

            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["strings"]["name"], "redis")
            self.assertIn("python", payload["sets"]["tags"])

    def test_snapshot_is_stable_after_following_writes(self) -> None:
        execute(["SET", "counter", "1"])

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir) / "stable.json"
            execute(["DUMP", str(snapshot_path)])
            execute(["SET", "counter", "999"])

            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            # Snapshot should preserve value at the dump point.
            self.assertEqual(payload["strings"]["counter"], "1")

    def test_snapshot_includes_closed_season_state(self) -> None:
        execute(["ZADD", "leaderboard", "10", "alice"])
        execute(["CLOSESEASON", "leaderboard"])

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir) / "season.json"
            execute(["SNAPSHOT", str(snapshot_path)])

            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["archived_zsets"]["leaderboard"], {"alice": 10.0})
            self.assertEqual(payload["closed_zsets"], ["leaderboard"])

    def test_snapshot_during_writes_produces_valid_dump(self) -> None:
        stop = False

        def writer() -> None:
            i = 0
            while not stop:
                execute(["SET", "hotkey", str(i)])
                i += 1

        thread = threading.Thread(target=writer, daemon=True)
        thread.start()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                snapshot_path = Path(tmpdir) / "concurrent.json"
                response = execute(["SNAPSHOT", str(snapshot_path)])
                self.assertEqual(response["type"], "bulk_string")

                payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
                self.assertIn("strings", payload)
                if "hotkey" in payload["strings"]:
                    self.assertTrue(payload["strings"]["hotkey"].isdigit())
        finally:
            stop = True
            thread.join(timeout=1)


if __name__ == "__main__":
    unittest.main()
