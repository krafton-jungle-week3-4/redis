import json
import tempfile
import threading
import unittest
from pathlib import Path

from redis import execute, expiry_store, hash_store, list_store, set_store, string_store, zset_store


class SnapshotCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        string_store.clear()
        set_store.clear()
        list_store.clear()
        hash_store.clear()
        zset_store.clear()
        expiry_store.clear()

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
