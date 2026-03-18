import tempfile
import threading
import time
import unittest
from pathlib import Path

from core_state import clear_all_stores
from redis import execute
from restore_manager import restore_snapshot, save_snapshot


class RestoreBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()

    def test_restore_recovers_data_after_cleared_state(self) -> None:
        execute(["SET", "name", "redis"])
        execute(["HSET", "user:1", "score", "10"])
        execute(["ZADD", "leaderboard", "30", "alice"])

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot.json"
            save_snapshot(snapshot_path)

            clear_all_stores()
            self.assertEqual(execute(["GET", "name"]), {"type": "null", "value": None})

            restore_snapshot(snapshot_path)

            self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
            self.assertEqual(execute(["HGET", "user:1", "score"]), {"type": "bulk_string", "value": "10"})
            self.assertEqual(execute(["ZSCORE", "leaderboard", "alice"]), {"type": "bulk_string", "value": "30"})

    def test_write_requests_wait_until_restore_completes(self) -> None:
        execute(["SET", "name", "redis"])

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot.json"
            save_snapshot(snapshot_path)
            clear_all_stores()

            restore_thread = threading.Thread(
                target=restore_snapshot,
                args=(snapshot_path,),
                kwargs={"delay_sec": 0.25},
            )
            restore_thread.start()
            time.sleep(0.05)

            started = time.perf_counter()
            response = execute(["SET", "pending", "ready"])
            elapsed = time.perf_counter() - started
            restore_thread.join()

            self.assertGreaterEqual(elapsed, 0.15)
            self.assertEqual(response, {"type": "simple_string", "value": "OK"})
            self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
            self.assertEqual(execute(["GET", "pending"]), {"type": "bulk_string", "value": "ready"})

    def test_restore_replace_policy_replaces_existing_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot-replace.json"

            execute(["SET", "same", "from-snapshot"])
            execute(["SET", "new-only", "from-snapshot"])
            save_snapshot(snapshot_path)

            execute(["SET", "same", "live-now"])
            execute(["SET", "old-only", "live-now"])

            restore_snapshot(snapshot_path, policy="replace")

            self.assertEqual(execute(["GET", "same"]), {"type": "bulk_string", "value": "from-snapshot"})
            self.assertEqual(execute(["GET", "new-only"]), {"type": "bulk_string", "value": "from-snapshot"})
            self.assertEqual(execute(["GET", "old-only"]), {"type": "null", "value": None})

    def test_restore_merge_policy_merges_snapshot_into_existing_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "snapshot-merge.json"

            execute(["SET", "same", "from-snapshot"])
            execute(["SET", "incoming", "from-snapshot"])
            save_snapshot(snapshot_path)

            execute(["SET", "same", "live-updated"])
            execute(["SET", "stay", "live-updated"])

            restore_snapshot(snapshot_path, policy="merge")

            self.assertEqual(execute(["GET", "same"]), {"type": "bulk_string", "value": "from-snapshot"})
            self.assertEqual(execute(["GET", "incoming"]), {"type": "bulk_string", "value": "from-snapshot"})
            self.assertEqual(execute(["GET", "stay"]), {"type": "bulk_string", "value": "live-updated"})


if __name__ == "__main__":
    unittest.main()
