import tempfile
import threading
import time
import unittest
from pathlib import Path

from aof_manager import get_aof_path, replay_aof, reset_aof, set_aof_path
from core_state import clear_all_stores
from redis import execute


class AofDurabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()
        self._previous_aof_path = get_aof_path()

    def tearDown(self) -> None:
        set_aof_path(self._previous_aof_path)

    def test_aof_replay_recovers_data_after_cleared_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            aof_path = Path(tmp_dir) / "appendonly.aof"
            set_aof_path(aof_path)
            reset_aof(aof_path)

            execute(["SET", "name", "redis"])
            execute(["HSET", "user:1", "score", "10"])
            execute(["ZADD", "leaderboard", "30", "alice"])
            execute(["EXPIRE", "name", "30"])

            clear_all_stores()
            self.assertEqual(execute(["GET", "name"]), {"type": "null", "value": None})

            replay_aof(aof_path)

            self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
            self.assertEqual(execute(["HGET", "user:1", "score"]), {"type": "bulk_string", "value": "10"})
            self.assertEqual(execute(["ZSCORE", "leaderboard", "alice"]), {"type": "bulk_string", "value": "30"})
            self.assertGreaterEqual(execute(["TTL", "name"])["value"], 0)

    def test_write_requests_wait_until_aof_replay_completes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            aof_path = Path(tmp_dir) / "appendonly.aof"
            set_aof_path(aof_path)
            reset_aof(aof_path)

            execute(["SET", "name", "redis"])
            clear_all_stores()

            replay_thread = threading.Thread(
                target=replay_aof,
                args=(aof_path,),
                kwargs={"delay_sec": 0.25},
            )
            replay_thread.start()
            time.sleep(0.05)

            started = time.perf_counter()
            response = execute(["SET", "pending", "ready"])
            elapsed = time.perf_counter() - started
            replay_thread.join()

            self.assertGreaterEqual(elapsed, 0.15)
            self.assertEqual(response, {"type": "simple_string", "value": "OK"})
            self.assertEqual(execute(["GET", "name"]), {"type": "bulk_string", "value": "redis"})
            self.assertEqual(execute(["GET", "pending"]), {"type": "bulk_string", "value": "ready"})


if __name__ == "__main__":
    unittest.main()
