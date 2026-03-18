import math
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from core.core_state import clear_all_stores
from redis import execute


def _percentile(samples: list[float], percent: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, math.ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


class HotKeyLoadTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()

    def test_hot_key_increments_keep_exact_total_under_heavy_contention(self) -> None:
        total_requests = 4000
        latencies_ms: list[float] = []

        def worker() -> None:
            started = time.perf_counter()
            response = execute(["INCR", "hotkey"])
            latencies_ms.append((time.perf_counter() - started) * 1000)
            self.assertEqual(response["type"], "integer")

        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [executor.submit(worker) for _ in range(total_requests)]
            for future in futures:
                future.result()

        self.assertEqual(execute(["GET", "hotkey"]), {"type": "bulk_string", "value": str(total_requests)})
        self.assertEqual(len(latencies_ms), total_requests)
        self.assertGreater(_percentile(latencies_ms, 95), 0.0)

    def test_hot_key_reads_never_move_backward_while_writes_continue(self) -> None:
        writer_count = 8
        increments_per_writer = 250
        stop_reading = threading.Event()
        regressions: list[tuple[int, int]] = []
        observations: list[int] = []

        def writer() -> None:
            for _ in range(increments_per_writer):
                execute(["INCR", "hotkey"])

        def reader() -> None:
            last_seen = -1
            while not stop_reading.is_set():
                response = execute(["GET", "hotkey"])
                current = 0 if response["type"] == "null" else int(response["value"])
                if current < last_seen:
                    regressions.append((last_seen, current))
                    break
                last_seen = current
                observations.append(current)

        reader_thread = threading.Thread(target=reader, daemon=True)
        reader_thread.start()

        with ThreadPoolExecutor(max_workers=writer_count) as executor:
            futures = [executor.submit(writer) for _ in range(writer_count)]
            for future in futures:
                future.result()

        stop_reading.set()
        reader_thread.join(timeout=1)

        expected_total = writer_count * increments_per_writer
        self.assertFalse(regressions)
        self.assertTrue(observations)
        self.assertEqual(execute(["GET", "hotkey"]), {"type": "bulk_string", "value": str(expected_total)})


if __name__ == "__main__":
    unittest.main()
