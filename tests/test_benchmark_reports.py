from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from performance.config import BenchmarkConfig, MongoConfig, RespConfig
from performance import plot_results, run_benchmarks


def _resp_config() -> RespConfig:
    return RespConfig(
        host="127.0.0.1",
        port=6379,
        label="mini-redis",
        connect_timeout_seconds=5.0,
        socket_timeout_seconds=30.0,
        tcp_nodelay=True,
        keepalive=True,
        use_tls=False,
        tls_server_hostname=None,
        tls_verify=True,
        tls_ca_file=None,
    )


def _mongo_config() -> MongoConfig:
    return MongoConfig(
        uri="mongodb://127.0.0.1:27017",
        database="mini_redis_benchmark",
        collection="kv_store",
        label="mongodb",
        app_name="mini-redis-benchmark",
        server_selection_timeout_ms=10000,
        connect_timeout_ms=10000,
        socket_timeout_ms=30000,
        tls=None,
        tls_allow_invalid_certificates=None,
        tls_ca_file=None,
        direct_connection=None,
    )


def _core_latency() -> dict[str, dict[str, float | int]]:
    return {
        operation: {
            "count": 2,
            "avg_us": 1.0,
            "p50_us": 1.0,
            "p95_us": 1.5,
            "p99_us": 1.8,
            "min_us": 0.9,
            "max_us": 2.0,
        }
        for operation in ("ping", "set", "get", "exists", "type", "del")
    }


def _network_latency(ping_avg: float) -> dict[str, dict[str, float | int]]:
    averages = {
        "ping": ping_avg,
        "set": ping_avg + 0.2,
        "get": ping_avg + 0.3,
        "exists": ping_avg + 0.1,
        "type": ping_avg + 0.4,
        "del": ping_avg + 0.5,
    }
    return {
        operation: {
            "count": 2,
            "avg_ms": avg_ms,
            "p50_ms": avg_ms,
            "p95_ms": avg_ms + 0.1,
            "p99_ms": avg_ms + 0.2,
            "min_ms": max(avg_ms - 0.1, 0.0),
            "max_ms": avg_ms + 0.3,
        }
        for operation, avg_ms in averages.items()
    }


def _network_load() -> list[dict[str, float | int]]:
    return [
        {
            "concurrency": 1,
            "total_requests": 20,
            "success_count": 20,
            "error_count": 0,
            "elapsed_seconds": 0.1,
            "throughput_rps": 200.0,
            "avg_latency_ms": 0.3,
            "p95_latency_ms": 0.4,
            "p99_latency_ms": 0.5,
        }
    ]


class RunBenchmarksTests(unittest.TestCase):
    def test_core_profile_generates_only_core_execute(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            config = BenchmarkConfig(
                latency_iterations=2,
                load_total_requests=20,
                concurrency_levels=(1,),
                profiles=("core",),
                random_seed=1729,
                output_dir=output_dir,
            )

            with (
                patch.object(run_benchmarks, "load_config", return_value=(_resp_config(), _mongo_config(), config)),
                patch.object(run_benchmarks, "run_core_execute_benchmark", return_value=_core_latency()),
                patch.object(run_benchmarks, "create_plots", return_value=[]),
                patch("sys.stdout", new=io.StringIO()),
            ):
                exit_code = run_benchmarks.main()

            self.assertEqual(exit_code, 0)
            report = json.loads((output_dir / "benchmark_report.json").read_text(encoding="utf-8"))
            self.assertIn("core_execute", report)
            self.assertNotIn("network_e2e", report)
            self.assertTrue((output_dir / "core_execute_summary.csv").exists())
            self.assertFalse((output_dir / "network_e2e_latency_summary.csv").exists())

    def test_network_profile_generates_only_network_e2e(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            config = BenchmarkConfig(
                latency_iterations=2,
                load_total_requests=20,
                concurrency_levels=(1,),
                profiles=("network",),
                random_seed=1729,
                output_dir=output_dir,
            )
            resp_latency = _network_latency(1.0)
            mongo_latency = _network_latency(2.0)

            with (
                patch.object(run_benchmarks, "load_config", return_value=(_resp_config(), _mongo_config(), config)),
                patch.object(run_benchmarks, "run_connection_check", return_value={"ok": True}),
                patch.object(
                    run_benchmarks,
                    "run_latency_benchmark",
                    side_effect=[resp_latency, mongo_latency],
                ),
                patch.object(
                    run_benchmarks,
                    "run_load_benchmark",
                    side_effect=[_network_load(), _network_load()],
                ),
                patch.object(run_benchmarks, "create_plots", return_value=[]),
                patch("sys.stdout", new=io.StringIO()),
            ):
                exit_code = run_benchmarks.main()

            self.assertEqual(exit_code, 0)
            report = json.loads((output_dir / "benchmark_report.json").read_text(encoding="utf-8"))
            self.assertIn("network_e2e", report)
            self.assertNotIn("core_execute", report)
            self.assertTrue((output_dir / "network_e2e_latency_summary.csv").exists())
            self.assertTrue((output_dir / "network_e2e_avg_over_ping.csv").exists())
            self.assertTrue((output_dir / "network_e2e_load_summary.csv").exists())
            self.assertFalse((output_dir / "core_execute_summary.csv").exists())

    def test_avg_ms_over_ping_is_shared_by_report_csv_and_plot(self) -> None:
        report = {
            "network_e2e": {
                "backends": {
                    "resp": {
                        "label": "mini-redis",
                        "ping_baseline_ms": 1.0,
                        "latency_ms": _network_latency(1.0),
                        "avg_ms_over_ping": {
                            "ping": 0.0,
                            "set": 0.2,
                            "get": 0.3,
                            "exists": 0.1,
                            "type": 0.4,
                            "del": 0.5,
                        },
                        "load": _network_load(),
                    }
                }
            }
        }

        with TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            csv_path = run_benchmarks._write_network_avg_over_ping_csv(report, output_dir)
            with csv_path.open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.DictReader(csv_file))

            values_from_csv = {
                row["operation"]: float(row["avg_ms_over_ping"])
                for row in rows
            }
            self.assertEqual(values_from_csv, report["network_e2e"]["backends"]["resp"]["avg_ms_over_ping"])

            captured: dict[str, object] = {}

            def fake_svg_plots(fake_report: dict[str, object], output_dir: Path, filename_prefix: str) -> list[Path]:
                captured["avg_ms_over_ping"] = (
                    fake_report["network_e2e"]["backends"]["resp"]["avg_ms_over_ping"]
                )
                svg_path = output_dir / f"{filename_prefix}network_e2e_avg_over_ping.svg"
                svg_path.write_text("Network E2E Average Over PING", encoding="utf-8")
                return [svg_path]

            with (
                patch.object(plot_results, "_create_matplotlib_plots", side_effect=ImportError),
                patch.object(plot_results, "_create_svg_plots", side_effect=fake_svg_plots),
            ):
                plot_paths = plot_results.create_plots(report, output_dir)

            self.assertEqual(
                captured["avg_ms_over_ping"],
                report["network_e2e"]["backends"]["resp"]["avg_ms_over_ping"],
            )
            self.assertTrue(any(path.name.endswith("network_e2e_avg_over_ping.svg") for path in plot_paths))


if __name__ == "__main__":
    unittest.main()
