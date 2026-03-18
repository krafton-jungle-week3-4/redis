from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from performance.benchmark import run_latency_benchmark, run_load_benchmark
from performance.clients import MongoBenchmarkClient, RespBenchmarkClient
from performance.config import load_config
from performance.plot_results import create_plots


def _write_latency_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "latency_summary.csv"
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ["backend", "operation", "count", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "min_ms", "max_ms"]
        )
        for backend_name, backend_result in report["backends"].items():
            if "error" in backend_result:
                continue
            for operation, summary in backend_result["latency"].items():
                writer.writerow(
                    [
                        backend_name,
                        operation,
                        summary["count"],
                        summary["avg_ms"],
                        summary["p50_ms"],
                        summary["p95_ms"],
                        summary["p99_ms"],
                        summary["min_ms"],
                        summary["max_ms"],
                    ]
                )
    return file_path


def _write_load_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "load_summary.csv"
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "backend",
                "concurrency",
                "total_requests",
                "success_count",
                "error_count",
                "elapsed_seconds",
                "throughput_rps",
                "avg_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
            ]
        )
        for backend_name, backend_result in report["backends"].items():
            if "error" in backend_result:
                continue
            for row in backend_result["load"]:
                writer.writerow(
                    [
                        backend_name,
                        row["concurrency"],
                        row["total_requests"],
                        row["success_count"],
                        row["error_count"],
                        row["elapsed_seconds"],
                        row["throughput_rps"],
                        row["avg_latency_ms"],
                        row["p95_latency_ms"],
                        row["p99_latency_ms"],
                    ]
                )
    return file_path


def main() -> int:
    resp_config, mongo_config, benchmark_config = load_config()
    benchmark_config.output_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "resp": {"host": resp_config.host, "port": resp_config.port},
            "mongo": {
                "uri": mongo_config.uri,
                "database": mongo_config.database,
                "collection": mongo_config.collection,
            },
            "benchmark": {
                "latency_iterations": benchmark_config.latency_iterations,
                "load_total_requests": benchmark_config.load_total_requests,
                "concurrency_levels": list(benchmark_config.concurrency_levels),
                "output_dir": str(benchmark_config.output_dir),
            },
        },
        "backends": {},
    }

    backends = {
        "resp": lambda: RespBenchmarkClient(resp_config),
        "mongo": lambda: MongoBenchmarkClient(mongo_config),
    }

    for backend_name, client_factory in backends.items():
        print(f"Running {backend_name} latency benchmark...")
        try:
            latency_result = run_latency_benchmark(
                backend_name,
                client_factory,
                benchmark_config.latency_iterations,
            )
            print(f"Running {backend_name} load benchmark...")
            load_result = run_load_benchmark(
                backend_name,
                client_factory,
                benchmark_config.load_total_requests,
                benchmark_config.concurrency_levels,
            )
            report["backends"][backend_name] = {
                "latency": latency_result,
                "load": load_result,
            }
        except Exception as exc:
            report["backends"][backend_name] = {"error": str(exc)}
            print(f"[WARN] {backend_name} benchmark skipped: {exc}")

    json_path = benchmark_config.output_dir / "benchmark_report.json"
    with json_path.open("w", encoding="utf-8") as json_file:
        json.dump(report, json_file, indent=2)

    latency_csv_path = _write_latency_csv(report, benchmark_config.output_dir)
    load_csv_path = _write_load_csv(report, benchmark_config.output_dir)

    try:
        plot_paths = create_plots(report, benchmark_config.output_dir)
    except RuntimeError as exc:
        plot_paths = []
        print(f"[WARN] plot generation skipped: {exc}")

    print(f"Report written to {json_path}")
    print(f"Latency CSV written to {latency_csv_path}")
    print(f"Load CSV written to {load_csv_path}")
    for plot_path in plot_paths:
        print(f"Plot written to {plot_path}")

    succeeded_backends = [
        backend_result for backend_result in report["backends"].values() if "error" not in backend_result
    ]
    return 0 if succeeded_backends else 1


if __name__ == "__main__":
    raise SystemExit(main())
