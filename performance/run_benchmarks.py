from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from urllib.parse import SplitResult, urlsplit, urlunsplit

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from performance.benchmark import (
    OPERATION_NAMES,
    run_core_execute_benchmark,
    run_latency_benchmark,
    run_load_benchmark,
)
from performance.check_connections import run_connection_check
from performance.clients import MongoBenchmarkClient, RespBenchmarkClient
from performance.config import load_config
from performance.plot_results import create_plots


def _redact_mongo_uri(uri: str) -> str:
    parsed = urlsplit(uri)
    netloc = parsed.netloc
    if "@" not in netloc:
        return uri

    credentials, hosts = netloc.rsplit("@", 1)
    if ":" in credentials:
        username, _, _ = credentials.partition(":")
        redacted_credentials = f"{username}:***"
    else:
        redacted_credentials = "***"

    return urlunsplit(
        SplitResult(
            scheme=parsed.scheme,
            netloc=f"{redacted_credentials}@{hosts}",
            path=parsed.path,
            query=parsed.query,
            fragment=parsed.fragment,
        )
    )


def _build_avg_ms_over_ping(
    latency_result: dict[str, dict[str, float | int]],
) -> dict[str, float]:
    ping_avg = float(latency_result["ping"]["avg_ms"])
    return {
        operation: round(max(float(summary["avg_ms"]) - ping_avg, 0.0), 6)
        for operation, summary in latency_result.items()
    }


def _write_core_execute_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "core_execute_summary.csv"
    core_execute = report["core_execute"]
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "operation",
                "count",
                "avg_us",
                "p50_us",
                "p95_us",
                "p99_us",
                "min_us",
                "max_us",
            ]
        )
        for operation in OPERATION_NAMES:
            summary = core_execute["latency_us"][operation]
            writer.writerow(
                [
                    operation,
                    summary["count"],
                    summary["avg_us"],
                    summary["p50_us"],
                    summary["p95_us"],
                    summary["p99_us"],
                    summary["min_us"],
                    summary["max_us"],
                ]
            )
    return file_path


def _write_network_latency_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "network_e2e_latency_summary.csv"
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "backend",
                "backend_label",
                "operation",
                "count",
                "avg_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "min_ms",
                "max_ms",
            ]
        )
        for backend_name, backend_result in report["network_e2e"]["backends"].items():
            if "error" in backend_result:
                continue
            for operation, summary in backend_result["latency_ms"].items():
                writer.writerow(
                    [
                        backend_name,
                        backend_result.get("label", backend_name),
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


def _write_network_avg_over_ping_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "network_e2e_avg_over_ping.csv"
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "backend",
                "backend_label",
                "operation",
                "ping_baseline_ms",
                "avg_ms_over_ping",
            ]
        )
        for backend_name, backend_result in report["network_e2e"]["backends"].items():
            if "error" in backend_result:
                continue
            for operation in OPERATION_NAMES:
                writer.writerow(
                    [
                        backend_name,
                        backend_result.get("label", backend_name),
                        operation,
                        backend_result["ping_baseline_ms"],
                        backend_result["avg_ms_over_ping"][operation],
                    ]
                )
    return file_path


def _write_network_load_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "network_e2e_load_summary.csv"
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "backend",
                "backend_label",
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
        for backend_name, backend_result in report["network_e2e"]["backends"].items():
            if "error" in backend_result:
                continue
            for row in backend_result["load"]:
                writer.writerow(
                    [
                        backend_name,
                        backend_result.get("label", backend_name),
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


def _write_connection_json(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "connection_summary.json"
    summary = {
        "generated_at": report["generated_at"],
        "backends": {},
    }

    for backend_name, backend_result in report["network_e2e"]["backends"].items():
        summary["backends"][backend_name] = {
            "label": backend_result.get("label", backend_name),
            "preflight": backend_result.get("preflight"),
            "error": backend_result.get("error"),
        }

    with file_path.open("w", encoding="utf-8") as json_file:
        json.dump(summary, json_file, indent=2)
    return file_path


def main() -> int:
    resp_config, mongo_config, benchmark_config = load_config()
    benchmark_config.output_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "resp": {
                "host": resp_config.host,
                "port": resp_config.port,
                "label": resp_config.label,
                "connect_timeout_seconds": resp_config.connect_timeout_seconds,
                "socket_timeout_seconds": resp_config.socket_timeout_seconds,
                "tcp_nodelay": resp_config.tcp_nodelay,
                "keepalive": resp_config.keepalive,
                "use_tls": resp_config.use_tls,
                "tls_server_hostname": resp_config.tls_server_hostname,
                "tls_verify": resp_config.tls_verify,
                "tls_ca_file": resp_config.tls_ca_file,
            },
            "mongo": {
                "uri_redacted": _redact_mongo_uri(mongo_config.uri),
                "database": mongo_config.database,
                "collection": mongo_config.collection,
                "label": mongo_config.label,
                "app_name": mongo_config.app_name,
                "server_selection_timeout_ms": mongo_config.server_selection_timeout_ms,
                "connect_timeout_ms": mongo_config.connect_timeout_ms,
                "socket_timeout_ms": mongo_config.socket_timeout_ms,
                "tls": mongo_config.tls,
                "tls_allow_invalid_certificates": mongo_config.tls_allow_invalid_certificates,
                "tls_ca_file": mongo_config.tls_ca_file,
                "direct_connection": mongo_config.direct_connection,
            },
            "benchmark": {
                "latency_iterations": benchmark_config.latency_iterations,
                "load_total_requests": benchmark_config.load_total_requests,
                "concurrency_levels": list(benchmark_config.concurrency_levels),
                "profiles": list(benchmark_config.profiles),
                "random_seed": benchmark_config.random_seed,
                "output_dir": str(benchmark_config.output_dir),
            },
        },
    }

    success_count = 0

    if "core" in benchmark_config.profiles:
        print("Running core_execute benchmark against redis.execute...")
        try:
            report["core_execute"] = {
                "label": "redis.execute",
                "latency_us": run_core_execute_benchmark(
                    benchmark_config.latency_iterations,
                    random_seed=benchmark_config.random_seed,
                ),
            }
            success_count += 1
        except Exception as exc:
            report["core_execute"] = {
                "label": "redis.execute",
                "error": str(exc),
            }
            print(f"[WARN] core_execute benchmark skipped: {exc}")

    if "network" in benchmark_config.profiles:
        report["network_e2e"] = {"backends": {}}
        backends = {
            "resp": {
                "label": resp_config.label,
                "factory": lambda: RespBenchmarkClient(resp_config),
            },
            "mongo": {
                "label": mongo_config.label,
                "factory": lambda: MongoBenchmarkClient(mongo_config),
            },
        }

        for backend_name, backend in backends.items():
            backend_label = backend["label"]
            client_factory = backend["factory"]
            print(f"Running {backend_name} connection check against {backend_label}...")
            try:
                preflight_result = run_connection_check(client_factory, backend_name)
                print(f"[OK] {backend_name} connection check passed")

                print(f"Running {backend_name} network_e2e latency benchmark against {backend_label}...")
                latency_result = run_latency_benchmark(
                    backend_name,
                    client_factory,
                    benchmark_config.latency_iterations,
                    random_seed=benchmark_config.random_seed,
                )
                print(f"Running {backend_name} network_e2e load benchmark against {backend_label}...")
                load_result = run_load_benchmark(
                    backend_name,
                    client_factory,
                    benchmark_config.load_total_requests,
                    benchmark_config.concurrency_levels,
                    random_seed=benchmark_config.random_seed,
                )
                report["network_e2e"]["backends"][backend_name] = {
                    "label": backend_label,
                    "preflight": preflight_result,
                    "ping_baseline_ms": latency_result["ping"]["avg_ms"],
                    "latency_ms": latency_result,
                    "avg_ms_over_ping": _build_avg_ms_over_ping(latency_result),
                    "load": load_result,
                }
                success_count += 1
            except Exception as exc:
                report["network_e2e"]["backends"][backend_name] = {
                    "label": backend_label,
                    "preflight": {"ok": False, "error": str(exc)},
                    "error": str(exc),
                }
                print(f"[WARN] {backend_name} benchmark skipped: {exc}")

    json_path = benchmark_config.output_dir / "benchmark_report.json"
    with json_path.open("w", encoding="utf-8") as json_file:
        json.dump(report, json_file, indent=2)

    written_paths: list[Path] = [json_path]

    if "core_execute" in report and "error" not in report["core_execute"]:
        written_paths.append(_write_core_execute_csv(report, benchmark_config.output_dir))

    if "network_e2e" in report:
        written_paths.append(_write_connection_json(report, benchmark_config.output_dir))
        written_paths.append(_write_network_latency_csv(report, benchmark_config.output_dir))
        written_paths.append(_write_network_avg_over_ping_csv(report, benchmark_config.output_dir))
        written_paths.append(_write_network_load_csv(report, benchmark_config.output_dir))

    try:
        plot_paths = create_plots(report, benchmark_config.output_dir)
    except RuntimeError as exc:
        plot_paths = []
        print(f"[WARN] plot generation skipped: {exc}")

    print(f"Report written to {json_path}")
    for path in written_paths[1:]:
        print(f"Artifact written to {path}")
    for plot_path in plot_paths:
        print(f"Plot written to {plot_path}")

    return 0 if success_count else 1


if __name__ == "__main__":
    raise SystemExit(main())
