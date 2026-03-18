from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from urllib.parse import SplitResult, urlsplit, urlunsplit

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from performance.benchmark import run_latency_benchmark, run_load_benchmark
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


def _write_latency_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "latency_summary.csv"
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
        for backend_name, backend_result in report["backends"].items():
            if "error" in backend_result:
                continue
            for operation, summary in backend_result["latency"].items():
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


def _write_adjusted_latency_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "latency_over_ping.csv"
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "backend",
                "backend_label",
                "operation",
                "count",
                "avg_ms_over_ping",
                "p50_ms_over_ping",
                "p95_ms_over_ping",
                "p99_ms_over_ping",
                "min_ms_over_ping",
                "max_ms_over_ping",
            ]
        )
        for backend_name, backend_result in report["backends"].items():
            if "error" in backend_result:
                continue
            for operation, summary in backend_result["latency_over_ping"].items():
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


def _write_load_csv(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "load_summary.csv"
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
        for backend_name, backend_result in report["backends"].items():
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


def _build_latency_over_ping(
    latency_result: dict[str, dict[str, float | int]],
) -> dict[str, dict[str, float | int]]:
    ping_summary = latency_result["ping"]
    adjusted: dict[str, dict[str, float | int]] = {}

    for operation, summary in latency_result.items():
        adjusted[operation] = {
            "count": summary["count"],
            "avg_ms": round(max(float(summary["avg_ms"]) - float(ping_summary["avg_ms"]), 0.0), 6),
            "p50_ms": round(max(float(summary["p50_ms"]) - float(ping_summary["p50_ms"]), 0.0), 6),
            "p95_ms": round(max(float(summary["p95_ms"]) - float(ping_summary["p95_ms"]), 0.0), 6),
            "p99_ms": round(max(float(summary["p99_ms"]) - float(ping_summary["p99_ms"]), 0.0), 6),
            "min_ms": round(max(float(summary["min_ms"]) - float(ping_summary["min_ms"]), 0.0), 6),
            "max_ms": round(max(float(summary["max_ms"]) - float(ping_summary["max_ms"]), 0.0), 6),
        }

    return adjusted


def _write_connection_json(report: dict[str, object], output_dir: Path) -> Path:
    file_path = output_dir / "connection_summary.json"
    summary = {
        "generated_at": report["generated_at"],
        "backends": {},
    }

    for backend_name, backend_result in report["backends"].items():
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
                "random_seed": benchmark_config.random_seed,
                "output_dir": str(benchmark_config.output_dir),
            },
        },
        "backends": {},
    }

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

            print(f"Running {backend_name} latency benchmark against {backend_label}...")
            latency_result = run_latency_benchmark(
                backend_name,
                client_factory,
                benchmark_config.latency_iterations,
                random_seed=benchmark_config.random_seed,
            )
            print(f"Running {backend_name} load benchmark against {backend_label}...")
            load_result = run_load_benchmark(
                backend_name,
                client_factory,
                benchmark_config.load_total_requests,
                benchmark_config.concurrency_levels,
                random_seed=benchmark_config.random_seed,
            )
            report["backends"][backend_name] = {
                "label": backend_label,
                "preflight": preflight_result,
                "latency": latency_result,
                "latency_over_ping": _build_latency_over_ping(latency_result),
                "load": load_result,
            }
        except Exception as exc:
            report["backends"][backend_name] = {
                "label": backend_label,
                "preflight": {"ok": False, "error": str(exc)},
                "error": str(exc),
            }
            print(f"[WARN] {backend_name} benchmark skipped: {exc}")

    json_path = benchmark_config.output_dir / "benchmark_report.json"
    with json_path.open("w", encoding="utf-8") as json_file:
        json.dump(report, json_file, indent=2)

    connection_json_path = _write_connection_json(report, benchmark_config.output_dir)
    latency_csv_path = _write_latency_csv(report, benchmark_config.output_dir)
    adjusted_latency_csv_path = _write_adjusted_latency_csv(report, benchmark_config.output_dir)
    load_csv_path = _write_load_csv(report, benchmark_config.output_dir)

    try:
        plot_paths = create_plots(report, benchmark_config.output_dir)
    except RuntimeError as exc:
        plot_paths = []
        print(f"[WARN] plot generation skipped: {exc}")

    print(f"Report written to {json_path}")
    print(f"Connection summary written to {connection_json_path}")
    print(f"Latency CSV written to {latency_csv_path}")
    print(f"Latency-over-ping CSV written to {adjusted_latency_csv_path}")
    print(f"Load CSV written to {load_csv_path}")
    for plot_path in plot_paths:
        print(f"Plot written to {plot_path}")

    succeeded_backends = [
        backend_result for backend_result in report["backends"].values() if "error" not in backend_result
    ]
    return 0 if succeeded_backends else 1


if __name__ == "__main__":
    raise SystemExit(main())
