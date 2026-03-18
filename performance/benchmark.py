from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import math
import statistics
import time
import uuid
from typing import Callable


ClientFactory = Callable[[], object]


@dataclass(frozen=True)
class LatencySummary:
    count: int
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float


@dataclass(frozen=True)
class LoadSummary:
    concurrency: int
    total_requests: int
    success_count: int
    error_count: int
    elapsed_seconds: float
    throughput_rps: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float


def _percentile(samples: list[float], percent: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, math.ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


def _summarize_latency(samples: list[float]) -> LatencySummary:
    if not samples:
        return LatencySummary(
            count=0,
            avg_ms=0.0,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            min_ms=0.0,
            max_ms=0.0,
        )
    return LatencySummary(
        count=len(samples),
        avg_ms=round(statistics.fmean(samples), 6),
        p50_ms=round(_percentile(samples, 50), 6),
        p95_ms=round(_percentile(samples, 95), 6),
        p99_ms=round(_percentile(samples, 99), 6),
        min_ms=round(min(samples), 6),
        max_ms=round(max(samples), 6),
    )


def _distribute_work(total_requests: int, workers: int) -> list[int]:
    base_count, remainder = divmod(total_requests, workers)
    assignments = [base_count] * workers
    for index in range(remainder):
        assignments[index] += 1
    return assignments


def run_latency_benchmark(
    backend_name: str,
    client_factory: ClientFactory,
    iterations: int,
) -> dict[str, dict[str, float | int]]:
    if iterations <= 0:
        raise ValueError("iterations must be positive")

    value = "benchmark-value"
    key_prefix = f"perf:{backend_name}:latency:{uuid.uuid4().hex}"
    set_samples: list[float] = []
    get_samples: list[float] = []

    with client_factory() as client:
        client.set_value(f"{key_prefix}:warmup", value)
        client.get_value(f"{key_prefix}:warmup")

        for index in range(iterations):
            key = f"{key_prefix}:item:{index}"
            start = time.perf_counter()
            client.set_value(key, value)
            set_samples.append((time.perf_counter() - start) * 1000)

        for index in range(iterations):
            key = f"{key_prefix}:item:{index}"
            start = time.perf_counter()
            result = client.get_value(key)
            get_samples.append((time.perf_counter() - start) * 1000)
            if result != value:
                raise RuntimeError(
                    f"{backend_name} returned an unexpected value for {key!r}: {result!r}"
                )

    return {
        "set": asdict(_summarize_latency(set_samples)),
        "get": asdict(_summarize_latency(get_samples)),
    }


def _run_load_worker(
    client_factory: ClientFactory,
    key_prefix: str,
    seed_keys: list[str],
    worker_id: int,
    start_index: int,
    request_count: int,
) -> dict[str, object]:
    latencies_ms: list[float] = []
    success_count = 0
    error_count = 0

    with client_factory() as client:
        for offset in range(request_count):
            global_index = start_index + offset
            start = time.perf_counter()
            try:
                if global_index % 2 == 0:
                    key = f"{key_prefix}:worker:{worker_id}:set:{global_index}"
                    client.set_value(key, "load-value")
                else:
                    key = seed_keys[global_index % len(seed_keys)]
                    result = client.get_value(key)
                    if result is None:
                        raise RuntimeError(f"seed key {key!r} was not found")
                latencies_ms.append((time.perf_counter() - start) * 1000)
                success_count += 1
            except Exception:
                error_count += 1

    return {
        "latencies_ms": latencies_ms,
        "success_count": success_count,
        "error_count": error_count,
    }


def run_load_benchmark(
    backend_name: str,
    client_factory: ClientFactory,
    total_requests: int,
    concurrency_levels: tuple[int, ...],
) -> list[dict[str, float | int]]:
    if total_requests <= 0:
        raise ValueError("total_requests must be positive")

    key_prefix = f"perf:{backend_name}:load:{uuid.uuid4().hex}"
    seed_keys = [f"{key_prefix}:seed:{index}" for index in range(max(64, total_requests // 4))]

    with client_factory() as client:
        for index, key in enumerate(seed_keys):
            client.set_value(key, f"seed-value-{index}")

    load_results: list[dict[str, float | int]] = []
    for concurrency in concurrency_levels:
        assignments = _distribute_work(total_requests, concurrency)
        worker_futures = []
        overall_start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            next_start_index = 0
            for worker_id, request_count in enumerate(assignments):
                future = executor.submit(
                    _run_load_worker,
                    client_factory,
                    key_prefix,
                    seed_keys,
                    worker_id,
                    next_start_index,
                    request_count,
                )
                worker_futures.append(future)
                next_start_index += request_count

        elapsed_seconds = time.perf_counter() - overall_start
        worker_results = [future.result() for future in worker_futures]
        latencies = [
            latency
            for worker_result in worker_results
            for latency in worker_result["latencies_ms"]
        ]
        success_count = sum(int(worker_result["success_count"]) for worker_result in worker_results)
        error_count = sum(int(worker_result["error_count"]) for worker_result in worker_results)
        latency_summary = _summarize_latency(latencies)

        load_results.append(
            asdict(
                LoadSummary(
                    concurrency=concurrency,
                    total_requests=total_requests,
                    success_count=success_count,
                    error_count=error_count,
                    elapsed_seconds=round(elapsed_seconds, 6),
                    throughput_rps=round(
                        success_count / elapsed_seconds if elapsed_seconds else 0.0,
                        6,
                    ),
                    avg_latency_ms=latency_summary.avg_ms,
                    p95_latency_ms=latency_summary.p95_ms,
                    p99_latency_ms=latency_summary.p99_ms,
                )
            )
        )

    return load_results
