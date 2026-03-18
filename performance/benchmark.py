from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import math
import random
import statistics
import time
import uuid
from typing import Callable, Protocol


OPERATION_NAMES = ("ping", "set", "get", "exists", "type", "del")
READ_OPERATION_NAMES = ("get", "exists", "type")


class BenchmarkClient(Protocol):
    def __enter__(self) -> "BenchmarkClient": ...
    def __exit__(self, exc_type, exc, tb) -> None: ...
    def ping(self) -> str: ...
    def set_value(self, key: str, value: str) -> None: ...
    def get_value(self, key: str) -> str | None: ...
    def delete_value(self, key: str) -> int: ...
    def exists(self, key: str) -> bool: ...
    def type_of(self, key: str) -> str: ...


ClientFactory = Callable[[], BenchmarkClient]


@dataclass(frozen=True)
class OperationSpec:
    name: str
    key: str | None = None
    value: str | None = None
    expected: str | int | bool | None = None


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


class CoreExecuteBenchmarkClient:
    def __init__(self) -> None:
        from redis import execute

        self._execute = execute

    def __enter__(self) -> "CoreExecuteBenchmarkClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def ping(self) -> str:
        response = self._execute(["PING"])
        if response != {"type": "simple_string", "value": "PONG"}:
            raise RuntimeError(f"unexpected core PING response: {response!r}")
        return "PONG"

    def set_value(self, key: str, value: str) -> None:
        response = self._execute(["SET", key, value])
        if response != {"type": "simple_string", "value": "OK"}:
            raise RuntimeError(f"unexpected core SET response: {response!r}")

    def get_value(self, key: str) -> str | None:
        response = self._execute(["GET", key])
        if response["type"] == "null":
            return None
        if response["type"] != "bulk_string":
            raise RuntimeError(f"unexpected core GET response: {response!r}")
        return str(response["value"])

    def delete_value(self, key: str) -> int:
        response = self._execute(["DEL", key])
        if response["type"] != "integer":
            raise RuntimeError(f"unexpected core DEL response: {response!r}")
        return int(response["value"])

    def exists(self, key: str) -> bool:
        response = self._execute(["EXISTS", key])
        if response["type"] != "integer":
            raise RuntimeError(f"unexpected core EXISTS response: {response!r}")
        return int(response["value"]) == 1

    def type_of(self, key: str) -> str:
        response = self._execute(["TYPE", key])
        if response["type"] != "bulk_string":
            raise RuntimeError(f"unexpected core TYPE response: {response!r}")
        return str(response["value"])

    def close(self) -> None:
        return None


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


def _summarize_latency_us(samples: list[float]) -> dict[str, float | int]:
    if not samples:
        return {
            "count": 0,
            "avg_us": 0.0,
            "p50_us": 0.0,
            "p95_us": 0.0,
            "p99_us": 0.0,
            "min_us": 0.0,
            "max_us": 0.0,
        }
    return {
        "count": len(samples),
        "avg_us": round(statistics.fmean(samples), 6),
        "p50_us": round(_percentile(samples, 50), 6),
        "p95_us": round(_percentile(samples, 95), 6),
        "p99_us": round(_percentile(samples, 99), 6),
        "min_us": round(min(samples), 6),
        "max_us": round(max(samples), 6),
    }


def _distribute_work(total_requests: int, workers: int) -> list[int]:
    base_count, remainder = divmod(total_requests, workers)
    assignments = [base_count] * workers
    for index in range(remainder):
        assignments[index] += 1
    return assignments


def _cleanup_keys(client_factory: ClientFactory, keys: list[str]) -> None:
    unique_keys = list(dict.fromkeys(keys))
    if not unique_keys:
        return

    try:
        with client_factory() as client:
            for key in unique_keys:
                try:
                    client.delete_value(key)
                except Exception:
                    # cleanup는 벤치마크 정확도 보조용이므로 실패해도 본 측정을 깨지 않는다.
                    continue
    except Exception:
        return


def _clear_core_state() -> None:
    from core.core_state import (
        expiry_store,
        hash_store,
        list_store,
        set_store,
        store_lock,
        string_store,
        zset_store,
    )

    with store_lock:
        string_store.clear()
        set_store.clear()
        list_store.clear()
        hash_store.clear()
        zset_store.clear()
        expiry_store.clear()


def _require_key(operation: OperationSpec) -> str:
    if operation.key is None:
        raise ValueError(f"{operation.name} requires a key")
    return operation.key


def _execute_operation(client: BenchmarkClient, operation: OperationSpec) -> str | int | bool | None:
    if operation.name == "ping":
        return client.ping()
    if operation.name == "set":
        key = _require_key(operation)
        if operation.value is None:
            raise ValueError("set requires a value")
        client.set_value(key, operation.value)
        return None
    if operation.name == "get":
        return client.get_value(_require_key(operation))
    if operation.name == "exists":
        return client.exists(_require_key(operation))
    if operation.name == "type":
        return client.type_of(_require_key(operation))
    if operation.name == "del":
        return client.delete_value(_require_key(operation))
    raise ValueError(f"unsupported benchmark operation: {operation.name}")


def _validate_operation_result(
    backend_name: str,
    operation: OperationSpec,
    result: str | int | bool | None,
) -> None:
    if operation.expected is None:
        return
    if result != operation.expected:
        raise RuntimeError(
            f"{backend_name} returned an unexpected {operation.name.upper()} result "
            f"for {operation.key!r}: {result!r}"
        )


def _build_latency_workload(
    key_prefix: str,
    iterations: int,
    value: str,
    random_seed: int,
) -> tuple[list[OperationSpec], list[tuple[str, str]], list[str]]:
    rng = random.Random(random_seed)
    shuffled_names = [OPERATION_NAMES[index % len(OPERATION_NAMES)] for index in range(iterations * len(OPERATION_NAMES))]
    rng.shuffle(shuffled_names)

    read_keys = [f"{key_prefix}:read:{index}" for index in range(iterations)]
    del_keys = [f"{key_prefix}:del:{index}" for index in range(iterations)]
    counters = {name: 0 for name in OPERATION_NAMES}
    operations: list[OperationSpec] = []
    created_set_keys: list[str] = []

    for name in shuffled_names:
        if name == "ping":
            operations.append(OperationSpec(name="ping", expected="PONG"))
            continue

        if name == "set":
            set_index = counters["set"]
            key = f"{key_prefix}:set:{set_index}"
            counters["set"] += 1
            created_set_keys.append(key)
            operations.append(OperationSpec(name="set", key=key, value=value))
            continue

        if name in READ_OPERATION_NAMES:
            read_index = counters[name]
            key = read_keys[read_index]
            counters[name] += 1
            expected = value if name == "get" else True if name == "exists" else "string"
            operations.append(OperationSpec(name=name, key=key, expected=expected))
            continue

        if name == "del":
            del_index = counters["del"]
            key = del_keys[del_index]
            counters["del"] += 1
            operations.append(OperationSpec(name="del", key=key, expected=1))
            continue

        raise ValueError(f"unsupported operation in latency workload: {name}")

    preseed_pairs = [(key, value) for key in read_keys + del_keys]
    cleanup_keys = read_keys + created_set_keys + del_keys
    return operations, preseed_pairs, cleanup_keys


def _build_load_workload(
    key_prefix: str,
    total_requests: int,
    random_seed: int,
) -> tuple[list[OperationSpec], list[tuple[str, str]], list[str]]:
    rng = random.Random(random_seed)
    shuffled_names = [OPERATION_NAMES[index % len(OPERATION_NAMES)] for index in range(total_requests)]
    rng.shuffle(shuffled_names)

    seed_key_count = max(64, total_requests // len(OPERATION_NAMES))
    seed_keys = [f"{key_prefix}:seed:{index}" for index in range(seed_key_count)]
    seed_values = {key: f"seed-value-{index}" for index, key in enumerate(seed_keys)}
    del_key_count = shuffled_names.count("del")
    del_keys = [f"{key_prefix}:del:{index}" for index in range(del_key_count)]

    counters = {name: 0 for name in OPERATION_NAMES}
    operations: list[OperationSpec] = []
    created_set_keys: list[str] = []

    for name in shuffled_names:
        if name == "ping":
            operations.append(OperationSpec(name="ping", expected="PONG"))
            continue

        if name == "set":
            set_index = counters["set"]
            key = f"{key_prefix}:set:{set_index}"
            counters["set"] += 1
            created_set_keys.append(key)
            operations.append(OperationSpec(name="set", key=key, value="load-value"))
            continue

        if name == "get":
            read_index = counters["get"]
            key = seed_keys[read_index % len(seed_keys)]
            counters["get"] += 1
            operations.append(OperationSpec(name="get", key=key, expected=seed_values[key]))
            continue

        if name == "exists":
            read_index = counters["exists"]
            key = seed_keys[read_index % len(seed_keys)]
            counters["exists"] += 1
            operations.append(OperationSpec(name="exists", key=key, expected=True))
            continue

        if name == "type":
            read_index = counters["type"]
            key = seed_keys[read_index % len(seed_keys)]
            counters["type"] += 1
            operations.append(OperationSpec(name="type", key=key, expected="string"))
            continue

        if name == "del":
            del_index = counters["del"]
            key = del_keys[del_index]
            counters["del"] += 1
            operations.append(OperationSpec(name="del", key=key, expected=1))
            continue

        raise ValueError(f"unsupported operation in load workload: {name}")

    preseed_pairs = [(key, seed_values[key]) for key in seed_keys] + [
        (key, "load-value") for key in del_keys
    ]
    cleanup_keys = seed_keys + created_set_keys + del_keys
    return operations, preseed_pairs, cleanup_keys


def run_latency_benchmark(
    backend_name: str,
    client_factory: ClientFactory,
    iterations: int,
    random_seed: int = 1729,
) -> dict[str, dict[str, float | int]]:
    if iterations <= 0:
        raise ValueError("iterations must be positive")

    value = "benchmark-value"
    key_prefix = f"perf:{backend_name}:latency:{uuid.uuid4().hex}"
    samples = {name: [] for name in OPERATION_NAMES}
    operations, preseed_pairs, cleanup_keys = _build_latency_workload(
        key_prefix,
        iterations,
        value,
        random_seed,
    )

    try:
        with client_factory() as client:
            warmup_key = f"{key_prefix}:warmup"
            client.ping()
            client.set_value(warmup_key, value)
            client.get_value(warmup_key)
            client.exists(warmup_key)
            client.type_of(warmup_key)
            client.delete_value(warmup_key)

            for key, seed_value in preseed_pairs:
                client.set_value(key, seed_value)

            for operation in operations:
                start = time.perf_counter()
                result = _execute_operation(client, operation)
                samples[operation.name].append((time.perf_counter() - start) * 1000)
                _validate_operation_result(backend_name, operation, result)
    finally:
        _cleanup_keys(client_factory, cleanup_keys)

    return {
        operation_name: asdict(_summarize_latency(samples[operation_name]))
        for operation_name in OPERATION_NAMES
    }


def run_core_execute_benchmark(
    iterations: int,
    random_seed: int = 1729,
) -> dict[str, dict[str, float | int]]:
    if iterations <= 0:
        raise ValueError("iterations must be positive")

    value = "benchmark-value"
    key_prefix = f"perf:core_execute:latency:{uuid.uuid4().hex}"
    samples = {name: [] for name in OPERATION_NAMES}
    operations, preseed_pairs, cleanup_keys = _build_latency_workload(
        key_prefix,
        iterations,
        value,
        random_seed,
    )

    _clear_core_state()
    try:
        with CoreExecuteBenchmarkClient() as client:
            warmup_key = f"{key_prefix}:warmup"
            client.ping()
            client.set_value(warmup_key, value)
            client.get_value(warmup_key)
            client.exists(warmup_key)
            client.type_of(warmup_key)
            client.delete_value(warmup_key)

            for key, seed_value in preseed_pairs:
                client.set_value(key, seed_value)

            for operation in operations:
                start = time.perf_counter()
                result = _execute_operation(client, operation)
                samples[operation.name].append((time.perf_counter() - start) * 1_000_000)
                _validate_operation_result("core_execute", operation, result)
    finally:
        _cleanup_keys(CoreExecuteBenchmarkClient, cleanup_keys)
        _clear_core_state()

    return {
        operation_name: _summarize_latency_us(samples[operation_name])
        for operation_name in OPERATION_NAMES
    }


def _run_load_worker(
    backend_name: str,
    client_factory: ClientFactory,
    operations: list[OperationSpec],
) -> dict[str, object]:
    latencies_ms: list[float] = []
    success_count = 0
    error_count = 0

    with client_factory() as client:
        for operation in operations:
            start = time.perf_counter()
            try:
                result = _execute_operation(client, operation)
                _validate_operation_result(backend_name, operation, result)
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
    random_seed: int = 1729,
) -> list[dict[str, float | int]]:
    if total_requests <= 0:
        raise ValueError("total_requests must be positive")

    key_prefix = f"perf:{backend_name}:load:{uuid.uuid4().hex}"
    load_results: list[dict[str, float | int]] = []
    for concurrency in concurrency_levels:
        concurrency_prefix = f"{key_prefix}:c{concurrency}"
        operations, preseed_pairs, cleanup_keys = _build_load_workload(
            concurrency_prefix,
            total_requests,
            random_seed + concurrency,
        )
        assignments = _distribute_work(total_requests, concurrency)
        worker_futures = []
        operation_slices: list[list[OperationSpec]] = []
        next_start_index = 0
        for request_count in assignments:
            operation_slices.append(operations[next_start_index : next_start_index + request_count])
            next_start_index += request_count

        try:
            with client_factory() as client:
                for key, seed_value in preseed_pairs:
                    client.set_value(key, seed_value)

            overall_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                for operation_slice in operation_slices:
                    future = executor.submit(
                        _run_load_worker,
                        backend_name,
                        client_factory,
                        operation_slice,
                    )
                    worker_futures.append(future)

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
        finally:
            _cleanup_keys(client_factory, cleanup_keys)

    return load_results
