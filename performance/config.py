from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import os


DEFAULT_RESULTS_ROOT = Path(__file__).resolve().parent / "results"


@dataclass(frozen=True)
class RespConfig:
    host: str
    port: int


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    database: str
    collection: str


@dataclass(frozen=True)
class BenchmarkConfig:
    latency_iterations: int
    load_total_requests: int
    concurrency_levels: tuple[int, ...]
    output_dir: Path


def _parse_concurrency_levels(raw_value: str) -> tuple[int, ...]:
    levels = []
    for chunk in raw_value.split(","):
        stripped = chunk.strip()
        if not stripped:
            continue
        level = int(stripped)
        if level <= 0:
            raise ValueError("PERF_CONCURRENCY_LEVELS must contain only positive integers")
        levels.append(level)
    if not levels:
        raise ValueError("PERF_CONCURRENCY_LEVELS must not be empty")
    return tuple(levels)


def load_config() -> tuple[RespConfig, MongoConfig, BenchmarkConfig]:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(
        os.getenv("PERF_OUTPUT_DIR", str(DEFAULT_RESULTS_ROOT / timestamp))
    ).resolve()

    resp_config = RespConfig(
        host=os.getenv("MINIREDIS_RESP_HOST", "127.0.0.1"),
        port=int(os.getenv("MINIREDIS_RESP_PORT", "6379")),
    )
    mongo_config = MongoConfig(
        uri=os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"),
        database=os.getenv("MONGO_DB_NAME", "mini_redis_benchmark"),
        collection=os.getenv("MONGO_COLLECTION_NAME", "kv_store"),
    )
    benchmark_config = BenchmarkConfig(
        latency_iterations=int(os.getenv("PERF_LATENCY_ITERATIONS", "200")),
        load_total_requests=int(os.getenv("PERF_LOAD_TOTAL_REQUESTS", "2000")),
        concurrency_levels=_parse_concurrency_levels(
            os.getenv("PERF_CONCURRENCY_LEVELS", "1,4,8,16")
        ),
        output_dir=output_dir,
    )
    return resp_config, mongo_config, benchmark_config
