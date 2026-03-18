from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import os


DEFAULT_RESULTS_ROOT = Path(__file__).resolve().parent / "results"
TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


@dataclass(frozen=True)
class RespConfig:
    host: str
    port: int
    label: str
    connect_timeout_seconds: float
    socket_timeout_seconds: float | None
    tcp_nodelay: bool
    keepalive: bool
    use_tls: bool
    tls_server_hostname: str | None
    tls_verify: bool
    tls_ca_file: str | None


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    database: str
    collection: str
    label: str
    app_name: str
    server_selection_timeout_ms: int
    connect_timeout_ms: int | None
    socket_timeout_ms: int | None
    tls: bool | None
    tls_allow_invalid_certificates: bool | None
    tls_ca_file: str | None
    direct_connection: bool | None


@dataclass(frozen=True)
class BenchmarkConfig:
    latency_iterations: int
    load_total_requests: int
    concurrency_levels: tuple[int, ...]
    random_seed: int
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


def _parse_bool(raw_value: str | None, *, env_name: str, default: bool | None = None) -> bool | None:
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f"{env_name} must be one of: {sorted(TRUE_VALUES | FALSE_VALUES)}")


def _parse_int(raw_value: str | None, *, env_name: str, default: int | None = None) -> int | None:
    if raw_value is None:
        return default
    stripped = raw_value.strip()
    if not stripped:
        return default
    return int(stripped)


def _parse_float(raw_value: str | None, *, env_name: str, default: float | None = None) -> float | None:
    if raw_value is None:
        return default
    stripped = raw_value.strip()
    if not stripped:
        return default
    return float(stripped)


def _optional_str(env_name: str) -> str | None:
    raw_value = os.getenv(env_name)
    if raw_value is None:
        return None
    stripped = raw_value.strip()
    return stripped or None


def load_config() -> tuple[RespConfig, MongoConfig, BenchmarkConfig]:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(
        os.getenv("PERF_OUTPUT_DIR", str(DEFAULT_RESULTS_ROOT / timestamp))
    ).resolve()
    resp_connect_timeout_seconds = _parse_float(
        os.getenv("MINIREDIS_RESP_CONNECT_TIMEOUT_SEC"),
        env_name="MINIREDIS_RESP_CONNECT_TIMEOUT_SEC",
        default=5.0,
    )
    mongo_server_selection_timeout_ms = _parse_int(
        os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS"),
        env_name="MONGO_SERVER_SELECTION_TIMEOUT_MS",
        default=10000,
    )

    resp_config = RespConfig(
        host=os.getenv("MINIREDIS_RESP_HOST", "127.0.0.1"),
        port=int(os.getenv("MINIREDIS_RESP_PORT", "6379")),
        label=os.getenv("MINIREDIS_RESP_LABEL", "mini-redis"),
        connect_timeout_seconds=5.0 if resp_connect_timeout_seconds is None else resp_connect_timeout_seconds,
        socket_timeout_seconds=_parse_float(
            os.getenv("MINIREDIS_RESP_SOCKET_TIMEOUT_SEC"),
            env_name="MINIREDIS_RESP_SOCKET_TIMEOUT_SEC",
            default=30.0,
        ),
        tcp_nodelay=_parse_bool(
            os.getenv("MINIREDIS_RESP_TCP_NODELAY"),
            env_name="MINIREDIS_RESP_TCP_NODELAY",
            default=True,
        )
        is not False,
        keepalive=_parse_bool(
            os.getenv("MINIREDIS_RESP_KEEPALIVE"),
            env_name="MINIREDIS_RESP_KEEPALIVE",
            default=True,
        )
        is not False,
        use_tls=_parse_bool(
            os.getenv("MINIREDIS_RESP_TLS"),
            env_name="MINIREDIS_RESP_TLS",
            default=False,
        )
        is True,
        tls_server_hostname=_optional_str("MINIREDIS_RESP_TLS_SERVER_HOSTNAME"),
        tls_verify=_parse_bool(
            os.getenv("MINIREDIS_RESP_TLS_VERIFY"),
            env_name="MINIREDIS_RESP_TLS_VERIFY",
            default=True,
        )
        is not False,
        tls_ca_file=_optional_str("MINIREDIS_RESP_TLS_CA_FILE"),
    )
    mongo_config = MongoConfig(
        uri=os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"),
        database=os.getenv("MONGO_DB_NAME", "mini_redis_benchmark"),
        collection=os.getenv("MONGO_COLLECTION_NAME", "kv_store"),
        label=os.getenv("MONGO_LABEL", "mongodb"),
        app_name=os.getenv("MONGO_APP_NAME", "mini-redis-benchmark"),
        server_selection_timeout_ms=10000
        if mongo_server_selection_timeout_ms is None
        else mongo_server_selection_timeout_ms,
        connect_timeout_ms=_parse_int(
            os.getenv("MONGO_CONNECT_TIMEOUT_MS"),
            env_name="MONGO_CONNECT_TIMEOUT_MS",
            default=10000,
        ),
        socket_timeout_ms=_parse_int(
            os.getenv("MONGO_SOCKET_TIMEOUT_MS"),
            env_name="MONGO_SOCKET_TIMEOUT_MS",
            default=30000,
        ),
        tls=_parse_bool(
            os.getenv("MONGO_TLS"),
            env_name="MONGO_TLS",
            default=None,
        ),
        tls_allow_invalid_certificates=_parse_bool(
            os.getenv("MONGO_TLS_ALLOW_INVALID_CERTIFICATES"),
            env_name="MONGO_TLS_ALLOW_INVALID_CERTIFICATES",
            default=None,
        ),
        tls_ca_file=_optional_str("MONGO_TLS_CA_FILE"),
        direct_connection=_parse_bool(
            os.getenv("MONGO_DIRECT_CONNECTION"),
            env_name="MONGO_DIRECT_CONNECTION",
            default=None,
        ),
    )
    benchmark_config = BenchmarkConfig(
        latency_iterations=int(os.getenv("PERF_LATENCY_ITERATIONS", "200")),
        load_total_requests=int(os.getenv("PERF_LOAD_TOTAL_REQUESTS", "2000")),
        concurrency_levels=_parse_concurrency_levels(
            os.getenv("PERF_CONCURRENCY_LEVELS", "1,4,8,16")
        ),
        random_seed=int(os.getenv("PERF_RANDOM_SEED", "1729")),
        output_dir=output_dir,
    )
    return resp_config, mongo_config, benchmark_config
