from __future__ import annotations

from dataclasses import dataclass
from io import BufferedReader
import socket
from typing import Any

from .config import MongoConfig, RespConfig, load_config


@dataclass
class MiniRedisConnection:
    sock: socket.socket
    reader: BufferedReader

    def __enter__(self) -> "MiniRedisConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self.reader.close()
        self.sock.close()


@dataclass
class MongoConnection:
    client: Any
    collection: Any

    def __enter__(self) -> "MongoConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self.client.close()


def connect_miniredis(
    config: RespConfig | None = None,
    timeout_seconds: float = 3.0,
) -> MiniRedisConnection:
    resolved_config = config or load_connection_configs()[0]
    sock = socket.create_connection(
        (resolved_config.host, resolved_config.port),
        timeout=timeout_seconds,
    )
    return MiniRedisConnection(sock=sock, reader=sock.makefile("rb"))


def connect_mongodb(
    config: MongoConfig | None = None,
    timeout_ms: int = 3000,
) -> MongoConnection:
    resolved_config = config or load_connection_configs()[1]

    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise RuntimeError(
            "pymongo is not installed. Install performance/requirements.txt first."
        ) from exc

    client = MongoClient(
        resolved_config.uri,
        serverSelectionTimeoutMS=timeout_ms,
    )
    client.admin.command("ping")
    collection = client[resolved_config.database][resolved_config.collection]
    return MongoConnection(client=client, collection=collection)


def load_connection_configs() -> tuple[RespConfig, MongoConfig]:
    resp_config, mongo_config, _ = load_config()
    return resp_config, mongo_config


def connect_all(
    resp_config: RespConfig | None = None,
    mongo_config: MongoConfig | None = None,
    resp_timeout_seconds: float = 3.0,
    mongo_timeout_ms: int = 3000,
) -> tuple[MiniRedisConnection, MongoConnection]:
    return (
        connect_miniredis(resp_config, timeout_seconds=resp_timeout_seconds),
        connect_mongodb(mongo_config, timeout_ms=mongo_timeout_ms),
    )
