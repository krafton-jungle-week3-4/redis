from __future__ import annotations

from dataclasses import dataclass
from io import BufferedReader
import socket
import ssl
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
    timeout_seconds: float | None = None,
) -> MiniRedisConnection:
    resolved_config = config or load_connection_configs()[0]
    connect_timeout_seconds = (
        resolved_config.connect_timeout_seconds
        if timeout_seconds is None
        else timeout_seconds
    )
    sock = socket.create_connection(
        (resolved_config.host, resolved_config.port),
        timeout=connect_timeout_seconds,
    )

    if resolved_config.tcp_nodelay:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if resolved_config.keepalive:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    wrapped_socket = sock
    if resolved_config.use_tls:
        ssl_context = ssl.create_default_context(cafile=resolved_config.tls_ca_file)
        if not resolved_config.tls_verify:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        server_hostname = resolved_config.tls_server_hostname
        if resolved_config.tls_verify and server_hostname is None:
            server_hostname = resolved_config.host
        wrapped_socket = ssl_context.wrap_socket(sock, server_hostname=server_hostname)

    if resolved_config.socket_timeout_seconds is not None:
        wrapped_socket.settimeout(resolved_config.socket_timeout_seconds)

    return MiniRedisConnection(sock=wrapped_socket, reader=wrapped_socket.makefile("rb"))


def connect_mongodb(
    config: MongoConfig | None = None,
    timeout_ms: int | None = None,
) -> MongoConnection:
    resolved_config = config or load_connection_configs()[1]

    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise RuntimeError(
            "pymongo is not installed. Install performance/requirements.txt first."
        ) from exc

    client_options: dict[str, Any] = {
        "serverSelectionTimeoutMS": (
            resolved_config.server_selection_timeout_ms
            if timeout_ms is None
            else timeout_ms
        ),
        "appname": resolved_config.app_name,
    }
    if resolved_config.connect_timeout_ms is not None:
        client_options["connectTimeoutMS"] = resolved_config.connect_timeout_ms
    if resolved_config.socket_timeout_ms is not None:
        client_options["socketTimeoutMS"] = resolved_config.socket_timeout_ms
    if resolved_config.tls is not None:
        client_options["tls"] = resolved_config.tls
    if resolved_config.tls_allow_invalid_certificates is not None:
        client_options["tlsAllowInvalidCertificates"] = resolved_config.tls_allow_invalid_certificates
    if resolved_config.tls_ca_file is not None:
        client_options["tlsCAFile"] = resolved_config.tls_ca_file
    if resolved_config.direct_connection is not None:
        client_options["directConnection"] = resolved_config.direct_connection

    client = MongoClient(resolved_config.uri, **client_options)
    client.admin.command("ping")
    collection = client[resolved_config.database][resolved_config.collection]
    return MongoConnection(client=client, collection=collection)


def load_connection_configs() -> tuple[RespConfig, MongoConfig]:
    resp_config, mongo_config, _ = load_config()
    return resp_config, mongo_config


def connect_all(
    resp_config: RespConfig | None = None,
    mongo_config: MongoConfig | None = None,
    resp_timeout_seconds: float | None = None,
    mongo_timeout_ms: int | None = None,
) -> tuple[MiniRedisConnection, MongoConnection]:
    return (
        connect_miniredis(resp_config, timeout_seconds=resp_timeout_seconds),
        connect_mongodb(mongo_config, timeout_ms=mongo_timeout_ms),
    )
