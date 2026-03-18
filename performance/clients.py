from __future__ import annotations

from io import BufferedReader

from .connection import connect_miniredis, connect_mongodb
from .config import MongoConfig, RespConfig


def encode_resp_command(*parts: str) -> bytes:
    message = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        encoded_part = part.encode()
        message.append(f"${len(encoded_part)}\r\n".encode())
        message.append(encoded_part + b"\r\n")
    return b"".join(message)


def _read_resp_line(reader: BufferedReader) -> bytes:
    line = reader.readline()
    if not line:
        raise RuntimeError("connection closed while reading RESP line")
    if not line.endswith(b"\r\n"):
        raise RuntimeError(f"invalid RESP line ending: {line!r}")
    return line[:-2]


def read_resp_frame(reader: BufferedReader) -> str | None:
    prefix = reader.read(1)
    if not prefix:
        raise RuntimeError("connection closed while reading RESP frame")
    if prefix == b"+":
        return _read_resp_line(reader).decode()
    if prefix == b"$":
        length = int(_read_resp_line(reader).decode())
        if length == -1:
            return None
        payload = reader.read(length)
        if len(payload) != length:
            raise RuntimeError("connection closed while reading RESP bulk string")
        trailer = reader.read(2)
        if trailer != b"\r\n":
            raise RuntimeError(f"invalid RESP bulk string trailer: {trailer!r}")
        return payload.decode()
    if prefix == b":":
        return _read_resp_line(reader).decode()
    if prefix == b"-":
        error_text = _read_resp_line(reader).decode()
        raise RuntimeError(f"RESP server returned an error: {error_text}")
    if prefix == b"H":
        raise RuntimeError("server is speaking HTTP, not RESP")
    raise RuntimeError(f"unsupported RESP frame prefix: {prefix!r}")


class RespBenchmarkClient:
    def __init__(self, config: RespConfig, timeout_seconds: float = 3.0) -> None:
        self._connection = connect_miniredis(config, timeout_seconds=timeout_seconds)

    def __enter__(self) -> "RespBenchmarkClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def set_value(self, key: str, value: str) -> None:
        self._connection.sock.sendall(encode_resp_command("SET", key, value))
        response = read_resp_frame(self._connection.reader)
        if response != "OK":
            raise RuntimeError(f"unexpected RESP SET response: {response!r}")

    def get_value(self, key: str) -> str | None:
        self._connection.sock.sendall(encode_resp_command("GET", key))
        response = read_resp_frame(self._connection.reader)
        return None if response is None else str(response)

    def close(self) -> None:
        self._connection.close()


class MongoBenchmarkClient:
    def __init__(self, config: MongoConfig, timeout_ms: int = 3000) -> None:
        self._connection = connect_mongodb(config, timeout_ms=timeout_ms)

    def __enter__(self) -> "MongoBenchmarkClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def set_value(self, key: str, value: str) -> None:
        self._connection.collection.replace_one(
            {"_id": key},
            {"_id": key, "value": value},
            upsert=True,
        )

    def get_value(self, key: str) -> str | None:
        document = self._connection.collection.find_one({"_id": key}, {"_id": 0, "value": 1})
        if document is None:
            return None
        return str(document["value"])

    def close(self) -> None:
        self._connection.close()
