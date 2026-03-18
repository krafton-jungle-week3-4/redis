import unittest
from io import BytesIO

from core_state import clear_all_stores
from error_contract import ERR_INTERNAL_SERVER
from server import handle_client_connection


class FakeSocket:
    def __init__(self, payload: bytes) -> None:
        self._reader = BytesIO(payload)
        self.written = bytearray()
        self.closed = False

    def makefile(self, mode: str) -> BytesIO:
        return self._reader

    def sendall(self, data: bytes) -> None:
        self.written.extend(data)

    def close(self) -> None:
        self.closed = True


class ServerProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_all_stores()

    def test_malformed_resp_returns_error_and_connection_stays_alive(self) -> None:
        fake_socket = FakeSocket(b"*1\r\n$-1\r\nPING\n")

        def execute(command: list[str]) -> dict:
            if command == ["PING"]:
                return {"type": "simple_string", "value": "PONG"}
            return {"type": "error", "value": "unexpected command"}

        handle_client_connection(fake_socket, execute)

        self.assertEqual(
            fake_socket.written.decode("utf-8"),
            "-RESP bulk string length must be non-negative\r\n+PONG\r\n",
        )
        self.assertTrue(fake_socket.closed)

    def test_internal_execute_exception_returns_error_and_next_command_still_works(self) -> None:
        fake_socket = FakeSocket(b"PING\nPING\n")
        calls = {"count": 0}

        def execute(command: list[str]) -> dict:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("boom")
            return {"type": "simple_string", "value": "PONG"}

        handle_client_connection(fake_socket, execute)

        self.assertEqual(
            fake_socket.written.decode("utf-8"),
            f"-{ERR_INTERNAL_SERVER}\r\n+PONG\r\n",
        )
        self.assertTrue(fake_socket.closed)
