import socket
import os
from collections.abc import Callable
from threading import Thread

from error_contract import ERR_INTERNAL_SERVER
from protocol_parser import ProtocolParseError, read_command
from protocol_response import ProtocolResponseError, encode_response


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 6379


def handle_client_connection(client_socket: socket.socket, execute: Callable[[list[str]], dict]) -> None:
    """
    클라이언트 연결 1개를 처리한다.

    현재 최소 구현 기준에서는 "한 줄 요청 -> 한 줄 응답" 흐름으로 처리한다.
    연결은 유지한 채 여러 줄 요청을 받을 수 있지만,
    각 요청은 반드시 줄바꿈 단위로 구분된다고 가정한다.
    """

    # RESP는 길이 기반 프레임을 포함하므로 바이너리 모드로 읽는다.
    reader = client_socket.makefile("rb")

    try:
        while True:
            try:
                command = read_command(reader)
            except ProtocolParseError as exc:
                client_socket.sendall(
                    encode_response({"type": "error", "value": str(exc)}).encode("utf-8")
                )
                continue

            if command is None:
                break

            try:
                result = execute(command)
            except Exception:
                client_socket.sendall(
                    encode_response({"type": "error", "value": ERR_INTERNAL_SERVER}).encode("utf-8")
                )
                continue

            try:
                response = encode_response(result)
            except ProtocolResponseError as exc:
                response = encode_response({"type": "error", "value": str(exc)})

            client_socket.sendall(response.encode("utf-8"))
    finally:
        reader.close()
        client_socket.close()


def run_server(
    execute: Callable[[list[str]], dict],
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
) -> None:
    """
    최소 TCP 서버를 실행한다.

    이 함수는 1번 담당의 실제 서버 루프 역할을 한다.
    다만 명령의 의미는 전혀 모르고, 오직 execute 함수만 호출한다.
    """

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 개발 중 서버를 껐다 켰을 때 포트가 바로 재사용되도록 옵션을 켠다.
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen()

    print(f"mini-redis server is listening on {host}:{port}")

    try:
        while True:
            client_socket, client_address = server_socket.accept()
            print(f"client connected: {client_address}")
            thread = Thread(
                target=handle_client_connection,
                args=(client_socket, execute),
                daemon=True,
            )
            thread.start()
    finally:
        server_socket.close()


def _get_server_config() -> tuple[str, int]:
    host = os.getenv("MINIREDIS_HOST", DEFAULT_HOST)
    port_text = os.getenv("MINIREDIS_PORT", str(DEFAULT_PORT))

    try:
        port = int(port_text)
    except ValueError as exc:
        raise RuntimeError("MINIREDIS_PORT must be an integer.") from exc

    return host, port


def _load_execute() -> Callable[[list[str]], dict]:
    """
    나중에 `redis.py`가 준비되면 그 안의 execute 함수를 자동으로 불러온다.

    아직 `redis.py`가 없는 상태에서 실수로 서버를 실행했을 때는,
    원인을 바로 알 수 있도록 친절한 에러를 낸다.
    """

    try:
        from redis import execute
    except ImportError as exc:
        raise RuntimeError(
            "redis.py with execute(command: list[str]) -> dict is required to run the server."
        ) from exc

    return execute


if __name__ == "__main__":
    # 직접 실행할 때는 redis.py의 execute를 불러와 서버를 시작한다.
    host, port = _get_server_config()
    run_server(_load_execute(), host=host, port=port)
