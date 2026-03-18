import socket
from collections.abc import Callable

from protocol_adapter import handle_raw_command


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6379


def handle_client_connection(client_socket: socket.socket, execute: Callable[[list[str]], dict]) -> None:
    """
    클라이언트 연결 1개를 처리한다.

    현재 최소 구현 기준에서는 "한 줄 요청 -> 한 줄 응답" 흐름으로 처리한다.
    연결은 유지한 채 여러 줄 요청을 받을 수 있지만,
    각 요청은 반드시 줄바꿈 단위로 구분된다고 가정한다.
    """

    # socket.makefile()을 쓰면 바이트 스트림을 "한 줄씩 읽는 파일"처럼 다룰 수 있다.
    # 지금 단계에서는 요청 1개가 1줄이라는 규칙이 있으므로 이 방식이 가장 단순하다.
    reader = client_socket.makefile("r", encoding="utf-8", newline="\n")

    try:
        while True:
            raw_line = reader.readline()

            # readline()이 빈 문자열을 돌려주면 상대가 연결을 끊은 것이다.
            if raw_line == "":
                break

            # protocol_adapter는
            # 1) raw 문자열 파싱
            # 2) execute 호출
            # 3) 응답 직렬화
            # 를 한 번에 이어주는 얇은 연결 계층이다.
            response = handle_raw_command(raw_line, execute)

            # 문자열 응답을 바이트로 바꿔 실제 소켓으로 보낸다.
            client_socket.sendall(response.encode("utf-8"))
    finally:
        # 예외가 나더라도 연결은 정리해 주는 편이 안전하다.
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
            handle_client_connection(client_socket, execute)
    finally:
        server_socket.close()


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
    run_server(_load_execute())

