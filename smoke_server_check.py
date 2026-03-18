import socket
import threading
import time

from mock_execute import execute
from server import run_server


HOST = "127.0.0.1"
PORT = 6380


def send_command(raw: str) -> str:
    """
    테스트용 클라이언트가 서버에 한 줄 명령을 보내고,
    서버가 돌려준 응답 문자열을 그대로 받는 함수이다.

    일부러 아주 단순하게 만들어서,
    "요청이 들어가고 응답이 나오는가"만 눈으로 확인하기 쉽게 했다.
    """

    with socket.create_connection((HOST, PORT), timeout=2) as client:
        # 서버는 한 줄 단위 요청을 기대하므로 줄바꿈을 붙여 보낸다.
        client.sendall(f"{raw}\n".encode("utf-8"))
        return client.recv(1024).decode("utf-8")


def main() -> None:
    """
    임시 execute를 사용해 서버 배선이 살아 있는지 확인하는 가장 작은 실행 스크립트이다.

    이 스크립트는 아래를 확인한다.
    - 서버가 실제로 포트를 열 수 있는지
    - 요청 문자열이 parser -> execute -> response encoder를 거치는지
    - 클라이언트가 응답을 다시 받을 수 있는지
    """

    # 서버는 무한 루프이기 때문에 별도 스레드에서 켠다.
    # 여기서는 데모/확인용이므로 daemon 스레드로 두고, 메인 함수가 끝나면 함께 종료되게 한다.
    thread = threading.Thread(
        target=run_server,
        args=(execute,),
        kwargs={"host": HOST, "port": PORT},
        daemon=True,
    )
    thread.start()

    # 서버가 bind/listen을 끝낼 시간을 아주 잠깐 준다.
    time.sleep(0.2)

    print("PING  ->", repr(send_command("PING")))
    print("ECHO  ->", repr(send_command("ECHO hello")))
    print("ERROR ->", repr(send_command("SET name redis")))


if __name__ == "__main__":
    main()
