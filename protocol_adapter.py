from collections.abc import Callable

from protocol_parser import ProtocolParseError, parse_command_line
from protocol_response import ProtocolResponseError, encode_response


def handle_raw_command(raw: str, execute: Callable[[list[str]], dict]) -> str:
    """
    서버가 클라이언트에게서 받은 raw 입력 한 줄을 최종 프로토콜 응답 문자열로 바꾼다.

    이 함수는 1번 담당의 책임 경계를 코드로 분리해 놓은 어댑터다.

    처리 순서:
    1. raw 문자열을 명령 토큰(list[str])으로 파싱한다.
    2. 파싱된 명령을 `redis.execute()`에 전달한다.
    3. 코어가 돌려준 dict를 실제 wire response 문자열로 직렬화한다.

    중요한 점:
    - 서버는 명령 의미를 직접 해석하지 않는다.
    - 코어는 raw 문자열이나 네트워크 형식을 몰라도 된다.
    - 이 함수는 두 레이어를 연결하는 "얇은 접착제" 역할만 한다.
    """

    try:
        # 1번 담당은 raw 입력을 코어가 이해할 수 있는 명령 토큰으로만 바꾼다.
        command = parse_command_line(raw)
    except ProtocolParseError as exc:
        # 파싱 자체가 실패한 경우에는 코어까지 내려보내지 않고,
        # 서버 레이어에서 바로 에러 응답을 만든다.
        return encode_response({"type": "error", "value": str(exc)})

    # 여기서부터는 코어의 책임 구간이다.
    # 서버는 명령의 의미를 몰라도 되므로, 파싱된 리스트를 그대로 넘긴다.
    result = execute(command)

    try:
        # 코어가 반환한 내부 응답 dict를 실제 프로토콜 문자열로 바꾼다.
        return encode_response(result)
    except ProtocolResponseError as exc:
        # 응답 직렬화 단계에서 문제가 생기면 서버가 그대로 죽지 않도록
        # 안전한 에러 응답으로 감싸서 반환한다.
        return encode_response({"type": "error", "value": str(exc)})

