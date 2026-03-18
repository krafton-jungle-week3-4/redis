from error_contract import ERR_EMPTY_COMMAND


class ProtocolParseError(ValueError):
    """프로토콜 입력을 명령 토큰으로 바꿀 수 없을 때 사용하는 예외."""


def parse_command_line(raw: str) -> list[str]:
    """
    클라이언트가 보낸 raw 한 줄을 `redis.execute()`에 넘길 수 있는
    `list[str]` 형태로 바꾼다.

    현재 최소 구현 기준 규칙:
    - 요청 1개는 1줄이다.
    - 앞뒤 공백은 무시한다.
    - 토큰 사이 공백이 여러 개여도 하나의 구분자로 본다.
    - 따옴표로 감싼 문자열은 아직 지원하지 않는다.
    - 명령어는 대문자로 정규화한다.
    - 빈 줄 또는 공백만 있는 줄은 에러다.
    """

    # 서버는 소켓에서 읽은 "원본 문자열"을 그대로 받을 수 있으므로,
    # 먼저 줄 끝 개행과 양쪽 공백을 정리한다.
    stripped = raw.strip()

    # 아무 내용도 남지 않으면 유효한 명령이 아니므로
    # 상위 서버 레이어가 에러 응답을 만들 수 있게 예외를 올린다.
    if not stripped:
        raise ProtocolParseError(ERR_EMPTY_COMMAND)

    # 최소 구현에서는 공백 기반 토큰화만 지원한다.
    # split()을 인자 없이 쓰면 공백 여러 개를 자연스럽게 하나처럼 처리해 준다.
    tokens = stripped.split()

    # 명령어 이름은 대소문자 차이 없이 처리하기 위해 첫 토큰만 대문자로 바꾼다.
    # 나머지 인자들은 사용자가 보낸 값을 그대로 유지해야 하므로 손대지 않는다.
    tokens[0] = tokens[0].upper()
    return tokens

