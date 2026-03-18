from __future__ import annotations

from io import BufferedReader


class ProtocolParseError(ValueError):
    """프로토콜 입력을 명령 토큰으로 바꿀 수 없을 때 사용하는 예외."""


def _normalize_tokens(tokens: list[str]) -> list[str]:
    if not tokens:
        raise ProtocolParseError("empty command")
    tokens[0] = tokens[0].upper()
    return tokens


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

    stripped = raw.strip()
    if not stripped:
        raise ProtocolParseError("empty command")

    return _normalize_tokens(stripped.split())


def _read_resp_header(line: bytes, expected_prefix: bytes, error_message: str) -> int:
    if not line.endswith(b"\r\n"):
        raise ProtocolParseError(error_message)

    header = line[:-2]
    if not header.startswith(expected_prefix):
        raise ProtocolParseError(error_message)

    try:
        return int(header[1:].decode("utf-8"))
    except ValueError as exc:
        raise ProtocolParseError(error_message) from exc


def _read_resp_bulk_string(reader: BufferedReader) -> str:
    length_line = reader.readline()
    if not length_line:
        raise ProtocolParseError("connection closed while reading RESP bulk length")

    length = _read_resp_header(length_line, b"$", "invalid RESP bulk string header")
    if length < 0:
        raise ProtocolParseError("RESP bulk string length must be non-negative")

    payload = reader.read(length)
    if len(payload) != length:
        raise ProtocolParseError("connection closed while reading RESP bulk string")

    trailer = reader.read(2)
    if trailer != b"\r\n":
        raise ProtocolParseError("invalid RESP bulk string trailer")

    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolParseError("RESP bulk string must be valid UTF-8") from exc


def parse_resp_array(reader: BufferedReader, first_line: bytes) -> list[str]:
    """
    RESP 배열 형식 요청을 읽어 `list[str]`로 바꾼다.

    최소 구현에서는 명령과 모든 인자가 bulk string이어야 한다.
    """

    item_count = _read_resp_header(first_line, b"*", "invalid RESP array header")
    if item_count <= 0:
        raise ProtocolParseError("empty command")

    return _normalize_tokens([_read_resp_bulk_string(reader) for _ in range(item_count)])


def read_command(reader: BufferedReader) -> list[str] | None:
    """
    소켓 스트림에서 다음 명령 1개를 읽어 `list[str]`로 반환한다.

    지원 형식:
    - inline text: `PING\\n`
    - RESP array : `*1\\r\\n$4\\r\\nPING\\r\\n`
    """

    first_line = reader.readline()
    if first_line == b"":
        return None

    if first_line.startswith(b"*"):
        return parse_resp_array(reader, first_line)

    try:
        return parse_command_line(first_line.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ProtocolParseError("command must be valid UTF-8") from exc
