ALLOWED_RESPONSE_TYPES = {
    "simple_string",
    "bulk_string",
    "null",
    "integer",
    "error",
}


class ProtocolResponseError(ValueError):
    """내부 응답 dict를 프로토콜 응답으로 바꿀 수 없을 때 사용하는 예외."""


def encode_response(result: dict) -> str:
    """
    `redis.execute()`가 반환한 dict를 실제 TCP로 내보낼 문자열로 바꾼다.

    현재 최소 구현에서는 Redis RESP와 비슷한 형식을 사용한다.

    지원 규칙:
    - simple_string -> +OK\\r\\n
    - bulk_string   -> $5\\r\\nhello\\r\\n
    - null          -> $-1\\r\\n
    - integer       -> :1\\r\\n
    - error         -> -unknown command\\r\\n

    이 함수의 목적은 "코어 응답 규격"과 "네트워크 응답 규격"을 분리하는 것이다.
    즉, 코어는 dict만 알면 되고, 서버는 이 함수를 통해 문자열 응답을 만든다.
    """

    # 코어가 약속한 최소 형식은 {"type": ..., "value": ...} 이므로
    # 먼저 필수 키가 있는지부터 확인한다.
    if "type" not in result or "value" not in result:
        raise ProtocolResponseError("response must include 'type' and 'value'")

    response_type = result["type"]
    value = result["value"]

    # 서버가 모르는 type을 그대로 흘려보내면 클라이언트 입장에서
    # 이상한 응답을 받게 되므로, 허용된 타입만 통과시킨다.
    if response_type not in ALLOWED_RESPONSE_TYPES:
        raise ProtocolResponseError(f"unsupported response type: {response_type}")

    # simple string은 사람이 읽기 쉬운 성공 메시지에 쓰인다.
    if response_type == "simple_string":
        if not isinstance(value, str):
            raise ProtocolResponseError("simple_string value must be str")
        return f"+{value}\r\n"

    # bulk string은 일반 문자열 데이터를 보낼 때 사용한다.
    # 길이 정보를 먼저 보내고, 다음 줄에 실제 값을 보낸다.
    if response_type == "bulk_string":
        if not isinstance(value, str):
            raise ProtocolResponseError("bulk_string value must be str")
        return f"${len(value)}\r\n{value}\r\n"

    # null은 "값이 없음"을 나타내는 고정 표현으로 보낸다.
    if response_type == "null":
        if value is not None:
            raise ProtocolResponseError("null value must be None")
        return "$-1\r\n"

    # integer는 Redis 스타일처럼 ':' 접두어를 붙여 보낸다.
    if response_type == "integer":
        if not isinstance(value, int):
            raise ProtocolResponseError("integer value must be int")
        return f":{value}\r\n"

    # error는 '-' 접두어를 붙여 클라이언트가 에러 응답임을 구분하게 한다.
    if response_type == "error":
        if not isinstance(value, str):
            raise ProtocolResponseError("error value must be str")
        return f"-{value}\r\n"

    # 위에서 허용 타입을 모두 검사했기 때문에 여기까지 오면
    # 내부적으로 예상하지 못한 분기 누락이 있는 것이다.
    raise ProtocolResponseError(f"unreachable response type: {response_type}")

