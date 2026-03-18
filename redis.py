"""AGENTS.md의 2번(redis.py 코어) 1단계 계약 고정용 모듈.

현재 파일의 목표는 "완전한 Redis 기능 구현"이 아니라,
아래 인터페이스 계약을 코드로 먼저 잠그는 것입니다.

    execute(command: list[str]) -> dict
"""

from typing import Literal, TypedDict


# 응답 dict의 type 필드는 아래 5가지만 허용한다.
# (AGENTS.md의 Allowed Response Types를 그대로 반영)
ResponseType = Literal["simple_string", "bulk_string", "null", "integer", "error"]


class RedisResponse(TypedDict):
    type: ResponseType
    value: str | int | None


# 1단계에서는 저장소도 "문자열 키/문자열 값" 단일 dict로 고정한다.
# 실제 명령 구현(SET/GET/DEL...)은 다음 단계에서 채운다.
store: dict[str, str] = {}


def _error(message: str) -> RedisResponse:
    """명령 레벨 오류를 항상 같은 포맷으로 감싼다."""
    return {"type": "error", "value": message}


def execute(command: list[str]) -> RedisResponse:
    """코어 진입점.

    계약(중요):
    1) 입력은 이미 파싱된 list[str] 여야 한다. (raw 문자열 금지)
    2) 출력은 항상 {"type": ..., "value": ...} dict 여야 한다.
    3) 네트워크/소켓/프로토콜 직렬화는 여기서 하지 않는다.
    """
    # 빈 명령은 예외를 던지지 않고, 합의된 error 응답으로 반환한다.
    if not command:
        return _error("empty command")

    # 명령어 대소문자 차이를 없애기 위해 단일 규칙(upper)만 사용한다.
    command_name = command[0].upper()

    # 각 명령의 arity(토큰 개수)를 먼저 고정한다.
    # 이 규칙은 명령 동작보다 우선이며, 틀리면 즉시 오류를 반환한다.
    expected_arity: dict[str, int] = {
        "PING": 1,
        "ECHO": 2,
        "SET": 3,
        "GET": 2,
        "DEL": 2,
        "EXISTS": 2,
        "TYPE": 2,
    }

    if command_name in expected_arity and len(command) != expected_arity[command_name]:
        return _error("wrong number of arguments")

    # 이제부터는 계약된 최소 명령만 처리한다.
    if command_name == "PING":
        return {"type": "simple_string", "value": "PONG"}

    if command_name == "ECHO":
        return {"type": "bulk_string", "value": command[1]}

    if command_name == "SET":
        key = command[1]
        value = command[2]
        store[key] = value
        return {"type": "simple_string", "value": "OK"}

    if command_name == "GET":
        key = command[1]
        if key not in store:
            return {"type": "null", "value": None}
        return {"type": "bulk_string", "value": store[key]}

    if command_name == "DEL":
        key = command[1]
        deleted = 1 if key in store else 0
        store.pop(key, None)
        return {"type": "integer", "value": deleted}

    if command_name == "EXISTS":
        key = command[1]
        return {"type": "integer", "value": 1 if key in store else 0}

    if command_name == "TYPE":
        key = command[1]
        if key in store:
            return {"type": "bulk_string", "value": "string"}
        return {"type": "bulk_string", "value": "none"}

    # 지원하지 않는 명령도 예외 대신 표준 error dict로 응답한다.
    return _error("unknown command")
