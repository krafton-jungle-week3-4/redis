"""redis.py 코어에서 사용하는 해시 명령 처리 모듈."""

from typing import Any

from error_contract import ERR_VALUE_NOT_INTEGER, ERR_WRONG_TYPE_HASH


FIXED_ARITY: dict[str, int] = {
    "HSET": 4,
    "HGET": 3,
    "HDEL": 3,
    "HGETALL": 2,
    "HEXISTS": 3,
    "HINCRBY": 4,
    "HLEN": 2,
}


def _get_hash_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    hash_store: dict[str, dict[str, str]],
) -> dict[str, str] | None | str:
    """저장소에서 해시 값을 읽어옵니다.

    반환 규칙:
    - key가 없으면 None
    - key는 있지만 다른 자료형이면 타입 에러 문자열
    - 정상적인 hash면 field-value 딕셔너리
    """
    if key in string_store or key in set_store or key in list_store:
        return ERR_WRONG_TYPE_HASH
    return hash_store.get(key)


def _ensure_hash_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    hash_store: dict[str, dict[str, str]],
) -> dict[str, str] | str:
    """쓰기 전에 key가 hash인지 보장하고, 없으면 새 hash를 만듭니다."""
    if key in string_store or key in set_store or key in list_store:
        return ERR_WRONG_TYPE_HASH

    fields = hash_store.get(key)
    if fields is None:
        hash_store[key] = {}
        return hash_store[key]
    return fields


def execute_hash_command(
    command_name: str,
    command: list[str],
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    hash_store: dict[str, dict[str, str]],
) -> dict[str, Any] | None:
    """해시 명령을 실행합니다.

    이 함수가 담당하지 않는 명령이면 None을 반환해서
    상위 dispatcher(redis.py)가 다른 자료형 모듈로 넘길 수 있게 합니다.
    """
    if command_name == "HSET":
        key = command[1]
        field = command[2]
        value = command[3]
        fields = _ensure_hash_entry(key, string_store, set_store, list_store, hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}

        is_new_field = field not in fields
        fields[field] = value
        # Redis의 HSET처럼 새 필드를 만들면 1, 기존 필드 덮어쓰면 0을 반환합니다.
        return {"type": "integer", "value": 1 if is_new_field else 0}

    if command_name == "HGET":
        key = command[1]
        field = command[2]
        fields = _get_hash_entry(key, string_store, set_store, list_store, hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        if fields is None or field not in fields:
            return {"type": "null", "value": None}
        return {"type": "bulk_string", "value": fields[field]}

    if command_name == "HDEL":
        key = command[1]
        field = command[2]
        fields = _get_hash_entry(key, string_store, set_store, list_store, hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        if fields is None or field not in fields:
            return {"type": "integer", "value": 0}

        fields.pop(field)
        if not fields:
            hash_store.pop(key, None)
        return {"type": "integer", "value": 1}

    if command_name == "HGETALL":
        key = command[1]
        fields = _get_hash_entry(key, string_store, set_store, list_store, hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        if fields is None:
            return {"type": "array", "value": []}

        # 테스트와 응답 안정성을 위해 field 이름 기준으로 정렬해 반환합니다.
        values: list[str] = []
        for field in sorted(fields):
            values.extend([field, fields[field]])
        return {"type": "array", "value": values}

    if command_name == "HEXISTS":
        key = command[1]
        field = command[2]
        fields = _get_hash_entry(key, string_store, set_store, list_store, hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        return {"type": "integer", "value": 1 if fields is not None and field in fields else 0}

    if command_name == "HINCRBY":
        key = command[1]
        field = command[2]
        increment_raw = command[3]

        try:
            increment = int(increment_raw)
        except ValueError:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}

        fields = _ensure_hash_entry(key, string_store, set_store, list_store, hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}

        current_raw = fields.get(field)
        if current_raw is None:
            next_value = increment
        else:
            try:
                next_value = int(current_raw) + increment
            except ValueError:
                return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}

        fields[field] = str(next_value)
        return {"type": "integer", "value": next_value}

    if command_name == "HLEN":
        key = command[1]
        fields = _get_hash_entry(key, string_store, set_store, list_store, hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        return {"type": "integer", "value": 0 if fields is None else len(fields)}

    return None
