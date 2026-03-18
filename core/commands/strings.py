"""String command handlers for redis.py core."""

from typing import Any

from core.core_state import key_type, string_value
from core.error_contract import ERR_VALUE_NOT_INTEGER, ERR_WRONG_TYPE_STRING


FIXED_ARITY: dict[str, int] = {
    "PING": 1,
    "ECHO": 2,
    "SET": 3,
    "GET": 2,
    "INCR": 2,
    "DECR": 2,
}


def has_wrong_variable_arity(command_name: str, command: list[str]) -> bool:
    if command_name == "MSET":
        return len(command) < 3 or (len(command) - 1) % 2 != 0
    if command_name == "MGET":
        return len(command) < 2
    return False


def _parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def execute_string_command(
    command_name: str,
    command: list[str],
    store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
) -> dict[str, Any] | None:
    if command_name == "PING":
        return {"type": "simple_string", "value": "PONG"}

    if command_name == "ECHO":
        return {"type": "bulk_string", "value": command[1]}

    if command_name == "SET":
        key = command[1]
        value = command[2]
        set_store.pop(key, None)
        list_store.pop(key, None)
        zset_store.pop(key, None)
        store[key] = value
        return {"type": "simple_string", "value": "OK"}

    if command_name == "GET":
        key = command[1]
        if key_type(key) not in {"none", "string"}:
            return {"type": "error", "value": ERR_WRONG_TYPE_STRING}
        value = string_value(key)
        if value is None:
            return {"type": "null", "value": None}
        return {"type": "bulk_string", "value": value}

    if command_name == "INCR":
        key = command[1]
        if key in set_store or key in list_store or key in zset_store:
            return {"type": "error", "value": ERR_WRONG_TYPE_STRING}
        current = store.get(key)
        if current is None:
            store[key] = "1"
            return {"type": "integer", "value": 1}
        parsed = _parse_int(current)
        if parsed is None:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}
        next_value = parsed + 1
        store[key] = str(next_value)
        return {"type": "integer", "value": next_value}

    if command_name == "DECR":
        key = command[1]
        if key in set_store or key in list_store or key in zset_store:
            return {"type": "error", "value": ERR_WRONG_TYPE_STRING}
        current = store.get(key)
        if current is None:
            store[key] = "-1"
            return {"type": "integer", "value": -1}
        parsed = _parse_int(current)
        if parsed is None:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}
        next_value = parsed - 1
        store[key] = str(next_value)
        return {"type": "integer", "value": next_value}

    if command_name == "MSET":
        for index in range(1, len(command), 2):
            key = command[index]
            value = command[index + 1]
            set_store.pop(key, None)
            list_store.pop(key, None)
            zset_store.pop(key, None)
            store[key] = value
        return {"type": "simple_string", "value": "OK"}

    if command_name == "MGET":
        keys = command[1:]
        for key in keys:
            if key_type(key) not in {"none", "string"}:
                return {"type": "error", "value": ERR_WRONG_TYPE_STRING}
        values = [string_value(key) for key in keys]
        return {"type": "array", "value": values}

    return None
