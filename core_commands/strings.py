"""String command handlers for redis.py core."""

from typing import Any


FIXED_ARITY: dict[str, int] = {
    "PING": 1,
    "ECHO": 2,
    "SET": 3,
    "GET": 2,
    "DEL": 2,
    "EXISTS": 2,
    "TYPE": 2,
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


def execute_string_command(command_name: str, command: list[str], store: dict[str, str]) -> dict[str, Any] | None:
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

    if command_name == "INCR":
        key = command[1]
        current = store.get(key)
        if current is None:
            store[key] = "1"
            return {"type": "integer", "value": 1}
        parsed = _parse_int(current)
        if parsed is None:
            return {"type": "error", "value": "ERR value is not an integer or out of range"}
        next_value = parsed + 1
        store[key] = str(next_value)
        return {"type": "integer", "value": next_value}

    if command_name == "DECR":
        key = command[1]
        current = store.get(key)
        if current is None:
            store[key] = "-1"
            return {"type": "integer", "value": -1}
        parsed = _parse_int(current)
        if parsed is None:
            return {"type": "error", "value": "ERR value is not an integer or out of range"}
        next_value = parsed - 1
        store[key] = str(next_value)
        return {"type": "integer", "value": next_value}

    if command_name == "MSET":
        for index in range(1, len(command), 2):
            key = command[index]
            value = command[index + 1]
            store[key] = value
        return {"type": "simple_string", "value": "OK"}

    if command_name == "MGET":
        keys = command[1:]
        values = [store.get(key) for key in keys]
        return {"type": "array", "value": values}

    return None
