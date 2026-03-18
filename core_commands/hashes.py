"""Hash command handlers for redis.py core."""

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


def _resolve_stores(
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_or_hash_store: dict[str, dict[str, float]] | dict[str, dict[str, str]],
    hash_store: dict[str, dict[str, str]] | None,
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, str]]]:
    # 하위 호환: 기존 호출은 (string, set, list, hash_store) 형태
    if hash_store is None:
        return {}, zset_or_hash_store  # type: ignore[return-value]
    return zset_or_hash_store, hash_store  # type: ignore[return-value]


def _get_hash_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
    hash_store: dict[str, dict[str, str]],
) -> dict[str, str] | None | str:
    if key in string_store or key in set_store or key in list_store or key in zset_store:
        return ERR_WRONG_TYPE_HASH
    return hash_store.get(key)


def _ensure_hash_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
    hash_store: dict[str, dict[str, str]],
) -> dict[str, str] | str:
    if key in string_store or key in set_store or key in list_store or key in zset_store:
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
    zset_or_hash_store: dict[str, dict[str, float]] | dict[str, dict[str, str]],
    hash_store: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any] | None:
    zset_store, resolved_hash_store = _resolve_stores(
        string_store,
        set_store,
        list_store,
        zset_or_hash_store,
        hash_store,
    )

    if command_name == "HSET":
        key = command[1]
        field = command[2]
        value = command[3]
        fields = _ensure_hash_entry(key, string_store, set_store, list_store, zset_store, resolved_hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        is_new_field = field not in fields
        fields[field] = value
        return {"type": "integer", "value": 1 if is_new_field else 0}

    if command_name == "HGET":
        key = command[1]
        field = command[2]
        fields = _get_hash_entry(key, string_store, set_store, list_store, zset_store, resolved_hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        if fields is None or field not in fields:
            return {"type": "null", "value": None}
        return {"type": "bulk_string", "value": fields[field]}

    if command_name == "HDEL":
        key = command[1]
        field = command[2]
        fields = _get_hash_entry(key, string_store, set_store, list_store, zset_store, resolved_hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        if fields is None or field not in fields:
            return {"type": "integer", "value": 0}
        fields.pop(field)
        if not fields:
            resolved_hash_store.pop(key, None)
        return {"type": "integer", "value": 1}

    if command_name == "HGETALL":
        key = command[1]
        fields = _get_hash_entry(key, string_store, set_store, list_store, zset_store, resolved_hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        if fields is None:
            return {"type": "array", "value": []}
        values: list[str] = []
        for field in sorted(fields):
            values.extend([field, fields[field]])
        return {"type": "array", "value": values}

    if command_name == "HEXISTS":
        key = command[1]
        field = command[2]
        fields = _get_hash_entry(key, string_store, set_store, list_store, zset_store, resolved_hash_store)
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

        fields = _ensure_hash_entry(key, string_store, set_store, list_store, zset_store, resolved_hash_store)
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
        fields = _get_hash_entry(key, string_store, set_store, list_store, zset_store, resolved_hash_store)
        if isinstance(fields, str):
            return {"type": "error", "value": fields}
        return {"type": "integer", "value": 0 if fields is None else len(fields)}

    return None
