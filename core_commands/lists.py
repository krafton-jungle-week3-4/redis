"""List command handlers for redis.py core."""

from typing import Any

from error_contract import ERR_VALUE_NOT_INTEGER, ERR_WRONG_TYPE_LIST


FIXED_ARITY: dict[str, int] = {
    "LPUSH": 3,
    "RPUSH": 3,
    "LPOP": 2,
    "RPOP": 2,
    "LRANGE": 4,
}


def _resolve_stores(
    store: dict[str, Any],
    set_store: dict[str, set[str]] | None,
    list_store: dict[str, list[str]] | None,
    zset_store: dict[str, dict[str, float]] | None,
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, list[str]], dict[str, dict[str, float]]]:
    if set_store is None and list_store is None and zset_store is None:
        return {}, {}, store, {}  # type: ignore[return-value]
    return store, set_store or {}, list_store or {}, zset_store or {}


def _get_list_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
) -> list[str] | None | str:
    if key in string_store or key in set_store or key in zset_store:
        return ERR_WRONG_TYPE_LIST
    return list_store.get(key)


def _ensure_list_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
) -> list[str] | str:
    if key in string_store or key in set_store or key in zset_store:
        return ERR_WRONG_TYPE_LIST

    items = list_store.get(key)
    if items is None:
        list_store[key] = []
        return list_store[key]
    return items


def _compute_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    actual_start = start if start >= 0 else length + start
    actual_stop = stop if stop >= 0 else length + stop

    actual_start = max(actual_start, 0)
    actual_stop = min(actual_stop, length - 1)

    if length == 0 or actual_start >= length or actual_start > actual_stop:
        return 0, 0

    return actual_start, actual_stop + 1


def execute_list_command(
    command_name: str,
    command: list[str],
    store: dict[str, Any],
    set_store: dict[str, set[str]] | None = None,
    list_store: dict[str, list[str]] | None = None,
    zset_store: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any] | None:
    string_store, resolved_set_store, resolved_list_store, resolved_zset_store = _resolve_stores(
        store,
        set_store,
        list_store,
        zset_store,
    )

    if command_name == "LPUSH":
        key = command[1]
        value = command[2]
        items = _ensure_list_entry(key, string_store, resolved_set_store, resolved_list_store, resolved_zset_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        items.insert(0, value)
        return {"type": "integer", "value": len(items)}

    if command_name == "RPUSH":
        key = command[1]
        value = command[2]
        items = _ensure_list_entry(key, string_store, resolved_set_store, resolved_list_store, resolved_zset_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        items.append(value)
        return {"type": "integer", "value": len(items)}

    if command_name == "LPOP":
        key = command[1]
        items = _get_list_entry(key, string_store, resolved_set_store, resolved_list_store, resolved_zset_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        if items is None or not items:
            return {"type": "null", "value": None}
        value = items.pop(0)
        if not items:
            resolved_list_store.pop(key, None)
        return {"type": "bulk_string", "value": value}

    if command_name == "RPOP":
        key = command[1]
        items = _get_list_entry(key, string_store, resolved_set_store, resolved_list_store, resolved_zset_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        if items is None or not items:
            return {"type": "null", "value": None}
        value = items.pop()
        if not items:
            resolved_list_store.pop(key, None)
        return {"type": "bulk_string", "value": value}

    if command_name == "LRANGE":
        key = command[1]
        items = _get_list_entry(key, string_store, resolved_set_store, resolved_list_store, resolved_zset_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        if items is None:
            return {"type": "array", "value": []}

        try:
            start = int(command[2])
            stop = int(command[3])
        except ValueError:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}

        slice_start, slice_end = _compute_slice(len(items), start, stop)
        return {"type": "array", "value": items[slice_start:slice_end]}

    return None
