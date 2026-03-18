"""Sorted-set(ZSET) command handlers for redis.py core."""

from typing import Any

from error_contract import ERR_VALUE_NOT_FLOAT, ERR_VALUE_NOT_INTEGER, ERR_WRONG_TYPE_ZSET
from snapshot_manager import prepare_mutable_write


FIXED_ARITY: dict[str, int] = {
    "ZADD": 4,
    "ZSCORE": 3,
    "ZRANK": 3,
    "ZREVRANK": 3,
    "ZRANGE": 4,
    "ZREVRANGE": 4,
    "ZINCRBY": 4,
    "ZREM": 3,
    "ZCARD": 2,
}


def _parse_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def _parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def _format_score(score: float) -> str:
    return format(score, "g")


def _ordered_members(scores: dict[str, float], reverse: bool = False) -> list[str]:
    if reverse:
        items = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    else:
        items = sorted(scores.items(), key=lambda item: (item[1], item[0]))
    return [member for member, _ in items]


def _compute_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    actual_start = start if start >= 0 else length + start
    actual_stop = stop if stop >= 0 else length + stop
    actual_start = max(actual_start, 0)
    actual_stop = min(actual_stop, length - 1)

    if length == 0 or actual_start >= length or actual_start > actual_stop:
        return 0, 0

    return actual_start, actual_stop + 1


def execute_zset_command(
    command_name: str,
    command: list[str],
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
) -> dict[str, Any] | None:
    key = command[1] if len(command) > 1 else ""

    if command_name in FIXED_ARITY and (
        key in string_store or key in set_store or key in list_store
    ):
        return {"type": "error", "value": ERR_WRONG_TYPE_ZSET}

    if command_name == "ZADD":
        score = _parse_float(command[2])
        if score is None:
            return {"type": "error", "value": ERR_VALUE_NOT_FLOAT}
        member = command[3]
        if key in zset_store:
            prepare_mutable_write("zset", key)
        scores = zset_store.setdefault(key, {})
        added = 0 if member in scores else 1
        scores[member] = score
        return {"type": "integer", "value": added}

    if command_name == "ZSCORE":
        scores = zset_store.get(key)
        if scores is None:
            return {"type": "null", "value": None}
        member = command[2]
        if member not in scores:
            return {"type": "null", "value": None}
        return {"type": "bulk_string", "value": _format_score(scores[member])}

    if command_name == "ZRANK":
        scores = zset_store.get(key)
        if scores is None:
            return {"type": "null", "value": None}
        member = command[2]
        ordered = _ordered_members(scores, reverse=False)
        if member not in scores:
            return {"type": "null", "value": None}
        return {"type": "integer", "value": ordered.index(member)}

    if command_name == "ZREVRANK":
        scores = zset_store.get(key)
        if scores is None:
            return {"type": "null", "value": None}
        member = command[2]
        ordered = _ordered_members(scores, reverse=True)
        if member not in scores:
            return {"type": "null", "value": None}
        return {"type": "integer", "value": ordered.index(member)}

    if command_name == "ZRANGE":
        scores = zset_store.get(key)
        if scores is None:
            return {"type": "array", "value": []}
        start = _parse_int(command[2])
        stop = _parse_int(command[3])
        if start is None or stop is None:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}
        ordered = _ordered_members(scores, reverse=False)
        slice_start, slice_end = _compute_slice(len(ordered), start, stop)
        return {"type": "array", "value": ordered[slice_start:slice_end]}

    if command_name == "ZREVRANGE":
        scores = zset_store.get(key)
        if scores is None:
            return {"type": "array", "value": []}
        start = _parse_int(command[2])
        stop = _parse_int(command[3])
        if start is None or stop is None:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}
        ordered = _ordered_members(scores, reverse=True)
        slice_start, slice_end = _compute_slice(len(ordered), start, stop)
        return {"type": "array", "value": ordered[slice_start:slice_end]}

    if command_name == "ZINCRBY":
        increment = _parse_float(command[2])
        if increment is None:
            return {"type": "error", "value": ERR_VALUE_NOT_FLOAT}
        member = command[3]
        if key in zset_store:
            prepare_mutable_write("zset", key)
        scores = zset_store.setdefault(key, {})
        current = scores.get(member, 0.0)
        next_score = current + increment
        scores[member] = next_score
        return {"type": "bulk_string", "value": _format_score(next_score)}

    if command_name == "ZREM":
        scores = zset_store.get(key)
        if scores is None:
            return {"type": "integer", "value": 0}
        member = command[2]
        if member not in scores:
            return {"type": "integer", "value": 0}
        prepare_mutable_write("zset", key)
        scores = zset_store.get(key)
        if scores is None or member not in scores:
            return {"type": "integer", "value": 0}
        del scores[member]
        if not scores:
            zset_store.pop(key, None)
        return {"type": "integer", "value": 1}

    if command_name == "ZCARD":
        scores = zset_store.get(key)
        return {"type": "integer", "value": 0 if scores is None else len(scores)}

    return None
