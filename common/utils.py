import time

from fastapi import HTTPException

from .store import expiry_store, redis_store


def purge_if_expired(key: str) -> None:
    expires_at = expiry_store.get(key)
    if expires_at is not None and expires_at <= time.time():
        redis_store.pop(key, None)
        expiry_store.pop(key, None)


def key_exists(key: str) -> bool:
    purge_if_expired(key)
    return key in redis_store


def get_entry(key: str) -> dict[str, object] | None:
    purge_if_expired(key)
    return redis_store.get(key)


def get_string_value(key: str) -> str | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "string":
        raise HTTPException(status_code=400, detail="wrong type operation against non-string value")
    return entry["value"]  # type: ignore[return-value]


def set_string_entry(key: str, value: str) -> None:
    redis_store[key] = {"type": "string", "value": value}


def get_list_value(key: str) -> list[str] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "list":
        raise HTTPException(status_code=400, detail="wrong type operation against non-list value")
    return entry["value"]  # type: ignore[return-value]


def ensure_list(key: str) -> list[str]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "list", "value": []}
        return redis_store[key]["value"]  # type: ignore[return-value]
    if entry["type"] != "list":
        raise HTTPException(status_code=400, detail="wrong type operation against non-list value")
    return entry["value"]  # type: ignore[return-value]


def get_set_value(key: str) -> set[str] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "set":
        raise HTTPException(status_code=400, detail="wrong type operation against non-set value")
    return entry["value"]  # type: ignore[return-value]


def ensure_set(key: str) -> set[str]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "set", "value": set()}
        return redis_store[key]["value"]  # type: ignore[return-value]
    if entry["type"] != "set":
        raise HTTPException(status_code=400, detail="wrong type operation against non-set value")
    return entry["value"]  # type: ignore[return-value]


def get_hash_value(key: str) -> dict[str, str] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "hash":
        raise HTTPException(status_code=400, detail="wrong type operation against non-hash value")
    return entry["value"]  # type: ignore[return-value]


def ensure_hash(key: str) -> dict[str, str]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "hash", "value": {}}
        return redis_store[key]["value"]  # type: ignore[return-value]
    if entry["type"] != "hash":
        raise HTTPException(status_code=400, detail="wrong type operation against non-hash value")
    return entry["value"]  # type: ignore[return-value]


def get_hash_field_value(key: str, field: str) -> str | None:
    values = get_hash_value(key)
    if values is None:
        return None
    return values.get(field)


def get_zset_value(key: str) -> dict[str, float] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "zset":
        raise HTTPException(status_code=400, detail="wrong type operation against non-zset value")
    return entry["value"]  # type: ignore[return-value]


def ensure_zset(key: str) -> dict[str, float]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "zset", "value": {}}
        return redis_store[key]["value"]  # type: ignore[return-value]
    if entry["type"] != "zset":
        raise HTTPException(status_code=400, detail="wrong type operation against non-zset value")
    return entry["value"]  # type: ignore[return-value]


def parse_integer_value(value: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="value is not an integer") from exc


def normalize_list_index(length: int, index: int) -> int | None:
    normalized = index if index >= 0 else length + index
    if normalized < 0 or normalized >= length:
        return None
    return normalized


def compute_lrange_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    actual_start = start if start >= 0 else length + start
    actual_stop = stop if stop >= 0 else length + stop
    actual_start = max(actual_start, 0)
    actual_stop = min(actual_stop, length - 1)
    if length == 0 or actual_start >= length or actual_start > actual_stop:
        return 0, 0
    return actual_start, actual_stop + 1


def collect_sets(keys: list[str]) -> list[set[str]]:
    collected: list[set[str]] = []
    for key in keys:
        members = get_set_value(key)
        collected.append(set() if members is None else members)
    return collected


def sorted_zset_items(values: dict[str, float], reverse: bool = False) -> list[tuple[str, float]]:
    if reverse:
        return sorted(values.items(), key=lambda item: (-item[1], item[0]))
    return sorted(values.items(), key=lambda item: (item[1], item[0]))


def find_zset_rank(values: dict[str, float], member: str, reverse: bool = False) -> int | None:
    ordered = sorted_zset_items(values, reverse=reverse)
    for index, (current_member, _) in enumerate(ordered):
        if current_member == member:
            return index
    return None


def slice_zset_members(values: dict[str, float], start: int, stop: int, reverse: bool = False) -> list[str]:
    ordered = sorted_zset_items(values, reverse=reverse)
    slice_start, slice_end = compute_lrange_slice(len(ordered), start, stop)
    return [member for member, _ in ordered[slice_start:slice_end]]
