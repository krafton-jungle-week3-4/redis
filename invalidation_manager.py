"""Key-read cache with explicit invalidation hooks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(slots=True)
class KeyReadCache:
    exists: bool | None = None
    key_type: str | None = None
    string_loaded: bool = False
    string_value: str | None = None


_key_read_cache: dict[str, KeyReadCache] = {}


def invalidate_key(key: str) -> None:
    _key_read_cache.pop(key, None)


def invalidate_many(keys: list[str]) -> None:
    for key in keys:
        invalidate_key(key)


def invalidate_all() -> None:
    _key_read_cache.clear()


def read_exists(key: str, resolver: Callable[[str], bool]) -> bool:
    cached = _key_read_cache.get(key)
    if cached is not None and cached.exists is not None:
        return cached.exists

    exists = resolver(key)
    entry = cached or KeyReadCache()
    entry.exists = exists
    _key_read_cache[key] = entry
    return exists


def read_type(key: str, resolver: Callable[[str], str]) -> str:
    cached = _key_read_cache.get(key)
    if cached is not None and cached.key_type is not None:
        return cached.key_type

    key_type = resolver(key)
    entry = cached or KeyReadCache()
    entry.key_type = key_type
    entry.exists = key_type != "none"
    _key_read_cache[key] = entry
    return key_type


def read_string_value(key: str, resolver: Callable[[str], str | None]) -> str | None:
    cached = _key_read_cache.get(key)
    if cached is not None and cached.string_loaded:
        return cached.string_value

    value = resolver(key)
    entry = cached or KeyReadCache()
    entry.string_loaded = True
    entry.string_value = value
    if entry.key_type is None:
        entry.key_type = "none" if value is None else "string"
    entry.exists = value is not None
    _key_read_cache[key] = entry
    return value


def debug_cache_snapshot() -> dict[str, dict[str, str | bool | None]]:
    """테스트 검증용으로 현재 캐시 상태를 조회합니다."""

    snapshot: dict[str, dict[str, str | bool | None]] = {}
    for key, entry in _key_read_cache.items():
        snapshot[key] = {
            "exists": entry.exists,
            "key_type": entry.key_type,
            "string_loaded": entry.string_loaded,
            "string_value": entry.string_value,
        }
    return snapshot
