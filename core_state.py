"""Shared in-memory stores and key-level helpers for redis core."""

from __future__ import annotations

import threading
from typing import Any

from invalidation_manager import invalidate_all, invalidate_key, read_exists, read_string_value, read_type


class InvalidationAwareDict(dict):
    """테스트에서 clear()로 스토어 초기화될 때 캐시도 함께 비웁니다."""

    def clear(self) -> None:  # type: ignore[override]
        super().clear()
        invalidate_all()


string_store: InvalidationAwareDict[str, str] = InvalidationAwareDict()
set_store: InvalidationAwareDict[str, set[str]] = InvalidationAwareDict()
list_store: InvalidationAwareDict[str, list[str]] = InvalidationAwareDict()
hash_store: InvalidationAwareDict[str, dict[str, str]] = InvalidationAwareDict()
zset_store: InvalidationAwareDict[str, dict[str, float]] = InvalidationAwareDict()
expiry_store: InvalidationAwareDict[str, float] = InvalidationAwareDict()

# 시즌 종료된 leaderboard는 별도 보관소에 남기고, 이후 쓰기는 차단합니다.
archived_zset_store: dict[str, dict[str, float]] = {}
closed_zset_keys: set[str] = set()

store_lock = threading.RLock()
loading_complete = threading.Event()
loading_complete.set()


def key_exists(key: str) -> bool:
    return read_exists(
        key,
        lambda target: (
            target in string_store
            or target in set_store
            or target in list_store
            or target in hash_store
            or target in zset_store
            or target in archived_zset_store
        ),
    )


def key_type(key: str) -> str:
    def _resolve_type(target: str) -> str:
        if target in string_store:
            return "string"
        if target in set_store:
            return "set"
        if target in list_store:
            return "list"
        if target in hash_store:
            return "hash"
        if target in zset_store or target in archived_zset_store:
            return "zset"
        return "none"

    return read_type(key, _resolve_type)


def string_value(key: str) -> str | None:
    return read_string_value(key, lambda target: string_store.get(target))


def delete_key_everywhere(key: str) -> None:
    string_store.pop(key, None)
    set_store.pop(key, None)
    list_store.pop(key, None)
    hash_store.pop(key, None)
    zset_store.pop(key, None)
    archived_zset_store.pop(key, None)
    closed_zset_keys.discard(key)
    expiry_store.pop(key, None)
    invalidate_key(key)


def clear_all_stores() -> None:
    string_store.clear()
    set_store.clear()
    list_store.clear()
    hash_store.clear()
    zset_store.clear()
    archived_zset_store.clear()
    closed_zset_keys.clear()
    expiry_store.clear()
    invalidate_all()


def snapshot_state() -> dict[str, Any]:
    """현재 메모리 상태를 직렬화 가능한 형태로 복사합니다."""
    return {
        "strings": dict(string_store),
        "sets": {key: sorted(value) for key, value in set_store.items()},
        "lists": {key: list(value) for key, value in list_store.items()},
        "hashes": {key: dict(value) for key, value in hash_store.items()},
        "zsets": {key: dict(value) for key, value in zset_store.items()},
        "archived_zsets": {key: dict(value) for key, value in archived_zset_store.items()},
        "closed_zsets": sorted(closed_zset_keys),
        "expiry": dict(expiry_store),
    }


def restore_state(snapshot: dict[str, Any]) -> None:
    """스냅샷 내용을 현재 메모리 상태로 완전히 교체합니다."""
    clear_all_stores()

    string_store.update(snapshot.get("strings", {}))
    set_store.update({key: set(value) for key, value in snapshot.get("sets", {}).items()})
    list_store.update({key: list(value) for key, value in snapshot.get("lists", {}).items()})
    hash_store.update({key: dict(value) for key, value in snapshot.get("hashes", {}).items()})
    zset_store.update(
        {key: {member: float(score) for member, score in value.items()} for key, value in snapshot.get("zsets", {}).items()}
    )
    archived_zset_store.update(
        {
            key: {member: float(score) for member, score in value.items()}
            for key, value in snapshot.get("archived_zsets", {}).items()
        }
    )
    closed_zset_keys.update(snapshot.get("closed_zsets", []))
    expiry_store.update({key: float(value) for key, value in snapshot.get("expiry", {}).items()})
    invalidate_all()


def begin_loading() -> None:
    loading_complete.clear()


def finish_loading() -> None:
    loading_complete.set()


def wait_until_ready() -> None:
    loading_complete.wait()


def is_loading() -> bool:
    return not loading_complete.is_set()
