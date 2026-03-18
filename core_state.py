"""Shared in-memory stores and key-level helpers for redis core."""

from __future__ import annotations

import threading


string_store: dict[str, str] = {}
set_store: dict[str, set[str]] = {}
list_store: dict[str, list[str]] = {}
hash_store: dict[str, dict[str, str]] = {}
zset_store: dict[str, dict[str, float]] = {}
expiry_store: dict[str, float] = {}

store_lock = threading.RLock()


def key_exists(key: str) -> bool:
    return (
        key in string_store
        or key in set_store
        or key in list_store
        or key in hash_store
        or key in zset_store
    )


def key_type(key: str) -> str:
    if key in string_store:
        return "string"
    if key in set_store:
        return "set"
    if key in list_store:
        return "list"
    if key in hash_store:
        return "hash"
    if key in zset_store:
        return "zset"
    return "none"


def delete_key_everywhere(key: str) -> None:
    string_store.pop(key, None)
    set_store.pop(key, None)
    list_store.pop(key, None)
    hash_store.pop(key, None)
    zset_store.pop(key, None)
    expiry_store.pop(key, None)
