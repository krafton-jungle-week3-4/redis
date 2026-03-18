"""Snapshot manager with copy-on-write semantics."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from core_state import (
    archived_zset_store,
    closed_zset_keys,
    expiry_store,
    hash_store,
    list_store,
    set_store,
    string_store,
    zset_store,
)


@dataclass
class SnapshotContext:
    path: Path
    strings: dict[str, str]
    sets: dict[str, set[str]]
    lists: dict[str, list[str]]
    hashes: dict[str, dict[str, str]]
    zsets: dict[str, dict[str, float]]
    archived_zsets: dict[str, dict[str, float]]
    closed_zsets: list[str]
    expiry: dict[str, float]
    detached_set_keys: set[str] = field(default_factory=set)
    detached_list_keys: set[str] = field(default_factory=set)
    detached_hash_keys: set[str] = field(default_factory=set)
    detached_zset_keys: set[str] = field(default_factory=set)


_active_snapshot: SnapshotContext | None = None


def _default_snapshot_path() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path("snapshots") / f"snapshot-{timestamp}.json"


def begin_snapshot(path_arg: str | None = None) -> SnapshotContext:
    global _active_snapshot

    path = Path(path_arg) if path_arg else _default_snapshot_path()
    context = SnapshotContext(
        path=path,
        strings=dict(string_store),
        sets=dict(set_store),
        lists=dict(list_store),
        hashes=dict(hash_store),
        zsets=dict(zset_store),
        archived_zsets={key: dict(value) for key, value in archived_zset_store.items()},
        closed_zsets=sorted(closed_zset_keys),
        expiry=dict(expiry_store),
    )
    _active_snapshot = context
    return context


def finish_snapshot() -> None:
    global _active_snapshot
    _active_snapshot = None


def prepare_mutable_write(store_name: str, key: str) -> None:
    """Detach mutable value for live store on first write during active snapshot."""

    if _active_snapshot is None:
        return

    if store_name == "set":
        if key in _active_snapshot.sets and key not in _active_snapshot.detached_set_keys and key in set_store:
            set_store[key] = copy.deepcopy(set_store[key])
            _active_snapshot.detached_set_keys.add(key)
        return

    if store_name == "list":
        if key in _active_snapshot.lists and key not in _active_snapshot.detached_list_keys and key in list_store:
            list_store[key] = copy.deepcopy(list_store[key])
            _active_snapshot.detached_list_keys.add(key)
        return

    if store_name == "hash":
        if key in _active_snapshot.hashes and key not in _active_snapshot.detached_hash_keys and key in hash_store:
            hash_store[key] = copy.deepcopy(hash_store[key])
            _active_snapshot.detached_hash_keys.add(key)
        return

    if store_name == "zset":
        if key in _active_snapshot.zsets and key not in _active_snapshot.detached_zset_keys and key in zset_store:
            zset_store[key] = copy.deepcopy(zset_store[key])
            _active_snapshot.detached_zset_keys.add(key)
        return


def _normalize_for_json(context: SnapshotContext) -> dict[str, Any]:
    return {
        "strings": context.strings,
        "sets": {key: sorted(list(values)) for key, values in context.sets.items()},
        "lists": context.lists,
        "hashes": context.hashes,
        "zsets": context.zsets,
        "archived_zsets": context.archived_zsets,
        "closed_zsets": context.closed_zsets,
        "expiry": context.expiry,
    }


def write_snapshot_file(context: SnapshotContext) -> str:
    context.path.parent.mkdir(parents=True, exist_ok=True)
    payload = _normalize_for_json(context)
    context.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(context.path)
