"""Snapshot save/restore helpers for redis core."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Literal

from core_state import snapshot_state, store_lock
from redis import restore_from_loader


def save_snapshot(path: str | Path) -> Path:
    snapshot_path = Path(path)
    with store_lock:
        payload = snapshot_state()
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return snapshot_path


def restore_snapshot(
    path: str | Path,
    delay_sec: float = 0.0,
    policy: Literal["replace", "merge"] = "replace",
) -> Path:
    snapshot_path = Path(path)
    if policy not in {"replace", "merge"}:
        raise ValueError("restore policy must be 'replace' or 'merge'")

    def _load_snapshot() -> dict:
        if delay_sec > 0:
            time.sleep(delay_sec)
        return json.loads(snapshot_path.read_text(encoding="utf-8"))

    restore_from_loader(_load_snapshot, restore_policy=policy)
    return snapshot_path
