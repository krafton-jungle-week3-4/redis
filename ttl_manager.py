"""TTL lifecycle management (lazy expiration + background cleanup)."""

from __future__ import annotations

import threading
import time
from typing import Callable

from core_state import delete_key_everywhere, expiry_store, key_exists
from error_contract import ERR_VALUE_NOT_INTEGER

_cleanup_started = False
_cleanup_interval_sec = 1.0


def purge_expired_keys(now: float | None = None) -> None:
    if now is None:
        now = time.time()
    expired_keys = [key for key, expires_at in expiry_store.items() if expires_at <= now]
    for key in expired_keys:
        delete_key_everywhere(key)


def _background_cleanup_loop(with_lock: Callable[[], threading.RLock]) -> None:
    while True:
        time.sleep(_cleanup_interval_sec)
        with with_lock():
            purge_expired_keys()


def ensure_background_cleanup_started(with_lock: Callable[[], threading.RLock]) -> None:
    global _cleanup_started
    if _cleanup_started:
        return
    _cleanup_started = True
    thread = threading.Thread(target=_background_cleanup_loop, args=(with_lock,), daemon=True)
    thread.start()


def handle_ttl_command(command_name: str, command: list[str]) -> dict | None:
    if command_name == "EXPIRE":
        key = command[1]
        try:
            seconds = int(command[2])
        except ValueError:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}
        if seconds < 0:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}
        if not key_exists(key):
            return {"type": "integer", "value": 0}
        expiry_store[key] = time.time() + seconds
        purge_expired_keys()
        return {"type": "integer", "value": 1}

    if command_name == "TTL":
        key = command[1]
        if not key_exists(key):
            return {"type": "integer", "value": -2}
        expires_at = expiry_store.get(key)
        if expires_at is None:
            return {"type": "integer", "value": -1}
        remaining = int(expires_at - time.time())
        if remaining < 0:
            delete_key_everywhere(key)
            return {"type": "integer", "value": -2}
        return {"type": "integer", "value": remaining}

    if command_name == "PERSIST":
        key = command[1]
        if not key_exists(key):
            return {"type": "integer", "value": 0}
        removed = 1 if key in expiry_store else 0
        expiry_store.pop(key, None)
        return {"type": "integer", "value": removed}

    return None


def clear_ttl_on_write(command_name: str, command: list[str]) -> None:
    # Redis-like behavior: SET/MSET overwrite removes previous TTL.
    if command_name == "SET":
        expiry_store.pop(command[1], None)
    if command_name == "MSET":
        for index in range(1, len(command), 2):
            expiry_store.pop(command[index], None)
