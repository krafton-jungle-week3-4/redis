"""Season-close helpers for leaderboard-style ZSETs."""

from __future__ import annotations

from core_state import archived_zset_store, closed_zset_keys, expiry_store, zset_store
from error_contract import ERR_LEADERBOARD_CLOSED


FIXED_ARITY: dict[str, int] = {
    "CLOSESEASON": 2,
}


def execute_season_command(command_name: str, command: list[str]) -> dict | None:
    if command_name != "CLOSESEASON":
        return None

    key = command[1]
    archived_zset_store[key] = dict(zset_store.get(key, {}))
    closed_zset_keys.add(key)
    zset_store.pop(key, None)
    expiry_store.pop(key, None)
    return {"type": "simple_string", "value": "OK"}


def reject_if_closed_leaderboard(key: str) -> dict | None:
    if key in closed_zset_keys:
        return {"type": "error", "value": ERR_LEADERBOARD_CLOSED}
    return None
