"""Set command handlers for redis.py core."""

from typing import Any

from core.error_contract import ERR_WRONG_TYPE_SET
from managers.snapshot_manager import prepare_mutable_write


FIXED_ARITY: dict[str, int] = {
    "SADD": 3,
    "SREM": 3,
    "SISMEMBER": 3,
    "SMEMBERS": 2,
    "SCARD": 2,
}


def has_wrong_variable_arity(command_name: str, command: list[str]) -> bool:
    if command_name in {"SINTER", "SUNION"}:
        return len(command) < 2
    return False


def _is_wrong_type_key(
    key: str,
    string_store: dict[str, str],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
) -> bool:
    return key in string_store or key in list_store or key in zset_store


def execute_set_command(
    command_name: str,
    command: list[str],
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
    zset_store: dict[str, dict[str, float]],
) -> dict[str, Any] | None:
    if command_name == "SADD":
        key = command[1]
        member = command[2]
        if _is_wrong_type_key(key, string_store, list_store, zset_store):
            return {"type": "error", "value": ERR_WRONG_TYPE_SET}
        if key in set_store:
            prepare_mutable_write("set", key)
        members = set_store.setdefault(key, set())
        before = len(members)
        members.add(member)
        return {"type": "integer", "value": 1 if len(members) > before else 0}

    if command_name == "SREM":
        key = command[1]
        member = command[2]
        if _is_wrong_type_key(key, string_store, list_store, zset_store):
            return {"type": "error", "value": ERR_WRONG_TYPE_SET}
        members = set_store.get(key)
        if members is None or member not in members:
            return {"type": "integer", "value": 0}
        prepare_mutable_write("set", key)
        members = set_store.get(key)
        if members is None:
            return {"type": "integer", "value": 0}
        members.remove(member)
        if not members:
            set_store.pop(key, None)
        return {"type": "integer", "value": 1}

    if command_name == "SISMEMBER":
        key = command[1]
        member = command[2]
        if _is_wrong_type_key(key, string_store, list_store, zset_store):
            return {"type": "error", "value": ERR_WRONG_TYPE_SET}
        members = set_store.get(key)
        return {"type": "integer", "value": 1 if members is not None and member in members else 0}

    if command_name == "SMEMBERS":
        key = command[1]
        if _is_wrong_type_key(key, string_store, list_store, zset_store):
            return {"type": "error", "value": ERR_WRONG_TYPE_SET}
        members = set_store.get(key)
        if members is None:
            return {"type": "array", "value": []}
        return {"type": "array", "value": sorted(members)}

    if command_name == "SINTER":
        keys = command[1:]
        for key in keys:
            if _is_wrong_type_key(key, string_store, list_store, zset_store):
                return {"type": "error", "value": ERR_WRONG_TYPE_SET}
        sets = [set_store.get(key, set()) for key in keys]
        if not sets:
            return {"type": "array", "value": []}
        intersection = set(sets[0])
        for members in sets[1:]:
            intersection &= members
        return {"type": "array", "value": sorted(intersection)}

    if command_name == "SUNION":
        keys = command[1:]
        for key in keys:
            if _is_wrong_type_key(key, string_store, list_store, zset_store):
                return {"type": "error", "value": ERR_WRONG_TYPE_SET}
        union_set: set[str] = set()
        for key in keys:
            union_set |= set_store.get(key, set())
        return {"type": "array", "value": sorted(union_set)}

    if command_name == "SCARD":
        key = command[1]
        if _is_wrong_type_key(key, string_store, list_store, zset_store):
            return {"type": "error", "value": ERR_WRONG_TYPE_SET}
        members = set_store.get(key)
        return {"type": "integer", "value": 0 if members is None else len(members)}

    return None
