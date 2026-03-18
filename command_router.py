"""Command validation and dispatch pipeline for redis core."""

from __future__ import annotations

from core_commands.common_keys import execute_common_key_command
from core_commands.hashes import FIXED_ARITY as HASH_FIXED_ARITY, execute_hash_command
from core_commands.lists import FIXED_ARITY as LIST_FIXED_ARITY, execute_list_command
from core_commands.sets import (
    FIXED_ARITY as SET_FIXED_ARITY,
    execute_set_command,
    has_wrong_variable_arity as has_wrong_set_variable_arity,
)
from core_commands.strings import (
    FIXED_ARITY as STRING_FIXED_ARITY,
    execute_string_command,
    has_wrong_variable_arity as has_wrong_string_variable_arity,
)
from core_commands.zsets import FIXED_ARITY as ZSET_FIXED_ARITY, execute_zset_command
from core_state import hash_store, list_store, set_store, string_store, zset_store
from invalidation_manager import invalidate_key, invalidate_many
from season_manager import FIXED_ARITY as SEASON_FIXED_ARITY, execute_season_command
from ttl_manager import clear_ttl_on_write, handle_ttl_command
from version_manager import FIXED_ARITY as VERSION_FIXED_ARITY, execute_version_command

COMMON_FIXED_ARITY: dict[str, int] = {
    "DEL": 2,
    "EXISTS": 2,
    "TYPE": 2,
    "EXPIRE": 3,
    "TTL": 2,
    "PERSIST": 2,
}

STATELESS_COMMANDS = {"PING", "ECHO"}
KEYED_COMMANDS = (
    set(COMMON_FIXED_ARITY)
    | set(STRING_FIXED_ARITY)
    | set(SET_FIXED_ARITY)
    | set(LIST_FIXED_ARITY)
    | set(HASH_FIXED_ARITY)
    | set(ZSET_FIXED_ARITY)
    | {"MSET", "MGET", "SINTER", "SUNION"}
) - STATELESS_COMMANDS


def get_wrong_arity_command(command_name: str, command: list[str]) -> str | None:
    if command_name in {"SNAPSHOT", "DUMP"} and len(command) not in {1, 2}:
        return command_name
    if command_name in COMMON_FIXED_ARITY and len(command) != COMMON_FIXED_ARITY[command_name]:
        return command_name
    if command_name in SEASON_FIXED_ARITY and len(command) != SEASON_FIXED_ARITY[command_name]:
        return command_name
    if command_name in VERSION_FIXED_ARITY and len(command) != VERSION_FIXED_ARITY[command_name]:
        return command_name
    if command_name in STRING_FIXED_ARITY and len(command) != STRING_FIXED_ARITY[command_name]:
        return command_name
    if command_name in SET_FIXED_ARITY and len(command) != SET_FIXED_ARITY[command_name]:
        return command_name
    if command_name in LIST_FIXED_ARITY and len(command) != LIST_FIXED_ARITY[command_name]:
        return command_name
    if command_name in HASH_FIXED_ARITY and len(command) != HASH_FIXED_ARITY[command_name]:
        return command_name
    if command_name in ZSET_FIXED_ARITY and len(command) != ZSET_FIXED_ARITY[command_name]:
        return command_name
    if has_wrong_string_variable_arity(command_name, command):
        return command_name
    if has_wrong_set_variable_arity(command_name, command):
        return command_name
    return None


def dispatch_command(command_name: str, command: list[str]) -> dict | None:
    _invalidate_on_write(command_name, command)

    common_result = execute_common_key_command(command_name, command)
    if common_result is not None:
        return common_result

    ttl_result = handle_ttl_command(command_name, command)
    if ttl_result is not None:
        return ttl_result

    season_result = execute_season_command(command_name, command)
    if season_result is not None:
        return season_result

    version_result = execute_version_command(command_name, command)
    if version_result is not None:
        return version_result

    clear_ttl_on_write(command_name, command)

    string_result = execute_string_command(
        command_name,
        command,
        string_store,
        set_store,
        list_store,
        zset_store,
    )
    if string_result is not None:
        return string_result

    set_result = execute_set_command(
        command_name,
        command,
        string_store,
        set_store,
        list_store,
        zset_store,
    )
    if set_result is not None:
        return set_result

    list_result = execute_list_command(
        command_name,
        command,
        string_store,
        set_store,
        list_store,
        zset_store,
    )
    if list_result is not None:
        return list_result

    hash_result = execute_hash_command(
        command_name,
        command,
        string_store,
        set_store,
        list_store,
        zset_store,
        hash_store,
    )
    if hash_result is not None:
        return hash_result

    zset_result = execute_zset_command(
        command_name,
        command,
        string_store,
        set_store,
        list_store,
        zset_store,
    )
    if zset_result is not None:
        return zset_result

    return None


def _invalidate_on_write(command_name: str, command: list[str]) -> None:
    single_key_write_commands = {
        "DEL",
        "SET",
        "INCR",
        "DECR",
        "LPUSH",
        "RPUSH",
        "LPOP",
        "RPOP",
        "SADD",
        "SREM",
        "HSET",
        "HDEL",
        "HINCRBY",
        "ZADD",
        "ZINCRBY",
        "ZREM",
        "EXPIRE",
        "PERSIST",
    }

    if command_name in single_key_write_commands and len(command) > 1:
        invalidate_key(command[1])
        return

    if command_name == "MSET":
        invalidate_many(command[1::2])
