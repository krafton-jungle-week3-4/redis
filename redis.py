"""mini-redis core entrypoint.

RESP parsing that already produced a token list calls this module's
`execute(command)` function. The function validates the command and
returns the agreed response dict.

Concurrency design:
- store-backed commands run under `store_lock`
- stateless commands like PING/ECHO bypass locking
"""

from __future__ import annotations

from typing import Literal, TypedDict

from command_router import KEYED_COMMANDS, STATELESS_COMMANDS, dispatch_command, get_wrong_arity_command
from core_state import hash_store, list_store, set_store, store_lock, string_store, zset_store
from error_contract import ERR_EMPTY_COMMAND, err_unknown_command, err_wrong_number_of_arguments
from ttl_manager import ensure_background_cleanup_started, purge_expired_keys

ResponseType = Literal["simple_string", "bulk_string", "null", "integer", "error", "array"]


class RedisResponse(TypedDict):
    type: ResponseType
    value: str | int | float | None | list[str | None] | list[str]


def _error(message: str) -> RedisResponse:
    return {"type": "error", "value": message}


def _execute_command(command: list[str], *, purge_expired: bool) -> RedisResponse:
    command_name = command[0].upper()
    if purge_expired:
        purge_expired_keys()

    result = dispatch_command(command_name, command)
    if result is not None:
        return result

    return _error(err_unknown_command(command[0]))


def execute(command: list[str]) -> RedisResponse:
    if not command:
        return _error(ERR_EMPTY_COMMAND)

    command_name = command[0].upper()
    wrong_arity_command = get_wrong_arity_command(command_name, command)
    if wrong_arity_command is not None:
        return _error(err_wrong_number_of_arguments(wrong_arity_command))

    if command_name in STATELESS_COMMANDS:
        return _execute_command(command, purge_expired=False)

    if command_name in KEYED_COMMANDS:
        ensure_background_cleanup_started(lambda: store_lock)
        with store_lock:
            return _execute_command(command, purge_expired=True)

    return _execute_command(command, purge_expired=False)
