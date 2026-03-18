"""mini-redis core entrypoint."""

from __future__ import annotations

from typing import Literal, TypedDict

from command_router import dispatch_command, get_wrong_arity_command
from core_state import (
    expiry_store,
    hash_store,
    list_store,
    set_store,
    store_lock,
    string_store,
    zset_store,
)
from error_contract import ERR_EMPTY_COMMAND, err_unknown_command, err_wrong_number_of_arguments
from ttl_manager import ensure_background_cleanup_started, purge_expired_keys

ResponseType = Literal["simple_string", "bulk_string", "null", "integer", "error", "array"]


class RedisResponse(TypedDict):
    type: ResponseType
    value: str | int | float | None | list[str | None] | list[str]


def _error(message: str) -> RedisResponse:
    return {"type": "error", "value": message}


def execute(command: list[str]) -> RedisResponse:
    ensure_background_cleanup_started(lambda: store_lock)

    with store_lock:
        purge_expired_keys()

        if not command:
            return _error(ERR_EMPTY_COMMAND)

        command_name = command[0].upper()
        wrong_arity_command = get_wrong_arity_command(command_name, command)
        if wrong_arity_command is not None:
            return _error(err_wrong_number_of_arguments(wrong_arity_command))

        result = dispatch_command(command_name, command)
        if result is not None:
            return result

        return _error(err_unknown_command(command[0]))
