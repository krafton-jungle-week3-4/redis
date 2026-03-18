"""Minimal redis core entrypoint: execute(command: list[str]) -> dict."""

from typing import Literal, TypedDict

from core_commands.strings import FIXED_ARITY, execute_string_command, has_wrong_variable_arity
from error_contract import ERR_EMPTY_COMMAND, err_unknown_command, err_wrong_number_of_arguments

ResponseType = Literal["simple_string", "bulk_string", "null", "integer", "error", "array"]


class RedisResponse(TypedDict):
    type: ResponseType
    value: str | int | None | list[str | None]


store: dict[str, str] = {}


def _error(message: str) -> RedisResponse:
    return {"type": "error", "value": message}


def _wrong_arity(command_name: str) -> RedisResponse:
    return _error(err_wrong_number_of_arguments(command_name))


def execute(command: list[str]) -> RedisResponse:
    if not command:
        return _error(ERR_EMPTY_COMMAND)

    command_name = command[0].upper()

    if command_name in FIXED_ARITY and len(command) != FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)

    if has_wrong_variable_arity(command_name, command):
        return _wrong_arity(command_name)

    result = execute_string_command(command_name, command, store)
    if result is not None:
        return result

    return _error(err_unknown_command(command[0]))
