"""mini-redis 코어 진입점.

RESP 파싱이 끝난 명령 배열을 받아서,
알맞은 자료형 모듈로 위임하고 응답 딕셔너리를 반환합니다.
"""

from typing import Literal, TypedDict

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
from error_contract import ERR_EMPTY_COMMAND, err_unknown_command, err_wrong_number_of_arguments

ResponseType = Literal["simple_string", "bulk_string", "null", "integer", "error", "array"]


class RedisResponse(TypedDict):
    type: ResponseType
    value: str | int | None | list[str | None] | list[str]


string_store: dict[str, str] = {}
set_store: dict[str, set[str]] = {}
list_store: dict[str, list[str]] = {}
hash_store: dict[str, dict[str, str]] = {}


def _error(message: str) -> RedisResponse:
    return {"type": "error", "value": message}


def _wrong_arity(command_name: str) -> RedisResponse:
    return _error(err_wrong_number_of_arguments(command_name))


def _handle_common_key_commands(command_name: str, command: list[str]) -> RedisResponse | None:
    if command_name == "DEL":
        key = command[1]
        deleted = 1 if key in string_store or key in set_store or key in list_store or key in hash_store else 0
        string_store.pop(key, None)
        set_store.pop(key, None)
        list_store.pop(key, None)
        hash_store.pop(key, None)
        return {"type": "integer", "value": deleted}

    if command_name == "EXISTS":
        key = command[1]
        exists = key in string_store or key in set_store or key in list_store or key in hash_store
        return {"type": "integer", "value": 1 if exists else 0}

    if command_name == "TYPE":
        key = command[1]
        if key in string_store:
            return {"type": "bulk_string", "value": "string"}
        if key in set_store:
            return {"type": "bulk_string", "value": "set"}
        if key in list_store:
            return {"type": "bulk_string", "value": "list"}
        if key in hash_store:
            return {"type": "bulk_string", "value": "hash"}
        return {"type": "bulk_string", "value": "none"}

    return None


def execute(command: list[str]) -> RedisResponse:
    # 빈 명령은 유효한 Redis 명령이 아니므로 바로 에러를 반환합니다.
    if not command:
        return _error(ERR_EMPTY_COMMAND)

    command_name = command[0].upper()

    common_fixed_arity = {"DEL": 2, "EXISTS": 2, "TYPE": 2}
    if command_name in common_fixed_arity and len(command) != common_fixed_arity[command_name]:
        return _wrong_arity(command_name)

    # 자료형별 고정 길이 명령은 인자 개수가 정확히 맞아야 합니다.
    if command_name in STRING_FIXED_ARITY and len(command) != STRING_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)
    if command_name in SET_FIXED_ARITY and len(command) != SET_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)
    if command_name in LIST_FIXED_ARITY and len(command) != LIST_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)
    if command_name in HASH_FIXED_ARITY and len(command) != HASH_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)

    # MSET/MGET, SINTER/SUNION처럼 가변 길이 명령은 별도 규칙으로 검사합니다.
    if has_wrong_string_variable_arity(command_name, command):
        return _wrong_arity(command_name)
    if has_wrong_set_variable_arity(command_name, command):
        return _wrong_arity(command_name)

    common_result = _handle_common_key_commands(command_name, command)
    if common_result is not None:
        return common_result

    string_result = execute_string_command(command_name, command, string_store, set_store)
    if string_result is not None:
        return string_result

    set_result = execute_set_command(command_name, command, string_store, set_store)
    if set_result is not None:
        return set_result

    list_result = execute_list_command(command_name, command, string_store, set_store, list_store)
    if list_result is not None:
        return list_result

    hash_result = execute_hash_command(command_name, command, string_store, set_store, list_store, hash_store)
    if hash_result is not None:
        return hash_result

    return _error(err_unknown_command(command[0]))
