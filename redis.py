"""mini-redis 코어 진입점.

RESP 파싱이 끝난 명령 배열을 받아서,
알맞은 자료형 모듈로 위임하고 응답 딕셔너리를 반환합니다.

동시성 설계 포인트:
- 모든 쓰기 명령은 단일 writer thread가 queue에서 순차 처리합니다.
- 읽기 명령도 동일한 store_lock 아래에서 수행해, 쓰기 중간 상태를 보지 않게 합니다.
"""

from dataclasses import dataclass, field
from queue import Queue
from threading import Event, RLock, Thread
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
from core_commands.zsets import FIXED_ARITY as ZSET_FIXED_ARITY, execute_zset_command
from error_contract import ERR_EMPTY_COMMAND, err_unknown_command, err_wrong_number_of_arguments

ResponseType = Literal["simple_string", "bulk_string", "null", "integer", "error", "array"]


class RedisResponse(TypedDict):
    type: ResponseType
    value: str | int | float | None | list[str | None] | list[str]


@dataclass(slots=True)
class WriteRequest:
    """writer queue에 넣을 쓰기 요청 단위입니다."""

    command: list[str]
    done: Event = field(default_factory=Event)
    result: RedisResponse | None = None


# 자료형별 저장소는 유지하되, 쓰기는 반드시 single writer만 수행합니다.
string_store: dict[str, str] = {}
set_store: dict[str, set[str]] = {}
list_store: dict[str, list[str]] = {}
hash_store: dict[str, dict[str, str]] = {}
zset_store: dict[str, dict[str, float]] = {}

# 읽기/쓰기 모두 같은 락을 사용해서 중간 상태를 보지 않도록 합니다.
store_lock = RLock()
write_queue: Queue[WriteRequest] = Queue()

# 순차 처리가 필요한 쓰기 명령 목록입니다.
WRITE_COMMANDS = {
    "DEL",
    "SET",
    "INCR",
    "DECR",
    "MSET",
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
}

COMMON_FIXED_ARITY = {"DEL": 2, "EXISTS": 2, "TYPE": 2}


def _error(message: str) -> RedisResponse:
    return {"type": "error", "value": message}


def _wrong_arity(command_name: str) -> RedisResponse:
    return _error(err_wrong_number_of_arguments(command_name))


def _validate_arity(command_name: str, command: list[str]) -> RedisResponse | None:
    """명령별 인자 개수를 먼저 검사합니다."""
    if command_name in COMMON_FIXED_ARITY and len(command) != COMMON_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)

    if command_name in STRING_FIXED_ARITY and len(command) != STRING_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)
    if command_name in SET_FIXED_ARITY and len(command) != SET_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)
    if command_name in LIST_FIXED_ARITY and len(command) != LIST_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)
    if command_name in HASH_FIXED_ARITY and len(command) != HASH_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)
    if command_name in ZSET_FIXED_ARITY and len(command) != ZSET_FIXED_ARITY[command_name]:
        return _wrong_arity(command_name)

    if has_wrong_string_variable_arity(command_name, command):
        return _wrong_arity(command_name)
    if has_wrong_set_variable_arity(command_name, command):
        return _wrong_arity(command_name)

    return None


def _handle_common_write_command(command_name: str, command: list[str]) -> RedisResponse | None:
    """공통 key 명령 중 store를 변경하는 명령만 처리합니다."""
    if command_name == "DEL":
        key = command[1]
        deleted = 1 if key in string_store or key in set_store or key in list_store or key in hash_store or key in zset_store else 0
        string_store.pop(key, None)
        set_store.pop(key, None)
        list_store.pop(key, None)
        hash_store.pop(key, None)
        zset_store.pop(key, None)
        return {"type": "integer", "value": deleted}

    return None


def _handle_common_read_command(command_name: str, command: list[str]) -> RedisResponse | None:
    """공통 key 명령 중 조회만 하는 명령을 처리합니다."""
    if command_name == "EXISTS":
        key = command[1]
        exists = key in string_store or key in set_store or key in list_store or key in hash_store or key in zset_store
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
        if key in zset_store:
            return {"type": "bulk_string", "value": "zset"}
        return {"type": "bulk_string", "value": "none"}

    return None


def _execute_write(command: list[str]) -> RedisResponse:
    """실제 쓰기 로직입니다. 반드시 writer thread 안에서만 호출합니다."""
    command_name = command[0].upper()

    common_result = _handle_common_write_command(command_name, command)
    if common_result is not None:
        return common_result

    string_result = execute_string_command(command_name, command, string_store, set_store, list_store, zset_store)
    if string_result is not None:
        return string_result

    set_result = execute_set_command(command_name, command, string_store, set_store, list_store, zset_store)
    if set_result is not None:
        return set_result

    list_result = execute_list_command(command_name, command, string_store, set_store, list_store, zset_store)
    if list_result is not None:
        return list_result

    hash_result = execute_hash_command(command_name, command, string_store, set_store, list_store, zset_store, hash_store)
    if hash_result is not None:
        return hash_result

    zset_result = execute_zset_command(command_name, command, string_store, set_store, list_store, zset_store)
    if zset_result is not None:
        return zset_result

    return _error(err_unknown_command(command[0]))


def _execute_read(command: list[str]) -> RedisResponse:
    """읽기 전용 로직입니다. store_lock 아래에서만 호출합니다."""
    command_name = command[0].upper()

    common_result = _handle_common_read_command(command_name, command)
    if common_result is not None:
        return common_result

    string_result = execute_string_command(command_name, command, string_store, set_store, list_store, zset_store)
    if string_result is not None:
        return string_result

    set_result = execute_set_command(command_name, command, string_store, set_store, list_store, zset_store)
    if set_result is not None:
        return set_result

    list_result = execute_list_command(command_name, command, string_store, set_store, list_store, zset_store)
    if list_result is not None:
        return list_result

    hash_result = execute_hash_command(command_name, command, string_store, set_store, list_store, zset_store, hash_store)
    if hash_result is not None:
        return hash_result

    zset_result = execute_zset_command(command_name, command, string_store, set_store, list_store, zset_store)
    if zset_result is not None:
        return zset_result

    return _error(err_unknown_command(command[0]))


def _writer_loop() -> None:
    """queue에 들어온 쓰기 요청을 하나씩 순차 처리합니다."""
    while True:
        request = write_queue.get()
        try:
            with store_lock:
                request.result = _execute_write(request.command)
        finally:
            request.done.set()
            write_queue.task_done()


def _submit_write(command: list[str]) -> RedisResponse:
    """호출 스레드는 요청을 queue에 넣고, writer 처리 완료를 기다립니다."""
    request = WriteRequest(command=command)
    write_queue.put(request)
    request.done.wait()
    if request.result is None:
        return _error("ERR write request finished without result")
    return request.result


# 서버 전체에서 writer는 하나만 실행되도록 모듈 import 시 한 번 시작합니다.
writer_thread = Thread(target=_writer_loop, name="mini-redis-single-writer", daemon=True)
writer_thread.start()


def execute(command: list[str]) -> RedisResponse:
    if not command:
        return _error(ERR_EMPTY_COMMAND)

    command_name = command[0].upper()

    arity_error = _validate_arity(command_name, command)
    if arity_error is not None:
        return arity_error

    if command_name in WRITE_COMMANDS:
        return _submit_write(command)

    with store_lock:
        return _execute_read(command)
