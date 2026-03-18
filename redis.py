"""mini-redis core entrypoint.

RESP 파싱이 끝난 명령 배열을 받아서,
command_router로 위임하고 응답 딕셔너리를 반환합니다.

동시성 설계 포인트:
- 모든 쓰기 명령은 단일 writer thread가 queue에서 순차 처리합니다.
- 읽기 명령도 동일한 store_lock 아래에서 수행해, 쓰기 중간 상태를 보지 않게 합니다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from queue import Queue
from threading import Event, Thread
from typing import Literal, TypedDict

from command_router import dispatch_command, get_wrong_arity_command
from core_state import store_lock
from error_contract import ERR_EMPTY_COMMAND, err_unknown_command, err_wrong_number_of_arguments
from ttl_manager import ensure_background_cleanup_started, purge_expired_keys

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
    "EXPIRE",
    "PERSIST",
}

write_queue: Queue[WriteRequest] = Queue()


def _error(message: str) -> RedisResponse:
    return {"type": "error", "value": message}


def _execute_locked(command: list[str]) -> RedisResponse:
    """store_lock을 잡은 상태에서 실제 명령을 실행합니다."""
    purge_expired_keys()

    command_name = command[0].upper()
    result = dispatch_command(command_name, command)
    if result is not None:
        return result

    return _error(err_unknown_command(command[0]))


def _writer_loop() -> None:
    """queue에 들어온 쓰기 요청을 하나씩 순차 처리합니다."""
    while True:
        request = write_queue.get()
        try:
            with store_lock:
                request.result = _execute_locked(request.command)
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
    ensure_background_cleanup_started(lambda: store_lock)

    if not command:
        return _error(ERR_EMPTY_COMMAND)

    command_name = command[0].upper()
    wrong_arity_command = get_wrong_arity_command(command_name, command)
    if wrong_arity_command is not None:
        return _error(err_wrong_number_of_arguments(wrong_arity_command))

    if command_name in WRITE_COMMANDS:
        return _submit_write(command)

    with store_lock:
        return _execute_locked(command)
