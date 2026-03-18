"""mini-redis core entrypoint.

RESP 파싱이 끝난 명령 배열을 받아서,
command_router로 위임하고 응답 딕셔너리를 반환합니다.

동시성 설계 포인트:
- 모든 쓰기 명령은 단일 writer thread가 queue에서 순차 처리합니다.
- 읽기 명령도 동일한 store_lock 아래에서 수행해, 쓰기 중간 상태를 보지 않게 합니다.
- 복구 중에는 request gate를 통해 새 요청 진입을 막고,
  복구 작업 자체도 writer queue에서 처리해 기존 쓰기와 순서를 맞춥니다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from queue import Queue
from threading import Event, RLock, Thread
from typing import Any, Literal, TypedDict

from command_router import dispatch_command, get_wrong_arity_command
from core_state import begin_loading, finish_loading, restore_state, store_lock, wait_until_ready
from error_contract import ERR_EMPTY_COMMAND, err_unknown_command, err_wrong_number_of_arguments
from snapshot_manager import begin_snapshot, finish_snapshot, write_snapshot_file
from ttl_manager import ensure_background_cleanup_started, purge_expired_keys

ResponseType = Literal["simple_string", "bulk_string", "null", "integer", "error", "array"]


class RedisResponse(TypedDict):
    type: ResponseType
    value: str | int | float | None | list[str | None] | list[str]


@dataclass(slots=True)
class WriteRequest:
    """writer queue에 넣을 작업 단위입니다."""

    command: list[str] | None = None
    snapshot: dict[str, Any] | None = None
    done: Event = field(default_factory=Event)
    result: RedisResponse | None = None


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
request_gate = RLock()


def _error(message: str) -> RedisResponse:
    return {"type": "error", "value": message}


def _execute_locked(command: list[str]) -> RedisResponse:
    purge_expired_keys()

    command_name = command[0].upper()
    result = dispatch_command(command_name, command)
    if result is not None:
        return result

    return _error(err_unknown_command(command[0]))


def _execute_restore_locked(snapshot: dict[str, Any]) -> RedisResponse:
    restore_state(snapshot)
    purge_expired_keys()
    return {"type": "simple_string", "value": "OK"}


def _writer_loop() -> None:
    while True:
        request = write_queue.get()
        try:
            with store_lock:
                if request.snapshot is not None:
                    request.result = _execute_restore_locked(request.snapshot)
                elif request.command is not None:
                    request.result = _execute_locked(request.command)
                else:
                    request.result = _error("ERR write request missing payload")
        finally:
            request.done.set()
            write_queue.task_done()


def _submit_write(command: list[str]) -> RedisResponse:
    request = WriteRequest(command=command)
    write_queue.put(request)
    request.done.wait()
    if request.result is None:
        return _error("ERR write request finished without result")
    return request.result


def _submit_restore(snapshot: dict[str, Any]) -> RedisResponse:
    request = WriteRequest(snapshot=snapshot)
    write_queue.put(request)
    request.done.wait()
    if request.result is None:
        return _error("ERR restore request finished without result")
    return request.result


writer_thread = Thread(target=_writer_loop, name="mini-redis-single-writer", daemon=True)
writer_thread.start()


def restore_from_loader(loader: Callable[[], dict[str, Any]]) -> RedisResponse:
    """복구 시작 시점부터 새 요청을 막고, loader 결과를 writer queue에서 복구합니다."""
    ensure_background_cleanup_started(lambda: store_lock)

    with request_gate:
        begin_loading()
        try:
            snapshot = loader()
            return _submit_restore(snapshot)
        finally:
            finish_loading()


def restore_from_snapshot_data(snapshot: dict[str, Any]) -> RedisResponse:
    return restore_from_loader(lambda: snapshot)


def execute(command: list[str]) -> RedisResponse:
    ensure_background_cleanup_started(lambda: store_lock)

    with request_gate:
        wait_until_ready()

        if not command:
            return _error(ERR_EMPTY_COMMAND)

        command_name = command[0].upper()
        wrong_arity_command = get_wrong_arity_command(command_name, command)
        if wrong_arity_command is not None:
            return _error(err_wrong_number_of_arguments(wrong_arity_command))

        # Snapshot/Dump는 파일 I/O가 있으므로 store 복사 구간만 lock으로 보호하고,
        # 실제 파일 쓰기는 lock 밖에서 처리합니다.
        if command_name in {"SNAPSHOT", "DUMP"}:
            with store_lock:
                purge_expired_keys()
                context = begin_snapshot(command[1] if len(command) == 2 else None)

            try:
                path = write_snapshot_file(context)
                return {"type": "bulk_string", "value": path}
            finally:
                with store_lock:
                    finish_snapshot()

        if command_name in WRITE_COMMANDS:
            return _submit_write(command)

        with store_lock:
            return _execute_locked(command)
