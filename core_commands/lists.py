"""redis.py 코어에서 사용하는 리스트 명령 처리 모듈."""

from typing import Any

from error_contract import ERR_VALUE_NOT_INTEGER, ERR_WRONG_TYPE_LIST


FIXED_ARITY: dict[str, int] = {
    "LPUSH": 3,
    "RPUSH": 3,
    "LPOP": 2,
    "RPOP": 2,
    "LRANGE": 4,
}


def _resolve_stores(
    store: dict[str, Any],
    set_store: dict[str, set[str]] | None,
    list_store: dict[str, list[str]] | None,
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, list[str]]]:
    """호출 형태에 따라 저장소 인자를 정리합니다.

    - 테스트에서는 예전처럼 단일 store 하나만 넘길 수 있습니다.
    - redis.py에서는 string/set/list 저장소를 나눠서 넘깁니다.
    """
    if set_store is None and list_store is None:
        return {}, {}, store  # type: ignore[return-value]
    return store, set_store or {}, list_store or {}


def _get_list_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
) -> list[str] | None | str:
    """저장소에서 리스트 값을 읽어옵니다."""
    if key in string_store or key in set_store:
        return ERR_WRONG_TYPE_LIST
    return list_store.get(key)


def _ensure_list_entry(
    key: str,
    string_store: dict[str, str],
    set_store: dict[str, set[str]],
    list_store: dict[str, list[str]],
) -> list[str] | str:
    """리스트를 쓰기 전에 key가 list인지 보장합니다."""
    if key in string_store or key in set_store:
        return ERR_WRONG_TYPE_LIST

    items = list_store.get(key)
    if items is None:
        list_store[key] = []
        return list_store[key]
    return items


def _compute_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    """LRANGE 인자를 Python 슬라이스 범위로 변환합니다."""
    actual_start = start if start >= 0 else length + start
    actual_stop = stop if stop >= 0 else length + stop

    actual_start = max(actual_start, 0)
    actual_stop = min(actual_stop, length - 1)

    if length == 0 or actual_start >= length or actual_start > actual_stop:
        return 0, 0

    return actual_start, actual_stop + 1


def execute_list_command(
    command_name: str,
    command: list[str],
    store: dict[str, Any],
    set_store: dict[str, set[str]] | None = None,
    list_store: dict[str, list[str]] | None = None,
) -> dict[str, Any] | None:
    """리스트 명령을 실행합니다.

    이 함수가 담당하지 않는 명령이면 None을 반환해서
    상위 dispatcher(redis.py)가 다른 자료형 모듈로 넘길 수 있게 합니다.
    """
    string_store, resolved_set_store, resolved_list_store = _resolve_stores(store, set_store, list_store)

    if command_name == "LPUSH":
        key = command[1]
        value = command[2]
        items = _ensure_list_entry(key, string_store, resolved_set_store, resolved_list_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        # LPUSH는 리스트의 왼쪽(head)에 값을 넣습니다.
        items.insert(0, value)
        return {"type": "integer", "value": len(items)}

    if command_name == "RPUSH":
        key = command[1]
        value = command[2]
        items = _ensure_list_entry(key, string_store, resolved_set_store, resolved_list_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        # RPUSH는 리스트의 오른쪽(tail)에 값을 넣습니다.
        items.append(value)
        return {"type": "integer", "value": len(items)}

    if command_name == "LPOP":
        key = command[1]
        items = _get_list_entry(key, string_store, resolved_set_store, resolved_list_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        if items is None or not items:
            return {"type": "null", "value": None}
        value = items.pop(0)
        if not items:
            resolved_list_store.pop(key, None)
        return {"type": "bulk_string", "value": value}

    if command_name == "RPOP":
        key = command[1]
        items = _get_list_entry(key, string_store, resolved_set_store, resolved_list_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        if items is None or not items:
            return {"type": "null", "value": None}
        value = items.pop()
        if not items:
            resolved_list_store.pop(key, None)
        return {"type": "bulk_string", "value": value}

    if command_name == "LRANGE":
        key = command[1]
        items = _get_list_entry(key, string_store, resolved_set_store, resolved_list_store)
        if isinstance(items, str):
            return {"type": "error", "value": items}
        if items is None:
            return {"type": "array", "value": []}

        try:
            start = int(command[2])
            stop = int(command[3])
        except ValueError:
            return {"type": "error", "value": ERR_VALUE_NOT_INTEGER}

        slice_start, slice_end = _compute_slice(len(items), start, stop)
        return {"type": "array", "value": items[slice_start:slice_end]}

    return None
