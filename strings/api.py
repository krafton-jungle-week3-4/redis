from fastapi import APIRouter, Body, HTTPException

from common import expiry_store, get_string_value, increment_lock, parse_integer_value, set_string_entry

from .schemas import (
    DecrementResponse,
    GetResponse,
    IncrementResponse,
    MGetResponse,
    MSetRequest,
    MSetResponse,
    SetRequest,
    SetResponse,
)

router = APIRouter()


@router.post("/set", response_model=SetResponse)
def set_value(payload: SetRequest) -> SetResponse:
    set_string_entry(payload.key, payload.value)
    expiry_store.pop(payload.key, None)
    return SetResponse(result="OK", key=payload.key, value=payload.value)


@router.get("/get/{key}", response_model=GetResponse)
def get_value(key: str) -> GetResponse:
    return GetResponse(key=key, value=get_string_value(key))


@router.post("/increment/{key}", response_model=IncrementResponse)
def increment_value(key: str) -> IncrementResponse:
    with increment_lock:
        current_value = get_string_value(key)
        next_value = 1 if current_value is None else parse_integer_value(current_value) + 1
        set_string_entry(key, str(next_value))
        return IncrementResponse(result="OK", key=key, value=next_value)


@router.post("/decrement/{key}", response_model=DecrementResponse)
def decrement_value(key: str) -> DecrementResponse:
    with increment_lock:
        current_value = get_string_value(key)
        next_value = -1 if current_value is None else parse_integer_value(current_value) - 1
        set_string_entry(key, str(next_value))
        return DecrementResponse(result="OK", key=key, value=next_value)


@router.post("/mset", response_model=MSetResponse)
def mset_values(payload: MSetRequest) -> MSetResponse:
    for item in payload.items:
        if not item.key:
            raise HTTPException(status_code=400, detail="key must not be empty")
    for item in payload.items:
        set_string_entry(item.key, item.value)
        expiry_store.pop(item.key, None)
    return MSetResponse(result="OK", count=len(payload.items))


@router.post("/mget", response_model=MGetResponse)
def mget_values(keys: list[str] = Body(...)) -> MGetResponse:
    return MGetResponse(values=[get_string_value(key) for key in keys])
