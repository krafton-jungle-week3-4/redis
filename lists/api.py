from fastapi import APIRouter, HTTPException

from common import compute_lrange_slice, ensure_list, get_list_value, normalize_list_index

from .schemas import LIndexResponse, LLenResponse, LRangeResponse, LSetRequest, LSetResponse, ListPopResponse, ListPushRequest, ListPushResponse

router = APIRouter()


@router.post("/lpush/{key}", response_model=ListPushResponse)
def lpush_value(key: str, payload: ListPushRequest) -> ListPushResponse:
    items = ensure_list(key)
    items.insert(0, payload.value)
    return ListPushResponse(result="OK", key=key, length=len(items))


@router.post("/rpush/{key}", response_model=ListPushResponse)
def rpush_value(key: str, payload: ListPushRequest) -> ListPushResponse:
    items = ensure_list(key)
    items.append(payload.value)
    return ListPushResponse(result="OK", key=key, length=len(items))


@router.post("/lpop/{key}", response_model=ListPopResponse)
def lpop_value(key: str) -> ListPopResponse:
    items = get_list_value(key)
    if items is None or not items:
        return ListPopResponse(key=key, value=None)
    return ListPopResponse(key=key, value=items.pop(0))


@router.post("/rpop/{key}", response_model=ListPopResponse)
def rpop_value(key: str) -> ListPopResponse:
    items = get_list_value(key)
    if items is None or not items:
        return ListPopResponse(key=key, value=None)
    return ListPopResponse(key=key, value=items.pop())


@router.get("/lrange/{key}/{start}/{stop}", response_model=LRangeResponse)
def lrange_value(key: str, start: int, stop: int) -> LRangeResponse:
    items = get_list_value(key)
    if items is None:
        return LRangeResponse(key=key, values=[])
    slice_start, slice_end = compute_lrange_slice(len(items), start, stop)
    return LRangeResponse(key=key, values=items[slice_start:slice_end])


@router.get("/llen/{key}", response_model=LLenResponse)
def llen_value(key: str) -> LLenResponse:
    items = get_list_value(key)
    return LLenResponse(key=key, length=0 if items is None else len(items))


@router.get("/lindex/{key}/{index}", response_model=LIndexResponse)
def lindex_value(key: str, index: int) -> LIndexResponse:
    items = get_list_value(key)
    if items is None:
        return LIndexResponse(key=key, index=index, value=None)
    normalized = normalize_list_index(len(items), index)
    if normalized is None:
        return LIndexResponse(key=key, index=index, value=None)
    return LIndexResponse(key=key, index=index, value=items[normalized])


@router.post("/lset/{key}/{index}", response_model=LSetResponse)
def lset_value(key: str, index: int, payload: LSetRequest) -> LSetResponse:
    items = get_list_value(key)
    if items is None:
        raise HTTPException(status_code=404, detail="no such key")
    normalized = normalize_list_index(len(items), index)
    if normalized is None:
        raise HTTPException(status_code=400, detail="index out of range")
    items[normalized] = payload.value
    return LSetResponse(result="OK", key=key, index=index, value=payload.value)
