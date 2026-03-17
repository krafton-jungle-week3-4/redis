from fastapi import APIRouter

from common import ensure_hash, get_hash_field_value, get_hash_value, increment_lock, parse_integer_value

from .schemas import HashDeleteResponse, HashExistsResponse, HashGetAllResponse, HashGetResponse, HashIncrementRequest, HashIncrementResponse, HashLenResponse, HashSetRequest, HashSetResponse

router = APIRouter()


@router.post("/hset/{key}", response_model=HashSetResponse)
def hset_value(key: str, payload: HashSetRequest) -> HashSetResponse:
    values = ensure_hash(key)
    added = 0 if payload.field in values else 1
    values[payload.field] = payload.value
    return HashSetResponse(key=key, field=payload.field, added=added)


@router.get("/hget/{key}/{field}", response_model=HashGetResponse)
def hget_value(key: str, field: str) -> HashGetResponse:
    return HashGetResponse(key=key, field=field, value=get_hash_field_value(key, field))


@router.delete("/hdel/{key}/{field}", response_model=HashDeleteResponse)
def hdel_value(key: str, field: str) -> HashDeleteResponse:
    values = get_hash_value(key)
    if values is None or field not in values:
        return HashDeleteResponse(key=key, field=field, removed=0)
    del values[field]
    return HashDeleteResponse(key=key, field=field, removed=1)


@router.get("/hgetall/{key}", response_model=HashGetAllResponse)
def hgetall_value(key: str) -> HashGetAllResponse:
    values = get_hash_value(key)
    return HashGetAllResponse(key=key, values={} if values is None else dict(values))


@router.get("/hexists/{key}/{field}", response_model=HashExistsResponse)
def hexists_value(key: str, field: str) -> HashExistsResponse:
    values = get_hash_value(key)
    exists = 1 if values is not None and field in values else 0
    return HashExistsResponse(key=key, field=field, exists=exists)


@router.post("/hincrby/{key}/{field}", response_model=HashIncrementResponse)
def hincrby_value(key: str, field: str, payload: HashIncrementRequest) -> HashIncrementResponse:
    with increment_lock:
        values = ensure_hash(key)
        current_value = values.get(field)
        next_value = payload.increment if current_value is None else parse_integer_value(current_value) + payload.increment
        values[field] = str(next_value)
        return HashIncrementResponse(key=key, field=field, value=next_value)


@router.get("/hlen/{key}", response_model=HashLenResponse)
def hlen_value(key: str) -> HashLenResponse:
    values = get_hash_value(key)
    return HashLenResponse(key=key, count=0 if values is None else len(values))
