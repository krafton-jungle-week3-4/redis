import math
import os
import time
from threading import Lock
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

app = FastAPI(title="mini-redis")

redis_store: dict[str, dict[str, Any]] = {}
expiry_store: dict[str, float] = {}
increment_lock = Lock()


class SetRequest(BaseModel):
    value: str


class SetResponse(BaseModel):
    result: str
    key: str
    value: str


class GetResponse(BaseModel):
    key: str
    value: str | None


class IncrementResponse(BaseModel):
    result: str
    key: str
    value: int


class DeleteResponse(BaseModel):
    deleted: int


class DecrementResponse(BaseModel):
    result: str
    key: str
    value: int


class MSetItem(BaseModel):
    key: str
    value: str


class MSetRequest(BaseModel):
    items: list[MSetItem]


class MSetResponse(BaseModel):
    result: str
    count: int


class MGetRequest(BaseModel):
    keys: list[str]


class MGetResponse(BaseModel):
    values: list[str | None]


class ExistsResponse(BaseModel):
    exists: int


class TypeResponse(BaseModel):
    key: str
    type: str


class ExpireRequest(BaseModel):
    ttl: int


class ExpireResponse(BaseModel):
    updated: int


class TtlResponse(BaseModel):
    ttl: int


class PersistResponse(BaseModel):
    removed: int


class ListPushRequest(BaseModel):
    value: str


class ListPushResponse(BaseModel):
    result: str
    key: str
    length: int


class ListPopResponse(BaseModel):
    key: str
    value: str | None


class LRangeResponse(BaseModel):
    key: str
    values: list[str]


class LLenResponse(BaseModel):
    key: str
    length: int


class LIndexResponse(BaseModel):
    key: str
    index: int
    value: str | None


class LSetRequest(BaseModel):
    value: str


class LSetResponse(BaseModel):
    result: str
    key: str
    index: int
    value: str


class SAddResponse(BaseModel):
    added: int
    key: str


class SRemResponse(BaseModel):
    removed: int
    key: str


class SIsMemberResponse(BaseModel):
    key: str
    member: str
    exists: int


class SMembersResponse(BaseModel):
    key: str
    members: list[str]


class SCardResponse(BaseModel):
    key: str
    count: int


class SetKeysRequest(BaseModel):
    keys: list[str]


class SetCombineResponse(BaseModel):
    keys: list[str]
    members: list[str]


class HashSetRequest(BaseModel):
    value: str


class HashSetResponse(BaseModel):
    key: str
    field: str
    added: int


class HashGetResponse(BaseModel):
    key: str
    field: str
    value: str | None


class HashDeleteResponse(BaseModel):
    key: str
    field: str
    removed: int


class HashGetAllResponse(BaseModel):
    key: str
    values: dict[str, str]


class HashExistsResponse(BaseModel):
    key: str
    field: str
    exists: int


class HashIncrementRequest(BaseModel):
    increment: int


class HashIncrementResponse(BaseModel):
    key: str
    field: str
    value: int


class HashLenResponse(BaseModel):
    key: str
    count: int


class ZAddRequest(BaseModel):
    score: float


class ZAddResponse(BaseModel):
    key: str
    member: str
    added: int
    score: float


class ZScoreResponse(BaseModel):
    key: str
    member: str
    score: float | None


class ZRankResponse(BaseModel):
    key: str
    member: str
    rank: int | None


class ZRangeResponse(BaseModel):
    key: str
    members: list[str]


class ZIncrByRequest(BaseModel):
    increment: float


class ZIncrByResponse(BaseModel):
    key: str
    member: str
    score: float


class ZRemResponse(BaseModel):
    key: str
    member: str
    removed: int


class ZCardResponse(BaseModel):
    key: str
    count: int


def purge_if_expired(key: str) -> None:
    expires_at = expiry_store.get(key)
    if expires_at is not None and expires_at <= time.time():
        redis_store.pop(key, None)
        expiry_store.pop(key, None)


def key_exists(key: str) -> bool:
    purge_if_expired(key)
    return key in redis_store


def get_entry(key: str) -> dict[str, Any] | None:
    purge_if_expired(key)
    return redis_store.get(key)


def set_string_entry(key: str, value: str) -> None:
    redis_store[key] = {"type": "string", "value": value}


def get_string_value(key: str) -> str | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "string":
        raise HTTPException(status_code=400, detail="wrong type operation against non-string value")
    return entry["value"]


def get_list_value(key: str) -> list[str] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "list":
        raise HTTPException(status_code=400, detail="wrong type operation against non-list value")
    return entry["value"]


def ensure_list(key: str) -> list[str]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "list", "value": []}
        return redis_store[key]["value"]
    if entry["type"] != "list":
        raise HTTPException(status_code=400, detail="wrong type operation against non-list value")
    return entry["value"]


def get_set_value(key: str) -> set[str] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "set":
        raise HTTPException(status_code=400, detail="wrong type operation against non-set value")
    return entry["value"]


def ensure_set(key: str) -> set[str]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "set", "value": set()}
        return redis_store[key]["value"]
    if entry["type"] != "set":
        raise HTTPException(status_code=400, detail="wrong type operation against non-set value")
    return entry["value"]


def get_hash_value(key: str) -> dict[str, str] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "hash":
        raise HTTPException(status_code=400, detail="wrong type operation against non-hash value")
    return entry["value"]


def ensure_hash(key: str) -> dict[str, str]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "hash", "value": {}}
        return redis_store[key]["value"]
    if entry["type"] != "hash":
        raise HTTPException(status_code=400, detail="wrong type operation against non-hash value")
    return entry["value"]


def get_hash_field_value(key: str, field: str) -> str | None:
    values = get_hash_value(key)
    if values is None:
        return None
    return values.get(field)


def get_zset_value(key: str) -> dict[str, float] | None:
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "zset":
        raise HTTPException(status_code=400, detail="wrong type operation against non-zset value")
    return entry["value"]


def ensure_zset(key: str) -> dict[str, float]:
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "zset", "value": {}}
        return redis_store[key]["value"]
    if entry["type"] != "zset":
        raise HTTPException(status_code=400, detail="wrong type operation against non-zset value")
    return entry["value"]


def parse_integer_value(value: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="value is not an integer") from exc


def normalize_list_index(length: int, index: int) -> int | None:
    normalized = index if index >= 0 else length + index
    if normalized < 0 or normalized >= length:
        return None
    return normalized


def compute_lrange_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    actual_start = start if start >= 0 else length + start
    actual_stop = stop if stop >= 0 else length + stop
    actual_start = max(actual_start, 0)
    actual_stop = min(actual_stop, length - 1)
    if length == 0 or actual_start >= length or actual_start > actual_stop:
        return 0, 0
    return actual_start, actual_stop + 1


def collect_sets(keys: list[str]) -> list[set[str]]:
    collected: list[set[str]] = []
    for key in keys:
        members = get_set_value(key)
        collected.append(set() if members is None else members)
    return collected


def sorted_zset_items(values: dict[str, float], reverse: bool = False) -> list[tuple[str, float]]:
    if reverse:
        return sorted(values.items(), key=lambda item: (-item[1], item[0]))
    return sorted(values.items(), key=lambda item: (item[1], item[0]))


def find_zset_rank(values: dict[str, float], member: str, reverse: bool = False) -> int | None:
    ordered = sorted_zset_items(values, reverse=reverse)
    for index, (current_member, _) in enumerate(ordered):
        if current_member == member:
            return index
    return None


def slice_zset_members(values: dict[str, float], start: int, stop: int, reverse: bool = False) -> list[str]:
    ordered = sorted_zset_items(values, reverse=reverse)
    slice_start, slice_end = compute_lrange_slice(len(ordered), start, stop)
    return [member for member, _ in ordered[slice_start:slice_end]]


@app.get("/")
def read_root() -> dict[str, str]:
    return {
        "message": (
            "mini-redis is running with a REST-style API. Use /keys/{key}, /lists/{key}/items, "
            "/sets/{key}/members/{member}, /hashes/{key}/fields/{field}, and /zsets/{key}/members/{member}."
        )
    }


@app.put("/keys/{key}", response_model=SetResponse)
def set_value(key: str, payload: SetRequest) -> SetResponse:
    set_string_entry(key, payload.value)
    expiry_store.pop(key, None)
    return SetResponse(result="OK", key=key, value=payload.value)


@app.get("/keys/{key}", response_model=GetResponse)
def get_value(key: str) -> GetResponse:
    return GetResponse(key=key, value=get_string_value(key))


@app.delete("/keys/{key}", response_model=DeleteResponse)
def delete_value(key: str) -> DeleteResponse:
    purge_if_expired(key)
    existed = 1 if key in redis_store else 0
    redis_store.pop(key, None)
    expiry_store.pop(key, None)
    return DeleteResponse(deleted=existed)


@app.post("/keys/{key}/increment", response_model=IncrementResponse)
def increment_value(key: str) -> IncrementResponse:
    with increment_lock:
        current_value = get_string_value(key)
        next_value = 1 if current_value is None else parse_integer_value(current_value) + 1
        set_string_entry(key, str(next_value))
        return IncrementResponse(result="OK", key=key, value=next_value)


@app.post("/keys/{key}/decrement", response_model=DecrementResponse)
def decrement_value(key: str) -> DecrementResponse:
    with increment_lock:
        current_value = get_string_value(key)
        next_value = -1 if current_value is None else parse_integer_value(current_value) - 1
        set_string_entry(key, str(next_value))
        return DecrementResponse(result="OK", key=key, value=next_value)


@app.put("/keys", response_model=MSetResponse)
def mset_values(payload: MSetRequest) -> MSetResponse:
    for item in payload.items:
        if not item.key:
            raise HTTPException(status_code=400, detail="key must not be empty")
    for item in payload.items:
        set_string_entry(item.key, item.value)
        expiry_store.pop(item.key, None)
    return MSetResponse(result="OK", count=len(payload.items))


@app.post("/keys/read", response_model=MGetResponse)
def mget_values(payload: MGetRequest) -> MGetResponse:
    return MGetResponse(values=[get_string_value(key) for key in payload.keys])


@app.get("/keys/{key}/exists", response_model=ExistsResponse)
def exists_value(key: str) -> ExistsResponse:
    return ExistsResponse(exists=1 if key_exists(key) else 0)


@app.get("/keys/{key}/type", response_model=TypeResponse)
def type_value(key: str) -> TypeResponse:
    entry = get_entry(key)
    if entry is None:
        return TypeResponse(key=key, type="none")
    return TypeResponse(key=key, type=entry["type"])


@app.put("/keys/{key}/expiry", response_model=ExpireResponse)
def expire_value(key: str, payload: ExpireRequest) -> ExpireResponse:
    if payload.ttl < 0:
        raise HTTPException(status_code=400, detail="seconds must be non-negative")
    if not key_exists(key):
        return ExpireResponse(updated=0)
    expiry_store[key] = time.time() + payload.ttl
    purge_if_expired(key)
    return ExpireResponse(updated=1 if key in redis_store else 0)


@app.get("/keys/{key}/ttl", response_model=TtlResponse)
def ttl_value(key: str) -> TtlResponse:
    if not key_exists(key):
        return TtlResponse(ttl=-2)
    expires_at = expiry_store.get(key)
    if expires_at is None:
        return TtlResponse(ttl=-1)
    remaining_seconds = max(0, math.ceil(expires_at - time.time()))
    return TtlResponse(ttl=remaining_seconds)


@app.delete("/keys/{key}/expiry", response_model=PersistResponse)
def persist_value(key: str) -> PersistResponse:
    if not key_exists(key):
        return PersistResponse(removed=0)
    removed = 1 if key in expiry_store else 0
    expiry_store.pop(key, None)
    return PersistResponse(removed=removed)


@app.post("/lists/{key}/items/left", response_model=ListPushResponse)
def lpush_value(key: str, payload: ListPushRequest) -> ListPushResponse:
    items = ensure_list(key)
    items.insert(0, payload.value)
    return ListPushResponse(result="OK", key=key, length=len(items))


@app.post("/lists/{key}/items/right", response_model=ListPushResponse)
def rpush_value(key: str, payload: ListPushRequest) -> ListPushResponse:
    items = ensure_list(key)
    items.append(payload.value)
    return ListPushResponse(result="OK", key=key, length=len(items))


@app.delete("/lists/{key}/items/left", response_model=ListPopResponse)
def lpop_value(key: str) -> ListPopResponse:
    items = get_list_value(key)
    if items is None or not items:
        return ListPopResponse(key=key, value=None)
    return ListPopResponse(key=key, value=items.pop(0))


@app.delete("/lists/{key}/items/right", response_model=ListPopResponse)
def rpop_value(key: str) -> ListPopResponse:
    items = get_list_value(key)
    if items is None or not items:
        return ListPopResponse(key=key, value=None)
    return ListPopResponse(key=key, value=items.pop())


@app.get("/lists/{key}/items", response_model=LRangeResponse)
def lrange_value(key: str, start: int = Query(0), stop: int = Query(-1)) -> LRangeResponse:
    items = get_list_value(key)
    if items is None:
        return LRangeResponse(key=key, values=[])
    slice_start, slice_end = compute_lrange_slice(len(items), start, stop)
    return LRangeResponse(key=key, values=items[slice_start:slice_end])


@app.get("/lists/{key}", response_model=LLenResponse)
def llen_value(key: str) -> LLenResponse:
    items = get_list_value(key)
    return LLenResponse(key=key, length=0 if items is None else len(items))


@app.get("/lists/{key}/items/{index}", response_model=LIndexResponse)
def lindex_value(key: str, index: int) -> LIndexResponse:
    items = get_list_value(key)
    if items is None:
        return LIndexResponse(key=key, index=index, value=None)
    normalized = normalize_list_index(len(items), index)
    if normalized is None:
        return LIndexResponse(key=key, index=index, value=None)
    return LIndexResponse(key=key, index=index, value=items[normalized])


@app.put("/lists/{key}/items/{index}", response_model=LSetResponse)
def lset_value(key: str, index: int, payload: LSetRequest) -> LSetResponse:
    items = get_list_value(key)
    if items is None:
        raise HTTPException(status_code=404, detail="no such key")
    normalized = normalize_list_index(len(items), index)
    if normalized is None:
        raise HTTPException(status_code=400, detail="index out of range")
    items[normalized] = payload.value
    return LSetResponse(result="OK", key=key, index=index, value=payload.value)


@app.put("/sets/{key}/members/{member}", response_model=SAddResponse)
def sadd_value(key: str, member: str) -> SAddResponse:
    members = ensure_set(key)
    before_size = len(members)
    members.add(member)
    added = 1 if len(members) > before_size else 0
    return SAddResponse(added=added, key=key)


@app.delete("/sets/{key}/members/{member}", response_model=SRemResponse)
def srem_value(key: str, member: str) -> SRemResponse:
    members = get_set_value(key)
    if members is None or member not in members:
        return SRemResponse(removed=0, key=key)
    members.remove(member)
    return SRemResponse(removed=1, key=key)


@app.get("/sets/{key}/members/{member}", response_model=SIsMemberResponse)
def sismember_value(key: str, member: str) -> SIsMemberResponse:
    members = get_set_value(key)
    exists = 1 if members is not None and member in members else 0
    return SIsMemberResponse(key=key, member=member, exists=exists)


@app.get("/sets/{key}/members", response_model=SMembersResponse)
def smembers_value(key: str) -> SMembersResponse:
    members = get_set_value(key)
    return SMembersResponse(key=key, members=[] if members is None else sorted(members))


@app.post("/sets/intersection", response_model=SetCombineResponse)
def sinter_value(payload: SetKeysRequest) -> SetCombineResponse:
    if not payload.keys:
        return SetCombineResponse(keys=[], members=[])
    sets = collect_sets(payload.keys)
    result = set.intersection(*sets) if sets else set()
    return SetCombineResponse(keys=payload.keys, members=sorted(result))


@app.post("/sets/union", response_model=SetCombineResponse)
def sunion_value(payload: SetKeysRequest) -> SetCombineResponse:
    sets = collect_sets(payload.keys)
    result = set().union(*sets) if sets else set()
    return SetCombineResponse(keys=payload.keys, members=sorted(result))


@app.get("/sets/{key}", response_model=SCardResponse)
def scard_value(key: str) -> SCardResponse:
    members = get_set_value(key)
    return SCardResponse(key=key, count=0 if members is None else len(members))


@app.put("/hashes/{key}/fields/{field}", response_model=HashSetResponse)
def hset_value(key: str, field: str, payload: HashSetRequest) -> HashSetResponse:
    values = ensure_hash(key)
    added = 0 if field in values else 1
    values[field] = payload.value
    return HashSetResponse(key=key, field=field, added=added)


@app.get("/hashes/{key}/fields/{field}", response_model=HashGetResponse)
def hget_value(key: str, field: str) -> HashGetResponse:
    return HashGetResponse(key=key, field=field, value=get_hash_field_value(key, field))


@app.delete("/hashes/{key}/fields/{field}", response_model=HashDeleteResponse)
def hdel_value(key: str, field: str) -> HashDeleteResponse:
    values = get_hash_value(key)
    if values is None or field not in values:
        return HashDeleteResponse(key=key, field=field, removed=0)
    del values[field]
    return HashDeleteResponse(key=key, field=field, removed=1)


@app.get("/hashes/{key}/fields", response_model=HashGetAllResponse)
def hgetall_value(key: str) -> HashGetAllResponse:
    values = get_hash_value(key)
    return HashGetAllResponse(key=key, values={} if values is None else dict(values))


@app.get("/hashes/{key}/fields/{field}/exists", response_model=HashExistsResponse)
def hexists_value(key: str, field: str) -> HashExistsResponse:
    values = get_hash_value(key)
    exists = 1 if values is not None and field in values else 0
    return HashExistsResponse(key=key, field=field, exists=exists)


@app.post("/hashes/{key}/fields/{field}/increment", response_model=HashIncrementResponse)
def hincrby_value(key: str, field: str, payload: HashIncrementRequest) -> HashIncrementResponse:
    with increment_lock:
        values = ensure_hash(key)
        current_value = values.get(field)
        next_value = payload.increment if current_value is None else parse_integer_value(current_value) + payload.increment
        values[field] = str(next_value)
        return HashIncrementResponse(key=key, field=field, value=next_value)


@app.get("/hashes/{key}", response_model=HashLenResponse)
def hlen_value(key: str) -> HashLenResponse:
    values = get_hash_value(key)
    return HashLenResponse(key=key, count=0 if values is None else len(values))


@app.put("/zsets/{key}/members/{member}", response_model=ZAddResponse)
def zadd_value(key: str, member: str, payload: ZAddRequest) -> ZAddResponse:
    values = ensure_zset(key)
    added = 0 if member in values else 1
    values[member] = payload.score
    return ZAddResponse(key=key, member=member, added=added, score=payload.score)


@app.get("/zsets/{key}/members/{member}", response_model=ZScoreResponse)
def zscore_value(key: str, member: str) -> ZScoreResponse:
    values = get_zset_value(key)
    score = None if values is None else values.get(member)
    return ZScoreResponse(key=key, member=member, score=score)


@app.get("/zsets/{key}/members/{member}/rank", response_model=ZRankResponse)
def zrank_value(key: str, member: str) -> ZRankResponse:
    values = get_zset_value(key)
    rank = None if values is None else find_zset_rank(values, member, reverse=False)
    return ZRankResponse(key=key, member=member, rank=rank)


@app.get("/zsets/{key}/members/{member}/reverse-rank", response_model=ZRankResponse)
def zrevrank_value(key: str, member: str) -> ZRankResponse:
    values = get_zset_value(key)
    rank = None if values is None else find_zset_rank(values, member, reverse=True)
    return ZRankResponse(key=key, member=member, rank=rank)


@app.get("/zsets/{key}/members", response_model=ZRangeResponse)
def zrange_value(
    key: str,
    start: int = Query(0),
    stop: int = Query(-1),
    order: Literal["asc", "desc"] = Query("asc"),
) -> ZRangeResponse:
    values = get_zset_value(key)
    if values is None:
        return ZRangeResponse(key=key, members=[])
    return ZRangeResponse(key=key, members=slice_zset_members(values, start, stop, reverse=(order == "desc")))


@app.post("/zsets/{key}/members/{member}/increment", response_model=ZIncrByResponse)
def zincrby_value(key: str, member: str, payload: ZIncrByRequest) -> ZIncrByResponse:
    with increment_lock:
        values = ensure_zset(key)
        current_score = values.get(member, 0.0)
        next_score = current_score + payload.increment
        values[member] = next_score
        return ZIncrByResponse(key=key, member=member, score=next_score)


@app.delete("/zsets/{key}/members/{member}", response_model=ZRemResponse)
def zrem_value(key: str, member: str) -> ZRemResponse:
    values = get_zset_value(key)
    if values is None or member not in values:
        return ZRemResponse(key=key, member=member, removed=0)
    del values[member]
    return ZRemResponse(key=key, member=member, removed=1)


@app.get("/zsets/{key}", response_model=ZCardResponse)
def zcard_value(key: str) -> ZCardResponse:
    values = get_zset_value(key)
    return ZCardResponse(key=key, count=0 if values is None else len(values))


def _get_api_server_config() -> tuple[str, int]:
    host = os.getenv("MINIREDIS_API_HOST", os.getenv("HOST", "0.0.0.0"))
    port_text = os.getenv("MINIREDIS_API_PORT", os.getenv("PORT", "8000"))

    try:
        port = int(port_text)
    except ValueError as exc:
        raise RuntimeError("API port must be an integer.") from exc

    return host, port


if __name__ == "__main__":
    import uvicorn

    host, port = _get_api_server_config()
    uvicorn.run(app, host=host, port=port)
