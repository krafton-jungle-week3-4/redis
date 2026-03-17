import math
import time
from threading import Lock
from typing import Any

from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="mini-redis")

# 각 key마다 자료형이 다를 수 있으므로 {"type": ..., "value": ...} 형태로 저장한다.
# {"name": {"type": "string", "value": "redis"}}
# {"numbers": {"type": "list", "value": ["1", "2"]}}
redis_store: dict[str, dict[str, Any]] = {}

# TTL은 실제 값과 분리해서 저장한다.
# 값은 "몇 초 뒤 만료"가 아니라 "언제 만료되는지"를 나타내는 Unix timestamp다.
expiry_store: dict[str, float] = {}

# 읽고-계산하고-다시 쓰는 연산은 동시에 실행되면 값이 꼬일 수 있다.
# 그래서 INCR / DECR / HINCRBY / ZINCRBY 같은 연산은 락으로 보호한다.
increment_lock = Lock()


# 아래 BaseModel 클래스들은 FastAPI가 요청과 응답 JSON 구조를 이해하도록 돕는 스키마다.
# 즉 "이 API는 어떤 모양의 데이터를 받고, 어떤 모양으로 돌려주는가"를 코드로 선언한 부분이다.
class SetRequest(BaseModel):
    # SET 요청 본문
    key: str
    value: str


class SetResponse(BaseModel):
    # SET 성공 응답
    result: str
    key: str
    value: str


class GetResponse(BaseModel):
    # GET 응답
    # key가 없으면 value는 None이다.
    key: str
    value: str | None


class IncrementResponse(BaseModel):
    # INCR 응답
    # 증가 후의 정수 값을 돌려준다.
    result: str
    key: str
    value: int


class DeleteResponse(BaseModel):
    # DEL 응답
    # 실제 삭제된 key가 있으면 1, 없으면 0이다.
    deleted: int


class DecrementResponse(BaseModel):
    # DECR 응답
    # 감소 후의 정수 값을 돌려준다.
    result: str
    key: str
    value: int


class MSetItem(BaseModel):
    # MSET에 들어가는 개별 key-value 항목
    key: str
    value: str


class MSetRequest(BaseModel):
    # MSET 요청 전체
    items: list[MSetItem]


class MSetResponse(BaseModel):
    # MSET 성공 응답
    result: str
    count: int


class MGetResponse(BaseModel):
    # MGET 응답
    # 요청한 key 순서대로 값 목록이 들어간다.
    values: list[str | None]


class ExistsResponse(BaseModel):
    # EXISTS 응답
    exists: int


class TypeResponse(BaseModel):
    # TYPE 응답
    key: str
    type: str


class ExpireResponse(BaseModel):
    # EXPIRE 응답
    updated: int


class TtlResponse(BaseModel):
    # TTL 응답
    ttl: int


class PersistResponse(BaseModel):
    # PERSIST 응답
    removed: int


class ListPushRequest(BaseModel):
    # LPUSH / RPUSH 요청 본문
    value: str


class ListPushResponse(BaseModel):
    # LPUSH / RPUSH 응답
    # 삽입 후 리스트 길이도 함께 돌려준다.
    result: str
    key: str
    length: int


class ListPopResponse(BaseModel):
    # LPOP / RPOP 응답
    # 비어 있으면 value는 None이다.
    key: str
    value: str | None


class LRangeResponse(BaseModel):
    # LRANGE 응답
    key: str
    values: list[str]


class LLenResponse(BaseModel):
    # LLEN 응답
    key: str
    length: int


class LIndexResponse(BaseModel):
    # LINDEX 응답
    key: str
    index: int
    value: str | None


class LSetRequest(BaseModel):
    # LSET 요청 본문
    value: str


class LSetResponse(BaseModel):
    # LSET 성공 응답
    result: str
    key: str
    index: int
    value: str


class SetMemberRequest(BaseModel):
    # SADD / SREM 요청 본문
    member: str


class SAddResponse(BaseModel):
    # SADD 응답
    # 새 멤버가 실제로 추가되면 added는 1이다.
    added: int
    key: str


class SRemResponse(BaseModel):
    # SREM 응답
    # 실제 제거되면 removed는 1이다.
    removed: int
    key: str


class SIsMemberResponse(BaseModel):
    # SISMEMBER 응답
    key: str
    member: str
    exists: int


class SMembersResponse(BaseModel):
    # SMEMBERS 응답
    key: str
    members: list[str]


class SCardResponse(BaseModel):
    # SCARD 응답
    key: str
    count: int


class SetCombineResponse(BaseModel):
    # SINTER / SUNION 응답
    keys: list[str]
    members: list[str]


class HashSetRequest(BaseModel):
    # HSET 요청 본문
    field: str
    value: str


class HashSetResponse(BaseModel):
    # HSET 응답
    # 새 field가 생기면 added는 1이다.
    key: str
    field: str
    added: int


class HashGetResponse(BaseModel):
    # HGET 응답
    key: str
    field: str
    value: str | None


class HashDeleteResponse(BaseModel):
    # HDEL 응답
    key: str
    field: str
    removed: int


class HashGetAllResponse(BaseModel):
    # HGETALL 응답
    key: str
    values: dict[str, str]


class HashExistsResponse(BaseModel):
    # HEXISTS 응답
    key: str
    field: str
    exists: int


class HashIncrementRequest(BaseModel):
    # HINCRBY 요청 본문
    increment: int


class HashIncrementResponse(BaseModel):
    # HINCRBY 응답
    key: str
    field: str
    value: int


class HashLenResponse(BaseModel):
    # HLEN 응답
    key: str
    count: int


class ZAddRequest(BaseModel):
    # ZADD 요청 본문
    score: float
    member: str


class ZAddResponse(BaseModel):
    # ZADD 응답
    # 새 멤버면 added=1, 기존 멤버 점수 갱신이면 added=0이다.
    key: str
    member: str
    added: int
    score: float


class ZScoreResponse(BaseModel):
    # ZSCORE 응답
    key: str
    member: str
    score: float | None


class ZRankResponse(BaseModel):
    # ZRANK / ZREVRANK 응답
    key: str
    member: str
    rank: int | None


class ZRangeResponse(BaseModel):
    # ZRANGE / ZREVRANGE 응답
    key: str
    members: list[str]


class ZIncrByRequest(BaseModel):
    # ZINCRBY 요청 본문
    increment: float
    member: str


class ZIncrByResponse(BaseModel):
    # ZINCRBY 응답
    key: str
    member: str
    score: float


class ZRemResponse(BaseModel):
    # ZREM 응답
    key: str
    member: str
    removed: int


class ZCardResponse(BaseModel):
    # ZCARD 응답
    key: str
    count: int


def purge_if_expired(key: str) -> None:
    # TTL이 지난 key는 값과 TTL 메타데이터를 함께 제거한다.
    # 이렇게 해야 만료된 데이터가 조회에서 계속 보이지 않는다.
    expires_at = expiry_store.get(key)
    if expires_at is not None and expires_at <= time.time():
        redis_store.pop(key, None)
        expiry_store.pop(key, None)


def key_exists(key: str) -> bool:
    # 존재 여부를 확인하기 전에 만료 여부부터 반영한다.
    # 그래야 이미 만료된 key가 남아 있어도 "없는 key"처럼 처리된다.
    purge_if_expired(key)
    return key in redis_store


def get_entry(key: str) -> dict[str, Any] | None:
    # 모든 자료형 조회의 공통 진입점이다.
    # 여기서 먼저 만료 정리를 하고 실제 엔트리를 반환한다.
    purge_if_expired(key)
    return redis_store.get(key)


def set_string_entry(key: str, value: str) -> None:
    # string 자료형 저장 유틸이다.
    redis_store[key] = {"type": "string", "value": value}


def get_string_value(key: str) -> str | None:
    # string 자료형만 읽는 전용 유틸이다.
    # key가 존재하지만 string이 아니면 타입 에러를 낸다.
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "string":
        raise HTTPException(status_code=400, detail="wrong type operation against non-string value")
    return entry["value"]


def get_list_value(key: str) -> list[str] | None:
    # list 자료형만 읽는 전용 유틸이다.
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "list":
        raise HTTPException(status_code=400, detail="wrong type operation against non-list value")
    return entry["value"]


def ensure_list(key: str) -> list[str]:
    # list 연산은 key가 없으면 새 빈 리스트를 만들고 시작한다.
    # 이미 값이 있다면 반드시 list 타입이어야 계속 작업할 수 있다.
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "list", "value": []}
        return redis_store[key]["value"]
    if entry["type"] != "list":
        raise HTTPException(status_code=400, detail="wrong type operation against non-list value")
    return entry["value"]


def get_set_value(key: str) -> set[str] | None:
    # set 자료형만 읽는 전용 유틸이다.
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "set":
        raise HTTPException(status_code=400, detail="wrong type operation against non-set value")
    return entry["value"]


def ensure_set(key: str) -> set[str]:
    # set 연산은 key가 없으면 새 빈 집합을 만들고 시작한다.
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "set", "value": set()}
        return redis_store[key]["value"]
    if entry["type"] != "set":
        raise HTTPException(status_code=400, detail="wrong type operation against non-set value")
    return entry["value"]


def get_hash_value(key: str) -> dict[str, str] | None:
    # hash는 key 아래에 field-value 딕셔너리가 하나 더 있는 구조다.
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "hash":
        raise HTTPException(status_code=400, detail="wrong type operation against non-hash value")
    return entry["value"]


def ensure_hash(key: str) -> dict[str, str]:
    # hash 연산은 key가 없으면 새 빈 딕셔너리를 만들고 시작한다.
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "hash", "value": {}}
        return redis_store[key]["value"]
    if entry["type"] != "hash":
        raise HTTPException(status_code=400, detail="wrong type operation against non-hash value")
    return entry["value"]


def get_zset_value(key: str) -> dict[str, float] | None:
    # zset은 member -> score 형태의 딕셔너리로 저장한다.
    entry = get_entry(key)
    if entry is None:
        return None
    if entry["type"] != "zset":
        raise HTTPException(status_code=400, detail="wrong type operation against non-zset value")
    return entry["value"]


def ensure_zset(key: str) -> dict[str, float]:
    # zset 연산은 key가 없으면 새 빈 랭킹보드를 만들고 시작한다.
    entry = get_entry(key)
    if entry is None:
        redis_store[key] = {"type": "zset", "value": {}}
        return redis_store[key]["value"]
    if entry["type"] != "zset":
        raise HTTPException(status_code=400, detail="wrong type operation against non-zset value")
    return entry["value"]


def parse_integer_value(value: str) -> int:
    # 문자열이 정수로 바뀔 수 있어야 INCR, DECR, HINCRBY 같은 명령을 수행할 수 있다.
    try:
        return int(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="value is not an integer") from exc


def normalize_list_index(length: int, index: int) -> int | None:
    # Redis처럼 음수 인덱스를 허용하기 위해 실제 위치로 바꿔 준다.
    normalized = index if index >= 0 else length + index
    if normalized < 0 or normalized >= length:
        return None
    return normalized


def compute_lrange_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    # LRANGE, ZRANGE 계열은 stop을 포함 범위로 취급한다.
    # 파이썬 슬라이스와 규칙이 다르므로 여기서 보정한다.
    actual_start = start if start >= 0 else length + start
    actual_stop = stop if stop >= 0 else length + stop
    actual_start = max(actual_start, 0)
    actual_stop = min(actual_stop, length - 1)
    if length == 0 or actual_start >= length or actual_start > actual_stop:
        return 0, 0
    return actual_start, actual_stop + 1


def collect_sets(keys: list[str]) -> list[set[str]]:
    # SINTER / SUNION은 여러 key를 집합 목록으로 바꾼 뒤 계산한다.
    # 존재하지 않는 key는 빈 집합처럼 취급한다.
    collected: list[set[str]] = []
    for key in keys:
        members = get_set_value(key)
        collected.append(set() if members is None else members)
    return collected


def get_hash_field_value(key: str, field: str) -> str | None:
    # hash 내부의 특정 field 값을 읽는 공통 유틸이다.
    values = get_hash_value(key)
    if values is None:
        return None
    return values.get(field)


def sorted_zset_items(values: dict[str, float], reverse: bool = False) -> list[tuple[str, float]]:
    # ZSET은 점수(score) 기준으로 정렬된다.
    # 점수가 같으면 member 이름으로 순서를 고정해서 결과가 매번 달라지지 않게 만든다.
    # 오름차순은 (score, member), 내림차순은 (-score, member) 기준이다.
    if reverse:
        return sorted(values.items(), key=lambda item: (-item[1], item[0]))
    return sorted(values.items(), key=lambda item: (item[1], item[0]))


def find_zset_rank(values: dict[str, float], member: str, reverse: bool = False) -> int | None:
    # 정렬된 결과에서 해당 member가 몇 번째인지 찾아 rank를 구한다.
    ordered = sorted_zset_items(values, reverse=reverse)
    for index, (current_member, _) in enumerate(ordered):
        if current_member == member:
            return index
    return None


def slice_zset_members(values: dict[str, float], start: int, stop: int, reverse: bool = False) -> list[str]:
    # ZRANGE / ZREVRANGE는 정렬된 항목에서 범위만 잘라 member 이름만 반환한다.
    ordered = sorted_zset_items(values, reverse=reverse)
    slice_start, slice_end = compute_lrange_slice(len(ordered), start, stop)
    return [member for member, _ in ordered[slice_start:slice_end]]


@app.get("/")
def read_root() -> dict[str, str]:
    # 서버가 살아 있는지 확인할 때 보는 간단한 안내용 엔드포인트다.
    return {
        "message": (
            "mini-redis is running. Use string, list, set, hash, and zset commands such as "
            "/set, /get/{key}, /lpush/{key}, /sadd/{key}, /hset/{key}, /zadd/{key}."
        )
    }


@app.post("/set", response_model=SetResponse)
def set_value(payload: SetRequest) -> SetResponse:
    # SET은 key에 string 값을 저장한다.
    # 기존 값이 어떤 타입이든 string으로 덮어쓴다.
    set_string_entry(payload.key, payload.value)

    # 값을 새로 저장했으므로 이전 TTL은 제거한다.
    expiry_store.pop(payload.key, None)
    return SetResponse(result="OK", key=payload.key, value=payload.value)


@app.get("/get/{key}", response_model=GetResponse)
def get_value(key: str) -> GetResponse:
    # GET은 string key 하나를 읽는다.
    value = get_string_value(key)
    return GetResponse(key=key, value=value)


@app.delete("/delete/{key}", response_model=DeleteResponse)
def delete_value(key: str) -> DeleteResponse:
    # DEL은 자료형과 상관없이 key 자체를 제거한다.
    purge_if_expired(key)
    existed = 1 if key in redis_store else 0
    redis_store.pop(key, None)
    expiry_store.pop(key, None)
    return DeleteResponse(deleted=existed)


@app.post("/increment/{key}", response_model=IncrementResponse)
def increment_value(key: str) -> IncrementResponse:
    # INCR은 읽고 계산한 뒤 다시 쓰는 연산이라 락으로 보호한다.
    with increment_lock:
        current_value = get_string_value(key)
        if current_value is None:
            next_value = 1
        else:
            next_value = parse_integer_value(current_value) + 1
        set_string_entry(key, str(next_value))
        return IncrementResponse(result="OK", key=key, value=next_value)


@app.post("/decrement/{key}", response_model=DecrementResponse)
def decrement_value(key: str) -> DecrementResponse:
    # DECR도 같은 이유로 락 안에서 처리한다.
    with increment_lock:
        current_value = get_string_value(key)
        if current_value is None:
            next_value = -1
        else:
            next_value = parse_integer_value(current_value) - 1
        set_string_entry(key, str(next_value))
        return DecrementResponse(result="OK", key=key, value=next_value)


@app.post("/mset", response_model=MSetResponse)
def mset_values(payload: MSetRequest) -> MSetResponse:
    # MSET은 여러 key-value를 한 번에 저장한다.
    # 최소 구현에서는 먼저 key 유효성만 확인한 뒤 모두 반영한다.
    for item in payload.items:
        if not item.key:
            raise HTTPException(status_code=400, detail="key must not be empty")
    for item in payload.items:
        set_string_entry(item.key, item.value)
        expiry_store.pop(item.key, None)
    return MSetResponse(result="OK", count=len(payload.items))


@app.post("/mget", response_model=MGetResponse)
def mget_values(keys: list[str] = Body(...)) -> MGetResponse:
    # MGET은 요청받은 key 순서를 유지하면서 값을 차례대로 모은다.
    values: list[str | None] = []
    for key in keys:
        values.append(get_string_value(key))
    return MGetResponse(values=values)


@app.get("/exists/{key}", response_model=ExistsResponse)
def exists_value(key: str) -> ExistsResponse:
    # EXISTS는 존재 여부만 1 또는 0으로 반환한다.
    return ExistsResponse(exists=1 if key_exists(key) else 0)


@app.get("/type/{key}", response_model=TypeResponse)
def type_value(key: str) -> TypeResponse:
    # TYPE은 key에 저장된 자료형 이름만 확인한다.
    entry = get_entry(key)
    if entry is None:
        return TypeResponse(key=key, type="none")
    return TypeResponse(key=key, type=entry["type"])


@app.post("/expire/{key}/{seconds}", response_model=ExpireResponse)
def expire_value(key: str, seconds: int) -> ExpireResponse:
    # EXPIRE는 key에 TTL을 설정한다.
    if seconds < 0:
        raise HTTPException(status_code=400, detail="seconds must be non-negative")
    if not key_exists(key):
        return ExpireResponse(updated=0)
    expiry_store[key] = time.time() + seconds
    purge_if_expired(key)
    return ExpireResponse(updated=1 if key in redis_store else 0)


@app.get("/ttl/{key}", response_model=TtlResponse)
def ttl_value(key: str) -> TtlResponse:
    # TTL은 세 가지 경우를 구분한다.
    # key가 없으면 -2, 만료 시간이 없으면 -1, 있으면 남은 초를 반환한다.
    if not key_exists(key):
        return TtlResponse(ttl=-2)
    expires_at = expiry_store.get(key)
    if expires_at is None:
        return TtlResponse(ttl=-1)
    remaining_seconds = max(0, math.ceil(expires_at - time.time()))
    return TtlResponse(ttl=remaining_seconds)


@app.post("/persist/{key}", response_model=PersistResponse)
def persist_value(key: str) -> PersistResponse:
    # PERSIST는 값은 유지하고 TTL만 제거한다.
    if not key_exists(key):
        return PersistResponse(removed=0)
    removed = 1 if key in expiry_store else 0
    expiry_store.pop(key, None)
    return PersistResponse(removed=removed)


@app.post("/sadd/{key}", response_model=SAddResponse)
def sadd_value(key: str, payload: SetMemberRequest) -> SAddResponse:
    # SADD는 집합에 멤버를 추가한다.
    members = ensure_set(key)
    before_size = len(members)
    members.add(payload.member)
    added = 1 if len(members) > before_size else 0
    return SAddResponse(added=added, key=key)


@app.post("/srem/{key}", response_model=SRemResponse)
def srem_value(key: str, payload: SetMemberRequest) -> SRemResponse:
    # SREM은 집합에서 멤버를 제거한다.
    members = get_set_value(key)
    if members is None or payload.member not in members:
        return SRemResponse(removed=0, key=key)
    members.remove(payload.member)
    return SRemResponse(removed=1, key=key)


@app.get("/sismember/{key}/{member}", response_model=SIsMemberResponse)
def sismember_value(key: str, member: str) -> SIsMemberResponse:
    # SISMEMBER는 특정 멤버가 집합 안에 있는지 확인한다.
    members = get_set_value(key)
    exists = 1 if members is not None and member in members else 0
    return SIsMemberResponse(key=key, member=member, exists=exists)


@app.get("/smembers/{key}", response_model=SMembersResponse)
def smembers_value(key: str) -> SMembersResponse:
    # SMEMBERS는 집합 전체를 반환한다.
    # 집합은 순서가 없으므로 보기 쉽게 정렬해서 돌려준다.
    members = get_set_value(key)
    return SMembersResponse(key=key, members=[] if members is None else sorted(members))


@app.post("/sinter", response_model=SetCombineResponse)
def sinter_value(keys: list[str] = Body(...)) -> SetCombineResponse:
    # SINTER는 여러 집합의 교집합을 계산한다.
    if not keys:
        return SetCombineResponse(keys=[], members=[])
    sets = collect_sets(keys)
    result = set.intersection(*sets) if sets else set()
    return SetCombineResponse(keys=keys, members=sorted(result))


@app.post("/sunion", response_model=SetCombineResponse)
def sunion_value(keys: list[str] = Body(...)) -> SetCombineResponse:
    # SUNION은 여러 집합의 합집합을 계산한다.
    sets = collect_sets(keys)
    result = set().union(*sets) if sets else set()
    return SetCombineResponse(keys=keys, members=sorted(result))


@app.get("/scard/{key}", response_model=SCardResponse)
def scard_value(key: str) -> SCardResponse:
    # SCARD는 집합 원소 개수만 반환한다.
    members = get_set_value(key)
    return SCardResponse(key=key, count=0 if members is None else len(members))


@app.post("/hset/{key}", response_model=HashSetResponse)
def hset_value(key: str, payload: HashSetRequest) -> HashSetResponse:
    # HSET은 hash 안의 field에 값을 저장한다.
    values = ensure_hash(key)
    added = 0 if payload.field in values else 1
    values[payload.field] = payload.value
    return HashSetResponse(key=key, field=payload.field, added=added)


@app.get("/hget/{key}/{field}", response_model=HashGetResponse)
def hget_value(key: str, field: str) -> HashGetResponse:
    # HGET은 hash 안의 특정 field 하나를 읽는다.
    value = get_hash_field_value(key, field)
    return HashGetResponse(key=key, field=field, value=value)


@app.delete("/hdel/{key}/{field}", response_model=HashDeleteResponse)
def hdel_value(key: str, field: str) -> HashDeleteResponse:
    # HDEL은 hash 안의 field를 제거한다.
    values = get_hash_value(key)
    if values is None or field not in values:
        return HashDeleteResponse(key=key, field=field, removed=0)
    del values[field]
    return HashDeleteResponse(key=key, field=field, removed=1)


@app.get("/hgetall/{key}", response_model=HashGetAllResponse)
def hgetall_value(key: str) -> HashGetAllResponse:
    # HGETALL은 hash 안의 모든 field-value를 반환한다.
    values = get_hash_value(key)
    return HashGetAllResponse(key=key, values={} if values is None else dict(values))


@app.get("/hexists/{key}/{field}", response_model=HashExistsResponse)
def hexists_value(key: str, field: str) -> HashExistsResponse:
    # HEXISTS는 특정 field 존재 여부만 확인한다.
    values = get_hash_value(key)
    exists = 1 if values is not None and field in values else 0
    return HashExistsResponse(key=key, field=field, exists=exists)


@app.post("/hincrby/{key}/{field}", response_model=HashIncrementResponse)
def hincrby_value(key: str, field: str, payload: HashIncrementRequest) -> HashIncrementResponse:
    # HINCRBY는 hash 안의 field 값을 정수로 보고 increment만큼 더한다.
    with increment_lock:
        values = ensure_hash(key)
        current_value = values.get(field)
        if current_value is None:
            next_value = payload.increment
        else:
            next_value = parse_integer_value(current_value) + payload.increment
        values[field] = str(next_value)
        return HashIncrementResponse(key=key, field=field, value=next_value)


@app.get("/hlen/{key}", response_model=HashLenResponse)
def hlen_value(key: str) -> HashLenResponse:
    # HLEN은 hash 안에 들어 있는 field 개수를 반환한다.
    values = get_hash_value(key)
    return HashLenResponse(key=key, count=0 if values is None else len(values))


@app.post("/zadd/{key}", response_model=ZAddResponse)
def zadd_value(key: str, payload: ZAddRequest) -> ZAddResponse:
    # ZADD는 멤버를 score와 함께 저장한다.
    # 이미 있던 멤버면 score를 갱신하고, 새 멤버면 추가로 본다.
    values = ensure_zset(key)
    added = 0 if payload.member in values else 1
    values[payload.member] = payload.score
    return ZAddResponse(key=key, member=payload.member, added=added, score=payload.score)


@app.get("/zscore/{key}/{member}", response_model=ZScoreResponse)
def zscore_value(key: str, member: str) -> ZScoreResponse:
    # ZSCORE는 특정 멤버의 현재 점수를 조회한다.
    values = get_zset_value(key)
    score = None if values is None else values.get(member)
    return ZScoreResponse(key=key, member=member, score=score)


@app.get("/zrank/{key}/{member}", response_model=ZRankResponse)
def zrank_value(key: str, member: str) -> ZRankResponse:
    # ZRANK는 점수가 낮은 순 정렬에서 rank를 구한다.
    values = get_zset_value(key)
    rank = None if values is None else find_zset_rank(values, member, reverse=False)
    return ZRankResponse(key=key, member=member, rank=rank)


@app.get("/zrevrank/{key}/{member}", response_model=ZRankResponse)
def zrevrank_value(key: str, member: str) -> ZRankResponse:
    # ZREVRANK는 점수가 높은 순 정렬에서 rank를 구한다.
    values = get_zset_value(key)
    rank = None if values is None else find_zset_rank(values, member, reverse=True)
    return ZRankResponse(key=key, member=member, rank=rank)


@app.get("/zrange/{key}/{start}/{stop}", response_model=ZRangeResponse)
def zrange_value(key: str, start: int, stop: int) -> ZRangeResponse:
    # ZRANGE는 점수 낮은 순으로 범위를 잘라 member 목록만 반환한다.
    values = get_zset_value(key)
    if values is None:
        return ZRangeResponse(key=key, members=[])
    return ZRangeResponse(key=key, members=slice_zset_members(values, start, stop, reverse=False))


@app.get("/zrevrange/{key}/{start}/{stop}", response_model=ZRangeResponse)
def zrevrange_value(key: str, start: int, stop: int) -> ZRangeResponse:
    # ZREVRANGE는 점수 높은 순으로 범위를 잘라 member 목록만 반환한다.
    values = get_zset_value(key)
    if values is None:
        return ZRangeResponse(key=key, members=[])
    return ZRangeResponse(key=key, members=slice_zset_members(values, start, stop, reverse=True))


@app.post("/zincrby/{key}", response_model=ZIncrByResponse)
def zincrby_value(key: str, payload: ZIncrByRequest) -> ZIncrByResponse:
    # ZINCRBY는 랭킹보드에서 가장 중요한 연산이다.
    # 멤버가 없으면 0점에서 시작해 increment를 더하고,
    # 이미 있으면 기존 score에 increment를 더한다.
    with increment_lock:
        values = ensure_zset(key)
        current_score = values.get(payload.member, 0.0)
        next_score = current_score + payload.increment
        values[payload.member] = next_score
        return ZIncrByResponse(key=key, member=payload.member, score=next_score)


@app.delete("/zrem/{key}/{member}", response_model=ZRemResponse)
def zrem_value(key: str, member: str) -> ZRemResponse:
    # ZREM은 랭킹보드에서 특정 멤버를 제거한다.
    values = get_zset_value(key)
    if values is None or member not in values:
        return ZRemResponse(key=key, member=member, removed=0)
    del values[member]
    return ZRemResponse(key=key, member=member, removed=1)


@app.get("/zcard/{key}", response_model=ZCardResponse)
def zcard_value(key: str) -> ZCardResponse:
    # ZCARD는 랭킹보드 전체 멤버 수를 반환한다.
    values = get_zset_value(key)
    return ZCardResponse(key=key, count=0 if values is None else len(values))


@app.post("/lpush/{key}", response_model=ListPushResponse)
def lpush_value(key: str, payload: ListPushRequest) -> ListPushResponse:
    # LPUSH는 리스트의 맨 앞에 값을 넣는다.
    items = ensure_list(key)
    items.insert(0, payload.value)
    return ListPushResponse(result="OK", key=key, length=len(items))


@app.post("/rpush/{key}", response_model=ListPushResponse)
def rpush_value(key: str, payload: ListPushRequest) -> ListPushResponse:
    # RPUSH는 리스트의 맨 뒤에 값을 넣는다.
    items = ensure_list(key)
    items.append(payload.value)
    return ListPushResponse(result="OK", key=key, length=len(items))


@app.post("/lpop/{key}", response_model=ListPopResponse)
def lpop_value(key: str) -> ListPopResponse:
    # LPOP은 리스트 맨 앞 원소를 꺼낸다.
    items = get_list_value(key)
    if items is None or not items:
        return ListPopResponse(key=key, value=None)
    return ListPopResponse(key=key, value=items.pop(0))


@app.post("/rpop/{key}", response_model=ListPopResponse)
def rpop_value(key: str) -> ListPopResponse:
    # RPOP은 리스트 맨 뒤 원소를 꺼낸다.
    items = get_list_value(key)
    if items is None or not items:
        return ListPopResponse(key=key, value=None)
    return ListPopResponse(key=key, value=items.pop())


@app.get("/lrange/{key}/{start}/{stop}", response_model=LRangeResponse)
def lrange_value(key: str, start: int, stop: int) -> LRangeResponse:
    # LRANGE는 지정한 구간의 리스트 원소를 반환한다.
    items = get_list_value(key)
    if items is None:
        return LRangeResponse(key=key, values=[])
    slice_start, slice_end = compute_lrange_slice(len(items), start, stop)
    return LRangeResponse(key=key, values=items[slice_start:slice_end])


@app.get("/llen/{key}", response_model=LLenResponse)
def llen_value(key: str) -> LLenResponse:
    # LLEN은 리스트 길이만 반환한다.
    items = get_list_value(key)
    return LLenResponse(key=key, length=0 if items is None else len(items))


@app.get("/lindex/{key}/{index}", response_model=LIndexResponse)
def lindex_value(key: str, index: int) -> LIndexResponse:
    # LINDEX는 특정 위치의 원소 하나만 읽는다.
    items = get_list_value(key)
    if items is None:
        return LIndexResponse(key=key, index=index, value=None)
    normalized = normalize_list_index(len(items), index)
    if normalized is None:
        return LIndexResponse(key=key, index=index, value=None)
    return LIndexResponse(key=key, index=index, value=items[normalized])


@app.post("/lset/{key}/{index}", response_model=LSetResponse)
def lset_value(key: str, index: int, payload: LSetRequest) -> LSetResponse:
    # LSET은 기존 리스트의 특정 위치를 새 값으로 덮어쓴다.
    items = get_list_value(key)
    if items is None:
        raise HTTPException(status_code=404, detail="no such key")
    normalized = normalize_list_index(len(items), index)
    if normalized is None:
        raise HTTPException(status_code=400, detail="index out of range")
    items[normalized] = payload.value
    return LSetResponse(result="OK", key=key, index=index, value=payload.value)