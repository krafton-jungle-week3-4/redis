from fastapi import APIRouter

from common import ensure_zset, find_zset_rank, get_zset_value, increment_lock, slice_zset_members

from .schemas import ZAddRequest, ZAddResponse, ZCardResponse, ZIncrByRequest, ZIncrByResponse, ZRangeResponse, ZRankResponse, ZRemResponse, ZScoreResponse

router = APIRouter()


@router.post("/zadd/{key}", response_model=ZAddResponse)
def zadd_value(key: str, payload: ZAddRequest) -> ZAddResponse:
    values = ensure_zset(key)
    added = 0 if payload.member in values else 1
    values[payload.member] = payload.score
    return ZAddResponse(key=key, member=payload.member, added=added, score=payload.score)


@router.get("/zscore/{key}/{member}", response_model=ZScoreResponse)
def zscore_value(key: str, member: str) -> ZScoreResponse:
    values = get_zset_value(key)
    score = None if values is None else values.get(member)
    return ZScoreResponse(key=key, member=member, score=score)


@router.get("/zrank/{key}/{member}", response_model=ZRankResponse)
def zrank_value(key: str, member: str) -> ZRankResponse:
    values = get_zset_value(key)
    rank = None if values is None else find_zset_rank(values, member, reverse=False)
    return ZRankResponse(key=key, member=member, rank=rank)


@router.get("/zrevrank/{key}/{member}", response_model=ZRankResponse)
def zrevrank_value(key: str, member: str) -> ZRankResponse:
    values = get_zset_value(key)
    rank = None if values is None else find_zset_rank(values, member, reverse=True)
    return ZRankResponse(key=key, member=member, rank=rank)


@router.get("/zrange/{key}/{start}/{stop}", response_model=ZRangeResponse)
def zrange_value(key: str, start: int, stop: int) -> ZRangeResponse:
    values = get_zset_value(key)
    if values is None:
        return ZRangeResponse(key=key, members=[])
    return ZRangeResponse(key=key, members=slice_zset_members(values, start, stop, reverse=False))


@router.get("/zrevrange/{key}/{start}/{stop}", response_model=ZRangeResponse)
def zrevrange_value(key: str, start: int, stop: int) -> ZRangeResponse:
    values = get_zset_value(key)
    if values is None:
        return ZRangeResponse(key=key, members=[])
    return ZRangeResponse(key=key, members=slice_zset_members(values, start, stop, reverse=True))


@router.post("/zincrby/{key}", response_model=ZIncrByResponse)
def zincrby_value(key: str, payload: ZIncrByRequest) -> ZIncrByResponse:
    with increment_lock:
        values = ensure_zset(key)
        current_score = values.get(payload.member, 0.0)
        next_score = current_score + payload.increment
        values[payload.member] = next_score
        return ZIncrByResponse(key=key, member=payload.member, score=next_score)


@router.delete("/zrem/{key}/{member}", response_model=ZRemResponse)
def zrem_value(key: str, member: str) -> ZRemResponse:
    values = get_zset_value(key)
    if values is None or member not in values:
        return ZRemResponse(key=key, member=member, removed=0)
    del values[member]
    return ZRemResponse(key=key, member=member, removed=1)


@router.get("/zcard/{key}", response_model=ZCardResponse)
def zcard_value(key: str) -> ZCardResponse:
    values = get_zset_value(key)
    return ZCardResponse(key=key, count=0 if values is None else len(values))
