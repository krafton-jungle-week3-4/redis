from fastapi import APIRouter, Body

from common import collect_sets, ensure_set, get_set_value

from .schemas import SAddResponse, SCardResponse, SIsMemberResponse, SMembersResponse, SRemResponse, SetCombineResponse, SetMemberRequest

router = APIRouter()


@router.post("/sadd/{key}", response_model=SAddResponse)
def sadd_value(key: str, payload: SetMemberRequest) -> SAddResponse:
    members = ensure_set(key)
    before_size = len(members)
    members.add(payload.member)
    added = 1 if len(members) > before_size else 0
    return SAddResponse(added=added, key=key)


@router.post("/srem/{key}", response_model=SRemResponse)
def srem_value(key: str, payload: SetMemberRequest) -> SRemResponse:
    members = get_set_value(key)
    if members is None or payload.member not in members:
        return SRemResponse(removed=0, key=key)
    members.remove(payload.member)
    return SRemResponse(removed=1, key=key)


@router.get("/sismember/{key}/{member}", response_model=SIsMemberResponse)
def sismember_value(key: str, member: str) -> SIsMemberResponse:
    members = get_set_value(key)
    exists = 1 if members is not None and member in members else 0
    return SIsMemberResponse(key=key, member=member, exists=exists)


@router.get("/smembers/{key}", response_model=SMembersResponse)
def smembers_value(key: str) -> SMembersResponse:
    members = get_set_value(key)
    return SMembersResponse(key=key, members=[] if members is None else sorted(members))


@router.post("/sinter", response_model=SetCombineResponse)
def sinter_value(keys: list[str] = Body(...)) -> SetCombineResponse:
    if not keys:
        return SetCombineResponse(keys=[], members=[])
    sets = collect_sets(keys)
    result = set.intersection(*sets) if sets else set()
    return SetCombineResponse(keys=keys, members=sorted(result))


@router.post("/sunion", response_model=SetCombineResponse)
def sunion_value(keys: list[str] = Body(...)) -> SetCombineResponse:
    sets = collect_sets(keys)
    result = set().union(*sets) if sets else set()
    return SetCombineResponse(keys=keys, members=sorted(result))


@router.get("/scard/{key}", response_model=SCardResponse)
def scard_value(key: str) -> SCardResponse:
    members = get_set_value(key)
    return SCardResponse(key=key, count=0 if members is None else len(members))
