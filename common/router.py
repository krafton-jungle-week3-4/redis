import math
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .store import expiry_store, redis_store
from .utils import get_entry, key_exists, purge_if_expired

common_router = APIRouter()


class DeleteResponse(BaseModel):
    deleted: int


class ExistsResponse(BaseModel):
    exists: int


class TypeResponse(BaseModel):
    key: str
    type: str


class ExpireResponse(BaseModel):
    updated: int


class TtlResponse(BaseModel):
    ttl: int


class PersistResponse(BaseModel):
    removed: int


@common_router.get("/")
def read_root() -> dict[str, str]:
    return {
        "message": (
            "mini-redis is running. Use string, list, set, hash, and zset commands such as "
            "/set, /get/{key}, /lpush/{key}, /sadd/{key}, /hset/{key}, /zadd/{key}."
        )
    }


@common_router.delete("/delete/{key}", response_model=DeleteResponse)
def delete_value(key: str) -> DeleteResponse:
    purge_if_expired(key)
    existed = 1 if key in redis_store else 0
    redis_store.pop(key, None)
    expiry_store.pop(key, None)
    return DeleteResponse(deleted=existed)


@common_router.get("/exists/{key}", response_model=ExistsResponse)
def exists_value(key: str) -> ExistsResponse:
    return ExistsResponse(exists=1 if key_exists(key) else 0)


@common_router.get("/type/{key}", response_model=TypeResponse)
def type_value(key: str) -> TypeResponse:
    entry = get_entry(key)
    if entry is None:
        return TypeResponse(key=key, type="none")
    return TypeResponse(key=key, type=str(entry["type"]))


@common_router.post("/expire/{key}/{seconds}", response_model=ExpireResponse)
def expire_value(key: str, seconds: int) -> ExpireResponse:
    if seconds < 0:
        raise HTTPException(status_code=400, detail="seconds must be non-negative")
    if not key_exists(key):
        return ExpireResponse(updated=0)
    expiry_store[key] = time.time() + seconds
    purge_if_expired(key)
    return ExpireResponse(updated=1 if key in redis_store else 0)


@common_router.get("/ttl/{key}", response_model=TtlResponse)
def ttl_value(key: str) -> TtlResponse:
    if not key_exists(key):
        return TtlResponse(ttl=-2)
    expires_at = expiry_store.get(key)
    if expires_at is None:
        return TtlResponse(ttl=-1)
    remaining_seconds = max(0, math.ceil(expires_at - time.time()))
    return TtlResponse(ttl=remaining_seconds)


@common_router.post("/persist/{key}", response_model=PersistResponse)
def persist_value(key: str) -> PersistResponse:
    if not key_exists(key):
        return PersistResponse(removed=0)
    removed = 1 if key in expiry_store else 0
    expiry_store.pop(key, None)
    return PersistResponse(removed=removed)
