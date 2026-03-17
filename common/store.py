from threading import Lock
from typing import Any

from fastapi import FastAPI

app = FastAPI(title="mini-redis")

redis_store: dict[str, dict[str, Any]] = {}
expiry_store: dict[str, float] = {}
increment_lock = Lock()
