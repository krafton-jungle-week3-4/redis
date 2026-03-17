from pydantic import BaseModel


class ZAddRequest(BaseModel):
    score: float
    member: str


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
    member: str


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
