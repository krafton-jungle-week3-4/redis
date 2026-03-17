from pydantic import BaseModel


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
