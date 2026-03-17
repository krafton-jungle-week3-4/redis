from pydantic import BaseModel


class SetRequest(BaseModel):
    key: str
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


class MGetResponse(BaseModel):
    values: list[str | None]
