from pydantic import BaseModel


class HashSetRequest(BaseModel):
    field: str
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
