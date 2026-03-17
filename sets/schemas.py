from pydantic import BaseModel


class SetMemberRequest(BaseModel):
    member: str


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


class SetCombineResponse(BaseModel):
    keys: list[str]
    members: list[str]
