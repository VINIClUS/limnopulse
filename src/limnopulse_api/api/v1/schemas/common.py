from pydantic import BaseModel


class VersionedResponse(BaseModel):
    created_at: str
    updated_at: str
    version: int
    status: str


class ErrorResponse(BaseModel):
    detail: str
