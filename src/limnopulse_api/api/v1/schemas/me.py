from pydantic import BaseModel


class MeResponse(BaseModel):
    cognito_sub: str
    email: str | None = None
    groups: tuple[str, ...] = ()
