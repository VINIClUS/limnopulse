from pydantic import BaseModel, ConfigDict, Field


class Principal(BaseModel):
    model_config = ConfigDict(frozen=True)

    cognito_sub: str
    email: str | None = None
    groups: tuple[str, ...] = ()
    access_token: str | None = Field(default=None, exclude=True, repr=False)
    scopes: frozenset[str] = Field(default_factory=frozenset, exclude=True, repr=False)
