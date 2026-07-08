from pydantic import BaseModel, ConfigDict


class Principal(BaseModel):
    model_config = ConfigDict(frozen=True)

    cognito_sub: str
    email: str | None = None
    groups: tuple[str, ...] = ()
