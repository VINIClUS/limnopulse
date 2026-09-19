from typing import Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field, StrictBool, field_validator


class LeadCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str = Field(min_length=2, max_length=120)
    email: EmailStr = Field(max_length=254)
    phone: str | None = Field(default=None, max_length=30, pattern=r"^[+()\d\s.-]*$")
    property_name: str | None = Field(default=None, max_length=120)
    source: str = Field(min_length=1, max_length=200)
    consent: StrictBool

    @field_validator("consent")
    @classmethod
    def require_consent(cls, value: bool) -> bool:
        if not value:
            raise ValueError("contact authorization is required")
        return value


class LeadResponse(BaseModel):
    lead_id: str
    status: Literal["received"] = "received"
