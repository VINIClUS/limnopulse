from datetime import datetime
from typing import Annotated, Literal

from fastapi import Header
from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.domain.v2_common import (
    IDEMPOTENCY_KEY_MAX_LENGTH,
    IDEMPOTENCY_KEY_MIN_LENGTH,
    format_utc_timestamp,
)

UtcTimestamp = Annotated[
    datetime,
    PlainSerializer(format_utc_timestamp, return_type=str, when_used="json"),
]

IdempotencyKeyHeader = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=IDEMPOTENCY_KEY_MIN_LENGTH,
        max_length=IDEMPOTENCY_KEY_MAX_LENGTH,
    ),
]

V2_ERROR_RESPONSES: dict[int | str, dict[str, object]] = {
    404: {"model": ErrorResponse},
    409: {"model": ErrorResponse},
    503: {"model": ErrorResponse},
}


class V2VersionedResponse(BaseModel):
    """Fields shared by every `/v2` resource response; subclasses add IDs and narrow `status`."""

    tenant_id: str
    status: str
    schema_version: Literal[2] = 2
    created_at: UtcTimestamp
    updated_at: UtcTimestamp
    version: int


class V2ExpectedVersionRequest(BaseModel):
    """Base for `/v2` mutations fenced by optimistic concurrency."""

    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1)
