"""Redacted validation errors: 422 bodies never echo rejected input."""

from collections.abc import Callable, Coroutine, Sequence
from typing import Any

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import ValidationError

from limnopulse_api.domain.ingestion import MAX_VALIDATION_ERRORS

# Validator messages can embed input (e.g. unknown source_metadata keys), so `/ingest` replaces
# every message with a fixed string chosen by the error type.
_FIXED_MESSAGES = {"missing": "field required", "extra_forbidden": "extra fields not permitted"}
_DEFAULT_MESSAGE = "invalid value"


class _RedactedValidationRoute(APIRoute):
    """Drop rejected input values from 422 bodies so secrets and identities are never echoed."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def redacted_handler(request: Request) -> Response:
            try:
                return await handler(request)
            except RequestValidationError as exc:
                errors = [
                    {key: value for key, value in error.items() if key not in {"input", "ctx"}}
                    for error in exc.errors()
                ]
                raise RequestValidationError(errors) from None

        return redacted_handler


def _redacted_loc(loc: Sequence[str | int], error_type: str) -> list[str | int]:
    """Locations name only schema fields: client-chosen keys (an unknown field, a
    `source_metadata` key) are cut off, so the body never echoes them."""
    redacted = list(loc[:-1] if error_type == "extra_forbidden" else loc)
    if "source_metadata" in redacted:
        redacted = redacted[: redacted.index("source_metadata") + 1]
    return redacted


def redacted_validation_detail(
    exc: ValidationError, *, loc_prefix: Sequence[str | int] = ("body",)
) -> list[dict[str, Any]]:
    """At most 20 `{loc, msg, type}` entries with fixed messages; never `input`, `ctx` or `url`."""
    errors = exc.errors(include_input=False, include_context=False, include_url=False)
    return [
        {
            "loc": [*loc_prefix, *_redacted_loc(error["loc"], error["type"])],
            "msg": _FIXED_MESSAGES.get(error["type"], _DEFAULT_MESSAGE),
            "type": error["type"],
        }
        for error in errors[:MAX_VALIDATION_ERRORS]
    ]
