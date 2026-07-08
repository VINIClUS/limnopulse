import pytest
from fastapi import Request

from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.core.errors import AuthError


def build_request(headers: dict[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "headers": [(key.lower().encode(), value.encode()) for key, value in headers.items()],
        }
    )


@pytest.mark.asyncio
async def test_dev_auth_reads_identity_headers() -> None:
    provider = DevAuthProvider()

    principal = await provider.authenticate(
        build_request(
            {
                "X-Dev-User-Sub": "user-1",
                "X-Dev-User-Email": "user@example.test",
                "X-Dev-User-Groups": "ops, support",
            }
        )
    )

    assert principal.cognito_sub == "user-1"
    assert principal.email == "user@example.test"
    assert principal.groups == ()


@pytest.mark.asyncio
async def test_dev_auth_missing_sub_raises_auth_error() -> None:
    provider = DevAuthProvider()

    with pytest.raises(AuthError):
        await provider.authenticate(build_request({}))
