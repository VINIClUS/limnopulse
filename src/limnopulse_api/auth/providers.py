from typing import Protocol

from fastapi import Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.config import Settings


class PrincipalProvider(Protocol):
    async def authenticate(self, request: Request) -> Principal:
        raise NotImplementedError


def build_auth_provider(settings: Settings) -> PrincipalProvider:
    if settings.auth_mode == "dev":
        from limnopulse_api.auth.dev import DevAuthProvider

        return DevAuthProvider()

    from limnopulse_api.auth.cognito import CognitoJwtAuthProvider

    return CognitoJwtAuthProvider(settings=settings)
