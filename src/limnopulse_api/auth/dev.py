from fastapi import Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.errors import AuthError


class DevAuthProvider:
    async def authenticate(self, request: Request) -> Principal:
        sub = request.headers.get("X-Dev-User-Sub")
        if not sub:
            raise AuthError("missing development identity")

        email = request.headers.get("X-Dev-User-Email")
        return Principal(cognito_sub=sub, email=email, groups=())
