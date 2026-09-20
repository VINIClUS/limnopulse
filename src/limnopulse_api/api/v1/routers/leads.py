import hashlib
import time

from fastapi import APIRouter, HTTPException, Request
from redis.exceptions import RedisError

from limnopulse_api.api.dependencies import _get_state_dependency
from limnopulse_api.api.v1.schemas.leads import LeadCreate, LeadResponse

router = APIRouter(prefix="/leads", tags=["leads"])


@router.post("", status_code=201, response_model=LeadResponse)
async def create_lead(payload: LeadCreate, request: Request) -> LeadResponse:
    redis = _get_state_dependency(request, "redis_client")
    repository = _get_state_dependency(request, "lead_repository")
    # Use the ASGI peer, never untrusted forwarding headers. Configure trusted
    # proxies at the ASGI server when deployed behind a reverse proxy.
    peer = request.client.host if request.client else "unknown"
    key = hashlib.sha256(peer.encode()).hexdigest()
    minute = int(time.time()) // 60
    try:
        async with redis.pipeline(transaction=True) as pipeline:
            pipeline.incr(f"leads:rate:{key}:{minute}")
            pipeline.expire(f"leads:rate:{key}:{minute}", 120)
            count, _ = await pipeline.execute()
    except RedisError as exc:
        raise HTTPException(503, "service unavailable") from exc
    if count > request.app.state.settings.lead_rate_limit_per_minute:
        raise HTTPException(
            429, "too many requests", headers={"Retry-After": str(60 - int(time.time()) % 60)}
        )
    lead_id = await repository.create(payload)
    return LeadResponse(lead_id=lead_id)
