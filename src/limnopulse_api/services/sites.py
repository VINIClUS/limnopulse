from collections.abc import Callable
from datetime import datetime

from limnopulse_api.adapters.dynamodb_v2 import (
    canonical_request_hash,
    idempotency_key_hash,
    utc_now,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import Site, SiteStatus, ensure_site_transition
from limnopulse_api.domain.ids import default_site_id, new_site_id
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import SiteAssetRepository

SITE_CREATE_SCOPE = "site-create"


def site_create_request_hash(tenant_id: str, body: dict[str, object]) -> str:
    """Hash of the client request only: scope, path parameters and the JSON body."""
    return canonical_request_hash(
        {"scope": SITE_CREATE_SCOPE, "path": {"tenant_id": tenant_id}, "body": body}
    )


class SiteService:
    def __init__(
        self,
        repository: SiteAssetRepository,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.repository = repository
        self.clock = clock

    async def list(self, tenant_id: str) -> list[Site]:
        return await self.repository.list_sites(tenant_id)

    async def get(self, tenant_id: str, site_id: str) -> Site:
        site = await self.repository.get_site(tenant_id, site_id)
        if site is None:
            raise NotFoundError("site not found")
        return site

    async def create(self, *, tenant_id: str, name: str, idempotency_key: str) -> Site:
        key_hash = idempotency_key_hash(idempotency_key)
        request_hash = site_create_request_hash(tenant_id, {"name": name})
        snapshot = await self.repository.get_idempotency_replay(
            tenant_id, SITE_CREATE_SCOPE, key_hash, request_hash
        )
        if snapshot is not None:
            return Site.model_validate_json(snapshot)

        now = self.clock()
        site = Site(
            tenant_id=tenant_id,
            site_id=new_site_id(),
            name=name,
            status=SiteStatus.ACTIVE,
            created_at=now,
            updated_at=now,
            version=1,
        )
        receipt = new_idempotency_receipt(
            tenant_id=tenant_id,
            scope=SITE_CREATE_SCOPE,
            key_hash=key_hash,
            request_hash=request_hash,
            response_snapshot_json=site.model_dump_json(),
            created_at=now,
        )
        return await self.repository.create_site(site, receipt)

    async def update(
        self,
        *,
        tenant_id: str,
        site_id: str,
        expected_version: int,
        name: str | None,
        status: SiteStatus | str | None,
    ) -> Site:
        target = SiteStatus(status) if status is not None else None
        current = await self.get(tenant_id, site_id)
        if current.version != expected_version:
            raise ConflictError("site version conflict")
        if target is not None:
            ensure_site_transition(current.status, target)
            if (
                target is SiteStatus.ARCHIVED
                and current.status is not SiteStatus.ARCHIVED
                and site_id == default_site_id(tenant_id)
            ):
                raise ConflictError("the default site cannot be archived")
        return await self.repository.update_site(
            tenant_id, site_id, expected_version, name=name, status=target
        )
