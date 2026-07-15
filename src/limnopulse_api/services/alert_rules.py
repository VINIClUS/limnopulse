from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from hashlib import sha256
import json
from typing import Any

from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.alerts import AlertRule, AlertRuleReplacement, AuditContext
from limnopulse_api.domain.ids import new_alert_rule_id
from limnopulse_api.repositories.alerts import AlertRuleRepository
from limnopulse_api.repositories.domain import DomainRepository


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class AlertRuleService:
    def __init__(
        self,
        repository: AlertRuleRepository,
        domain_repository: DomainRepository,
        *,
        clock: Callable[[], datetime] = utc_now,
        id_factory: Callable[[], str] = new_alert_rule_id,
    ) -> None:
        self.repository = repository
        self.domain_repository = domain_repository
        self.clock = clock
        self.id_factory = id_factory

    async def list(self, tenant_id: str) -> list[AlertRule]:
        return await self.repository.list_rules(tenant_id)

    async def create(
        self,
        tenant_id: str,
        definition: Mapping[str, Any],
        audit: AuditContext,
    ) -> AlertRule:
        await self._validate_target(
            tenant_id,
            pond_id=str(definition["pond_id"]),
            device_id=self._optional_string(definition.get("device_id")),
        )
        now = self.clock()
        rule = AlertRule.model_validate(
            {
                **dict(definition),
                "tenant_id": tenant_id,
                "rule_id": self.id_factory(),
                "created_at": now,
                "updated_at": now,
                "version": 1,
                "status": "active",
            }
        )
        return await self.repository.create_rule(rule, audit)

    async def update(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        updates: Mapping[str, Any],
        audit: AuditContext,
    ) -> AlertRule:
        return await self.repository.update_rule(
            tenant_id,
            rule_id,
            expected_version,
            dict(updates),
            audit,
        )

    async def replace(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        definition: Mapping[str, Any],
        idempotency_key: str,
        audit: AuditContext,
    ) -> AlertRuleReplacement:
        request_hash = self._replacement_request_hash(
            tenant_id,
            rule_id,
            expected_version,
            definition,
        )
        replay = await self.repository.get_replacement_replay(
            tenant_id,
            idempotency_key,
            request_hash,
        )
        if replay is not None:
            return replay
        await self._validate_target(
            tenant_id,
            pond_id=str(definition["pond_id"]),
            device_id=self._optional_string(definition.get("device_id")),
        )
        now = self.clock()
        replacement = AlertRule.model_validate(
            {
                **dict(definition),
                "tenant_id": tenant_id,
                "rule_id": self.id_factory(),
                "replaces_rule_id": rule_id,
                "created_at": now,
                "updated_at": now,
                "version": 1,
                "status": "active",
            }
        )
        return await self.repository.replace_rule(
            tenant_id,
            rule_id,
            expected_version,
            replacement,
            idempotency_key,
            request_hash,
            audit,
        )

    async def _validate_target(
        self,
        tenant_id: str,
        *,
        pond_id: str,
        device_id: str | None,
    ) -> None:
        pond = await self.domain_repository.get_pond(tenant_id, pond_id)
        if pond is None:
            raise NotFoundError("alert rule target not found")
        if device_id is None:
            return
        device = await self.domain_repository.get_device(tenant_id, device_id)
        if device is None or device.pond_id != pond_id:
            raise NotFoundError("alert rule target not found")

    def _replacement_request_hash(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        definition: Mapping[str, Any],
    ) -> str:
        canonical = json.dumps(
            {
                "tenant_id": tenant_id,
                "rule_id": rule_id,
                "expected_version": expected_version,
                "replacement": dict(definition),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        return sha256(canonical).hexdigest()

    def _optional_string(self, value: Any) -> str | None:
        if value is None:
            return None
        return str(value)
