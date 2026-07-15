from typing import Protocol

from limnopulse_api.domain.alerts import (
    AlertRule,
    AlertRuleReplacement,
    AlertRuleUpdates,
    AuditContext,
)


class AlertRuleRepository(Protocol):
    async def list_rules(self, tenant_id: str) -> list[AlertRule]:
        raise NotImplementedError

    async def create_rule(self, rule: AlertRule, audit: AuditContext) -> AlertRule:
        raise NotImplementedError

    async def update_rule(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        updates: AlertRuleUpdates,
        audit: AuditContext,
    ) -> AlertRule:
        raise NotImplementedError

    async def replace_rule(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        replacement: AlertRule,
        idempotency_key: str,
        request_hash: str,
        audit: AuditContext,
    ) -> AlertRuleReplacement:
        raise NotImplementedError
