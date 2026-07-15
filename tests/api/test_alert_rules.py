from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import (
    AlertAggregation,
    AlertChannel,
    AlertMetric,
    AlertOperator,
    AlertRule,
    AlertRuleReplacement,
    AlertSeverity,
    AuditContext,
)
from limnopulse_api.domain.entities import Device, Membership, Pond
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


class FakeMembershipService:
    def __init__(self, role: TenantRole) -> None:
        self.membership = Membership(
            tenant_id="tnt_1",
            cognito_sub="sub_1",
            role=role,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )

    async def get_active_membership(
        self,
        cognito_sub: str,
        tenant_id: str,
    ) -> Membership | None:
        if cognito_sub != "sub_1" or tenant_id != "tnt_1":
            return None
        return self.membership


class FakeDomainRepository:
    def __init__(self) -> None:
        self.ponds = {
            "pond_1": Pond(
                tenant_id="tnt_1",
                pond_id="pond_1",
                name="West",
                created_at=NOW,
                updated_at=NOW,
                version=1,
            )
        }
        self.devices = {
            "dev_1": Device(
                tenant_id="tnt_1",
                pond_id="pond_1",
                device_id="dev_1",
                name="Probe",
                created_at=NOW,
                updated_at=NOW,
                version=1,
            ),
            "dev_other_pond": Device(
                tenant_id="tnt_1",
                pond_id="pond_2",
                device_id="dev_other_pond",
                name="Other probe",
                created_at=NOW,
                updated_at=NOW,
                version=1,
            ),
        }

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        pond = self.ponds.get(pond_id)
        if pond is None or pond.tenant_id != tenant_id:
            return None
        return pond

    async def get_device(self, tenant_id: str, device_id: str) -> Device | None:
        device = self.devices.get(device_id)
        if device is None or device.tenant_id != tenant_id:
            return None
        return device


class FakeAlertRuleRepository:
    def __init__(self) -> None:
        self.rules: dict[str, AlertRule] = {"rule_1": make_rule()}
        self.audits: list[AuditContext] = []
        self.idempotency: dict[str, tuple[str, AlertRuleReplacement]] = {}

    async def list_rules(self, tenant_id: str) -> list[AlertRule]:
        return [rule for rule in self.rules.values() if rule.tenant_id == tenant_id]

    async def create_rule(self, rule: AlertRule, audit: AuditContext) -> AlertRule:
        self.rules[rule.rule_id] = rule
        self.audits.append(audit)
        return rule

    async def update_rule(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        updates: dict[str, Any],
        audit: AuditContext,
    ) -> AlertRule:
        existing = self.rules.get(rule_id)
        if existing is None or existing.tenant_id != tenant_id:
            raise NotFoundError("not found")
        if existing.version != expected_version:
            raise ConflictError("version conflict")
        updated = AlertRule.model_validate(
            {
                **existing.model_dump(mode="python"),
                **updates,
                "updated_at": NOW,
                "version": expected_version + 1,
            }
        )
        self.rules[rule_id] = updated
        self.audits.append(audit)
        return updated

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
        replay = self.idempotency.get(idempotency_key)
        if replay is not None:
            stored_hash, stored_result = replay
            if stored_hash != request_hash:
                raise ConflictError("idempotency conflict")
            return stored_result
        existing = self.rules.get(rule_id)
        if existing is None or existing.tenant_id != tenant_id:
            raise NotFoundError("not found")
        if existing.version != expected_version:
            raise ConflictError("version conflict")
        replaced = AlertRule.model_validate(
            {
                **existing.model_dump(mode="python"),
                "enabled": False,
                "status": "replaced",
                "replaced_by_rule_id": replacement.rule_id,
                "updated_at": NOW,
                "version": expected_version + 1,
            }
        )
        result = AlertRuleReplacement(replaced=replaced, replacement=replacement)
        self.rules[rule_id] = replaced
        self.rules[replacement.rule_id] = replacement
        self.idempotency[idempotency_key] = (request_hash, result)
        self.audits.append(audit)
        return result


def make_rule() -> AlertRule:
    return AlertRule(
        tenant_id="tnt_1",
        rule_id="rule_1",
        pond_id="pond_1",
        device_id="dev_1",
        metric=AlertMetric.DO_MG_L,
        name="Low oxygen",
        operator=AlertOperator.LESS_THAN,
        threshold=5.0,
        aggregation=AlertAggregation.MIN,
        window="5m",
        duration="3m",
        severity=AlertSeverity.CRITICAL,
        channels=(AlertChannel.EMAIL, AlertChannel.TELEGRAM),
        cooldown_seconds=1_800,
        enabled=True,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def create_payload(**updates: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pond_id": "pond_1",
        "device_id": "dev_1",
        "metric": "do_mg_l",
        "name": "Low oxygen",
        "operator": "<",
        "threshold": 5.0,
        "aggregation": "min",
        "window": "5m",
        "duration": "3m",
        "severity": "critical",
        "channels": ["email", "telegram"],
        "cooldown_seconds": 1_800,
        "enabled": True,
    }
    payload.update(updates)
    return payload


def app_for_role(
    role: TenantRole,
) -> tuple[Any, FakeAlertRuleRepository, FakeDomainRepository]:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    repository = FakeAlertRuleRepository()
    domain_repository = FakeDomainRepository()
    app.state.domain_repository = domain_repository
    app.state.alert_rule_repository = repository
    app.state.membership_service = FakeMembershipService(role)
    return app, repository, domain_repository


def dev_headers(**updates: str) -> dict[str, str]:
    headers = {"X-Dev-User-Sub": "sub_1", "User-Agent": "limnopulse-tests"}
    headers.update(updates)
    return headers


@pytest.mark.parametrize("role", list(TenantRole))
def test_all_tenant_roles_can_list_alert_rules(role: TenantRole) -> None:
    app, _, _ = app_for_role(role)

    response = TestClient(app).get(
        "/v1/tenants/tnt_1/alert-rules",
        headers=dev_headers(),
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["rule_id"] == "rule_1"


@pytest.mark.parametrize("role", [TenantRole.MEMBER, TenantRole.VIEWER])
def test_read_only_roles_cannot_mutate_alert_rules(role: TenantRole) -> None:
    app, _, _ = app_for_role(role)
    client = TestClient(app)
    path = "/v1/tenants/tnt_1/alert-rules"

    assert client.post(path, json=create_payload(), headers=dev_headers()).status_code == 403
    assert (
        client.patch(
            f"{path}/rule_1",
            json={"expected_version": 1, "threshold": 4.5},
            headers=dev_headers(),
        ).status_code
        == 403
    )
    assert (
        client.post(
            f"{path}/rule_1/replace",
            json={"expected_version": 1, **create_payload(metric="ph")},
            headers=dev_headers(**{"Idempotency-Key": "replace-123"}),
        ).status_code
        == 403
    )


@pytest.mark.parametrize("role", [TenantRole.OWNER, TenantRole.ADMIN])
def test_write_roles_can_create_alert_rule_with_server_id(role: TenantRole) -> None:
    app, repository, _ = app_for_role(role)

    response = TestClient(app).post(
        "/v1/tenants/tnt_1/alert-rules",
        json=create_payload(),
        headers=dev_headers(),
    )

    assert response.status_code == 201
    body = response.json()
    assert body["rule_id"].startswith("rule_")
    assert body["version"] == 1
    assert body["status"] == "active"
    assert body["channels"] == ["email", "telegram"]
    assert body["rule_id"] in repository.rules


@pytest.mark.parametrize(
    ("updates", "expected_status"),
    [
        ({"unexpected": "value"}, 422),
        ({"window": "59s"}, 422),
        ({"duration": "25h"}, 422),
        ({"cooldown_seconds": 59}, 422),
        ({"channels": []}, 422),
        ({"channels": ["email", "email"]}, 422),
        ({"metric": "unknown"}, 422),
    ],
)
def test_create_rejects_invalid_alert_rule_payloads(
    updates: dict[str, Any],
    expected_status: int,
) -> None:
    app, _, _ = app_for_role(TenantRole.ADMIN)

    response = TestClient(app).post(
        "/v1/tenants/tnt_1/alert-rules",
        json=create_payload(**updates),
        headers=dev_headers(),
    )

    assert response.status_code == expected_status


@pytest.mark.parametrize(
    "updates",
    [
        {"pond_id": "pond_missing"},
        {"device_id": "dev_missing"},
        {"device_id": "dev_other_pond"},
    ],
)
def test_create_hides_missing_or_mismatched_targets(updates: dict[str, Any]) -> None:
    app, _, _ = app_for_role(TenantRole.ADMIN)

    response = TestClient(app).post(
        "/v1/tenants/tnt_1/alert-rules",
        json=create_payload(**updates),
        headers=dev_headers(),
    )

    assert response.status_code == 404


def test_patch_updates_mutable_fields_and_increments_version() -> None:
    app, _, _ = app_for_role(TenantRole.ADMIN)

    response = TestClient(app).patch(
        "/v1/tenants/tnt_1/alert-rules/rule_1",
        json={"expected_version": 1, "threshold": 4.5, "enabled": False},
        headers=dev_headers(),
    )

    assert response.status_code == 200
    assert response.json()["threshold"] == 4.5
    assert response.json()["enabled"] is False
    assert response.json()["version"] == 2


@pytest.mark.parametrize(
    "payload",
    [
        {"expected_version": 1},
        {"expected_version": 1, "metric": "ph"},
        {"expected_version": 1, "pond_id": "pond_1"},
        {"expected_version": 1, "threshold": None},
    ],
)
def test_patch_rejects_empty_immutable_or_null_changes(payload: dict[str, Any]) -> None:
    app, _, _ = app_for_role(TenantRole.ADMIN)

    response = TestClient(app).patch(
        "/v1/tenants/tnt_1/alert-rules/rule_1",
        json=payload,
        headers=dev_headers(),
    )

    assert response.status_code == 422


def test_replace_requires_valid_idempotency_key() -> None:
    app, _, _ = app_for_role(TenantRole.ADMIN)
    client = TestClient(app)
    path = "/v1/tenants/tnt_1/alert-rules/rule_1/replace"
    payload = {"expected_version": 1, **create_payload(metric="ph")}

    assert client.post(path, json=payload, headers=dev_headers()).status_code == 422
    assert (
        client.post(
            path,
            json=payload,
            headers=dev_headers(**{"Idempotency-Key": "short"}),
        ).status_code
        == 422
    )


def test_replace_returns_old_and_new_rule_and_replays() -> None:
    app, repository, _ = app_for_role(TenantRole.ADMIN)
    client = TestClient(app)
    path = "/v1/tenants/tnt_1/alert-rules/rule_1/replace"
    payload = {"expected_version": 1, **create_payload(metric="ph", threshold=6.5)}
    headers = dev_headers(**{"Idempotency-Key": "replace-123"})

    first = client.post(path, json=payload, headers=headers)
    replay = client.post(path, json=payload, headers=headers)

    assert first.status_code == 201
    assert replay.status_code == 201
    assert replay.json() == first.json()
    assert first.json()["replaced"]["status"] == "replaced"
    assert first.json()["replaced"]["enabled"] is False
    assert first.json()["replacement"]["metric"] == "ph"
    assert first.json()["replacement"]["replaces_rule_id"] == "rule_1"
    assert len(repository.idempotency) == 1


def test_replace_rejects_same_idempotency_key_with_other_payload() -> None:
    app, _, _ = app_for_role(TenantRole.ADMIN)
    client = TestClient(app)
    path = "/v1/tenants/tnt_1/alert-rules/rule_1/replace"
    headers = dev_headers(**{"Idempotency-Key": "replace-123"})

    first = client.post(
        path,
        json={"expected_version": 1, **create_payload(metric="ph")},
        headers=headers,
    )
    conflict = client.post(
        path,
        json={"expected_version": 1, **create_payload(metric="temp_c")},
        headers=headers,
    )

    assert first.status_code == 201
    assert conflict.status_code == 409


def test_mutation_passes_redacted_request_context_to_repository() -> None:
    app, repository, _ = app_for_role(TenantRole.OWNER)

    response = TestClient(app).post(
        "/v1/tenants/tnt_1/alert-rules",
        json=create_payload(),
        headers=dev_headers(),
    )

    assert response.status_code == 201
    assert repository.audits[-1].actor_id == "sub_1"
    assert repository.audits[-1].ip is not None
    assert repository.audits[-1].user_agent == "limnopulse-tests"
