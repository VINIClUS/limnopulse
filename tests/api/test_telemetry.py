from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings
from limnopulse_api.domain.entities import Membership, Pond
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telemetry import LatestMetrics, TelemetryReading
from limnopulse_api.main import create_app


class FakeMembershipService:
    def __init__(self, membership: Membership | None) -> None:
        self.membership = membership

    async def get_active_membership(
        self,
        cognito_sub: str,
        tenant_id: str,
    ) -> Membership | None:
        if self.membership is None:
            return None
        if self.membership.cognito_sub != cognito_sub or self.membership.tenant_id != tenant_id:
            return None
        return self.membership


class FakeDomainRepository:
    def __init__(self, pond: Pond | None) -> None:
        self.pond = pond
        self.get_pond_calls: list[tuple[str, str]] = []

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        self.get_pond_calls.append((tenant_id, pond_id))
        if self.pond is None:
            return None
        if self.pond.tenant_id != tenant_id or self.pond.pond_id != pond_id:
            return None
        return self.pond


class FakeTelemetryRepository:
    def __init__(self) -> None:
        self.reading_calls: list[dict[str, object]] = []
        self.latest_calls: list[dict[str, str]] = []

    async def query_readings(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: str,
        stop: str | None,
        limit: int,
    ) -> list[TelemetryReading]:
        self.reading_calls.append(
            {
                "tenant_id": tenant_id,
                "pond_id": pond_id,
                "start": start,
                "stop": stop,
                "limit": limit,
            }
        )
        return [
            TelemetryReading(
                measured_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
                tenant_id=tenant_id,
                pond_id=pond_id,
                device_id="dev_1",
                temp_c=25.1,
                ph=7.2,
            )
        ]

    async def query_latest_metrics(self, *, tenant_id: str, pond_id: str) -> LatestMetrics:
        self.latest_calls.append({"tenant_id": tenant_id, "pond_id": pond_id})
        return LatestMetrics(
            measured_at=datetime(2026, 1, 1, 12, 5, tzinfo=UTC),
            tenant_id=tenant_id,
            pond_id=pond_id,
            temp_c=25.1,
            ph=7.2,
        )


def make_membership(role: TenantRole = TenantRole.VIEWER) -> Membership:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Membership(
        tenant_id="tnt_1",
        cognito_sub="sub_1",
        role=role,
        created_at=now,
        updated_at=now,
        version=1,
    )


def make_pond() -> Pond:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Pond(
        tenant_id="tnt_1",
        pond_id="pond_1",
        name="North",
        created_at=now,
        updated_at=now,
        version=1,
    )


def make_app(
    *,
    membership: Membership | None = None,
    pond: Pond | None = None,
    telemetry_repository: FakeTelemetryRepository | None = None,
):
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = FakeDomainRepository(pond=pond)
    app.state.membership_service = FakeMembershipService(membership=membership)
    app.state.telemetry_repository = telemetry_repository or FakeTelemetryRepository()
    return app


@pytest.mark.parametrize(
    "path",
    [
        "/v1/tenants/tnt_1/ponds/pond_1/readings",
        "/v1/tenants/tnt_1/ponds/pond_1/metrics/latest",
    ],
)
def test_telemetry_without_membership_returns_403_without_pond_or_telemetry_calls(path: str) -> None:
    app = make_app(membership=None, pond=make_pond())
    domain_repository = app.state.domain_repository
    telemetry_repository = app.state.telemetry_repository

    with TestClient(app) as client:
        response = client.get(
            path,
            headers={"X-Dev-User-Sub": "sub_1"},
        )

    assert response.status_code == 403
    assert domain_repository.get_pond_calls == []
    assert telemetry_repository.reading_calls == []
    assert telemetry_repository.latest_calls == []


@pytest.mark.parametrize(
    "path",
    [
        "/v1/tenants/tnt_1/ponds/pond_missing/readings",
        "/v1/tenants/tnt_1/ponds/pond_missing/metrics/latest",
    ],
)
def test_telemetry_missing_pond_returns_404_without_querying_telemetry(path: str) -> None:
    telemetry_repository = FakeTelemetryRepository()
    app = make_app(
        membership=make_membership(),
        pond=None,
        telemetry_repository=telemetry_repository,
    )
    domain_repository = app.state.domain_repository

    with TestClient(app) as client:
        response = client.get(
            path,
            headers={"X-Dev-User-Sub": "sub_1"},
        )

    assert response.status_code == 404
    assert domain_repository.get_pond_calls == [("tnt_1", "pond_missing")]
    assert telemetry_repository.reading_calls == []
    assert telemetry_repository.latest_calls == []


def test_readings_delegates_with_server_side_tenant_and_pond_filters() -> None:
    telemetry_repository = FakeTelemetryRepository()
    app = make_app(
        membership=make_membership(TenantRole.MEMBER),
        pond=make_pond(),
        telemetry_repository=telemetry_repository,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/tenants/tnt_1/ponds/pond_1/readings",
            params={"start": "-30m", "stop": "2026-01-01T13:00:00Z", "limit": 100},
            headers={"X-Dev-User-Sub": "sub_1"},
        )

    assert response.status_code == 200
    assert response.json()["items"][0]["tenant_id"] == "tnt_1"
    assert response.json()["items"][0]["pond_id"] == "pond_1"
    assert telemetry_repository.reading_calls == [
        {
            "tenant_id": "tnt_1",
            "pond_id": "pond_1",
            "start": "-30m",
            "stop": "2026-01-01T13:00:00Z",
            "limit": 100,
        }
    ]


def test_readings_rejects_invalid_start_before_querying_telemetry() -> None:
    telemetry_repository = FakeTelemetryRepository()
    app = make_app(
        membership=make_membership(TenantRole.MEMBER),
        pond=make_pond(),
        telemetry_repository=telemetry_repository,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/tenants/tnt_1/ponds/pond_1/readings",
            params={"start": "not-a-time"},
            headers={"X-Dev-User-Sub": "sub_1"},
        )

    assert response.status_code == 422
    assert telemetry_repository.reading_calls == []


def test_latest_metrics_delegates_with_server_side_tenant_and_pond_filters() -> None:
    telemetry_repository = FakeTelemetryRepository()
    app = make_app(
        membership=make_membership(TenantRole.OWNER),
        pond=make_pond(),
        telemetry_repository=telemetry_repository,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/tenants/tnt_1/ponds/pond_1/metrics/latest",
            headers={"X-Dev-User-Sub": "sub_1"},
        )

    assert response.status_code == 200
    assert response.json()["tenant_id"] == "tnt_1"
    assert response.json()["pond_id"] == "pond_1"
    assert telemetry_repository.latest_calls == [{"tenant_id": "tnt_1", "pond_id": "pond_1"}]
