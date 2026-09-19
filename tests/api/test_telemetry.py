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
        self.latest_tenant_calls: list[dict[str, str]] = []

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

    async def query_latest_metrics_for_tenant(self, *, tenant_id: str) -> list[LatestMetrics]:
        self.latest_tenant_calls.append({"tenant_id": tenant_id})
        return [
            LatestMetrics(
                measured_at=datetime(2026, 1, 1, 12, 5, tzinfo=UTC),
                tenant_id=tenant_id,
                pond_id="pond_1",
                temp_c=25.1,
                ph=7.2,
            ),
            LatestMetrics(
                measured_at=datetime(2026, 1, 1, 12, 4, tzinfo=UTC),
                tenant_id=tenant_id,
                pond_id="pond_2",
                temp_c=24.9,
                ph=7.1,
            ),
        ]


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
def test_telemetry_without_membership_returns_403_without_pond_or_telemetry_calls(
    path: str,
) -> None:
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


def test_latest_metrics_for_tenant_uses_one_batched_query() -> None:
    telemetry_repository = FakeTelemetryRepository()
    app = make_app(
        membership=make_membership(TenantRole.OWNER),
        pond=make_pond(),
        telemetry_repository=telemetry_repository,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/tenants/tnt_1/metrics/latest",
            headers={"X-Dev-User-Sub": "sub_1"},
        )

    assert response.status_code == 200
    assert [item["pond_id"] for item in response.json()["items"]] == ["pond_1", "pond_2"]
    assert telemetry_repository.latest_tenant_calls == [{"tenant_id": "tnt_1"}]


@pytest.mark.parametrize(
    "membership,pond,expected", [(None, make_pond(), 403), (make_membership(), None, 404)]
)
def test_summary_requires_membership_and_pond(membership, pond, expected):
    with TestClient(make_app(membership=membership, pond=pond)) as client:
        response = client.get(
            "/v1/tenants/tnt_1/ponds/pond_1/metrics/summary", headers={"X-Dev-User-Sub": "sub_1"}
        )
    assert response.status_code == expected


def test_summary_returns_statistics_and_validates_period():
    class SummaryRepository(FakeTelemetryRepository):
        async def query_summary(self, *, tenant_id, pond_id, period):
            from limnopulse_api.domain.telemetry import MetricsSummary, MetricStatistics

            return MetricsSummary(
                tenant_id=tenant_id,
                pond_id=pond_id,
                period=period,
                interval="5m",
                statistics={"do_mg_l": MetricStatistics(mean=6, min=4, max=8, count=10000)},
            )

    app = make_app(
        membership=make_membership(), pond=make_pond(), telemetry_repository=SummaryRepository()
    )
    with TestClient(app) as client:
        path = "/v1/tenants/tnt_1/ponds/pond_1/metrics/summary"
        response = client.get(path, headers={"X-Dev-User-Sub": "sub_1"})
        assert response.status_code == 200
        assert response.json()["statistics"]["do_mg_l"]["count"] == 10000
        assert (
            client.get(path + "?period=365d", headers={"X-Dev-User-Sub": "sub_1"}).status_code
            == 422
        )
