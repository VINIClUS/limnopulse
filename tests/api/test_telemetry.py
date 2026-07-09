from datetime import UTC, datetime

from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings
from limnopulse_api.domain.entities import Membership, Pond
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telemetry import TelemetryReading
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

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        if self.pond is None:
            return None
        if self.pond.tenant_id != tenant_id or self.pond.pond_id != pond_id:
            return None
        return self.pond


class FakeTelemetryRepository:
    def __init__(self) -> None:
        self.readings: list[TelemetryReading] = []
        self.latest: TelemetryReading | None = None
        self.list_calls: list[dict[str, object]] = []
        self.latest_calls: list[dict[str, object]] = []

    async def list_readings(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        limit: int,
        fields: tuple[str, ...],
    ) -> list[TelemetryReading]:
        self.list_calls.append(
            {
                "tenant_id": tenant_id,
                "pond_id": pond_id,
                "start": start,
                "stop": stop,
                "limit": limit,
                "fields": fields,
            }
        )
        return self.readings

    async def latest_metrics(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        fields: tuple[str, ...],
    ) -> TelemetryReading | None:
        self.latest_calls.append(
            {
                "tenant_id": tenant_id,
                "pond_id": pond_id,
                "start": start,
                "stop": stop,
                "fields": fields,
            }
        )
        return self.latest


def make_pond() -> Pond:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Pond(
        tenant_id="tnt_1",
        pond_id="pond_1",
        name="West",
        created_at=now,
        updated_at=now,
        version=1,
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


_DEFAULT = object()


def app_with_dependencies(
    *,
    membership: Membership | None | object = _DEFAULT,
    pond: Pond | None | object = _DEFAULT,
) -> tuple[TestClient, FakeTelemetryRepository]:
    telemetry_repository = FakeTelemetryRepository()
    resolved_membership = make_membership() if membership is _DEFAULT else membership
    resolved_pond = make_pond() if pond is _DEFAULT else pond

    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = FakeDomainRepository(pond=resolved_pond)
    app.state.membership_service = FakeMembershipService(membership=resolved_membership)
    app.state.telemetry_repository = telemetry_repository
    return TestClient(app), telemetry_repository


def test_viewer_can_query_authorized_pond_readings() -> None:
    client, telemetry_repository = app_with_dependencies()
    telemetry_repository.readings = [
        TelemetryReading(
            timestamp=datetime(2026, 7, 8, 14, 35, tzinfo=UTC),
            tenant_id="tnt_1",
            pond_id="pond_1",
            device_id="dev_1",
            metrics={"temp_c": 25.5, "ph": 7.1},
        )
    ]

    response = client.get(
        "/v1/tenants/tnt_1/ponds/pond_1/readings",
        params={
            "start": "2026-07-08T14:00:00Z",
            "stop": "2026-07-08T15:00:00Z",
            "fields": ["temp_c", "ph"],
            "limit": 25,
        },
        headers={"X-Dev-User-Sub": "sub_1"},
    )

    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "ts": "2026-07-08T14:35:00Z",
            "tenant_id": "tnt_1",
            "pond_id": "pond_1",
            "device_id": "dev_1",
            "metrics": {"temp_c": 25.5, "ph": 7.1},
        }
    ]
    assert telemetry_repository.list_calls == [
        {
            "tenant_id": "tnt_1",
            "pond_id": "pond_1",
            "start": datetime(2026, 7, 8, 14, 0, tzinfo=UTC),
            "stop": datetime(2026, 7, 8, 15, 0, tzinfo=UTC),
            "limit": 25,
            "fields": ("temp_c", "ph"),
        }
    ]


def test_valid_identity_without_membership_cannot_query_readings() -> None:
    client, telemetry_repository = app_with_dependencies(membership=None)

    response = client.get(
        "/v1/tenants/tnt_1/ponds/pond_1/readings",
        params={"start": "2026-07-08T14:00:00Z"},
        headers={"X-Dev-User-Sub": "sub_1"},
    )

    assert response.status_code == 403
    assert telemetry_repository.list_calls == []


def test_readings_return_404_when_pond_is_not_in_tenant() -> None:
    client, telemetry_repository = app_with_dependencies(pond=None)

    response = client.get(
        "/v1/tenants/tnt_1/ponds/pond_1/readings",
        params={"start": "2026-07-08T14:00:00Z"},
        headers={"X-Dev-User-Sub": "sub_1"},
    )

    assert response.status_code == 404
    assert telemetry_repository.list_calls == []


def test_readings_reject_unsupported_fields() -> None:
    client, telemetry_repository = app_with_dependencies()

    response = client.get(
        "/v1/tenants/tnt_1/ponds/pond_1/readings",
        params={"start": "2026-07-08T14:00:00Z", "fields": ["email"]},
        headers={"X-Dev-User-Sub": "sub_1"},
    )

    assert response.status_code == 422
    assert telemetry_repository.list_calls == []


def test_latest_metrics_uses_lookback_window_and_returns_empty_payload_when_no_data() -> None:
    client, telemetry_repository = app_with_dependencies()

    response = client.get(
        "/v1/tenants/tnt_1/ponds/pond_1/metrics/latest",
        params={"stop": "2026-07-08T15:00:00Z", "lookback_seconds": 600},
        headers={"X-Dev-User-Sub": "sub_1"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "tenant_id": "tnt_1",
        "pond_id": "pond_1",
        "ts": None,
        "device_id": None,
        "metrics": {},
    }
    assert telemetry_repository.latest_calls == [
        {
            "tenant_id": "tnt_1",
            "pond_id": "pond_1",
            "start": datetime(2026, 7, 8, 14, 50, tzinfo=UTC),
            "stop": datetime(2026, 7, 8, 15, 0, tzinfo=UTC),
            "fields": (
                "temp_c",
                "ph",
                "do_mg_l",
                "turbidity_ntu",
                "salinity_ppt",
                "battery_v",
                "rssi",
                "seq",
            ),
        }
    ]
