import sys
from pathlib import Path

import fakeredis.aioredis
from botocore.exceptions import EndpointConnectionError
from fastapi.testclient import TestClient
from test_tenants import FakeMembershipService, make_membership

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app

sys.path.insert(0, str(Path(__file__).parents[1] / "unit"))
from test_domain_repository import RecordingDynamoClient

LEAD = {
    "name": " Maria Silva ",
    "email": "maria@example.com",
    "consent": True,
    "source": "/produto",
}


def lead_app():
    app = create_app(Settings(app_env="test"))
    app.state.domain_repository = object()
    app.state.redis_client = fakeredis.aioredis.FakeRedis()
    from limnopulse_api.adapters.leads import DynamoLeadRepository

    db = RecordingDynamoClient()
    app.state.lead_repository = DynamoLeadRepository("Domain", db)
    return app, db


def test_lead_is_public_persisted_and_rate_limited():
    app, db = lead_app()
    with TestClient(app) as client:
        for _ in range(5):
            response = client.post("/v1/leads", json=LEAD)
            assert response.status_code == 201
        response = client.post("/v1/leads", json=LEAD)
        assert response.status_code == 429
        assert int(response.headers["retry-after"]) > 0
        assert client.get("/v1/leads").status_code == 405
    assert len(db.items) == 5
    item = next(iter(db.items.values()))
    assert item["PK"].startswith("LEADS#")
    assert item["name"] == "Maria Silva"
    assert item["consent"] is True
    assert item["created_at"]
    assert "ip" not in item


def test_lead_rejects_invalid_and_sensitive_fields():
    app, db = lead_app()
    with TestClient(app) as client:
        for extra in [
            {"name": "  "},
            {"email": "bad"},
            {"consent": False},
            {"consent": "true"},
            {"card": "1234"},
            {"source": "x" * 201},
        ]:
            assert client.post("/v1/leads", json=LEAD | extra).status_code == 422
    assert not db.items


def test_lead_does_not_report_success_on_write_failure():
    app, db = lead_app()

    def fail(**kwargs):
        raise EndpointConnectionError(endpoint_url="http://db")

    db.put_item = fail
    with TestClient(app) as client:
        assert client.post("/v1/leads", json=LEAD).status_code == 503


def test_city_create_patch_clear_and_old_records():
    app = create_app(Settings(app_env="test"))
    db = RecordingDynamoClient()
    app.state.domain_repository = DynamoDomainRepository("Domain", db)
    app.state.membership_service = FakeMembershipService(make_membership(TenantRole.OWNER))
    headers = {"X-Dev-User-Sub": "sub_1"}
    with TestClient(app) as client:
        created = client.post(
            "/v1/tenants", json={"name": "Farm", "city": "Panorama - SP"}, headers=headers
        )
        assert created.status_code == 201
        assert created.json()["city"] == "Panorama - SP"
        tenant_id = created.json()["tenant_id"]
        membership = make_membership(TenantRole.OWNER).model_copy(update={"tenant_id": tenant_id})
        app.state.membership_service = FakeMembershipService(membership)
        db.items[("TENANT#" + tenant_id, "META")]["settings"]["timezone"] = "UTC"
        db.items[("TENANT#" + tenant_id, "META")]["settings"]["other"] = None
        changed = client.patch(
            "/v1/tenants/" + tenant_id,
            json={"name": "Farm 2", "expected_version": 1},
            headers=headers,
        )
        assert changed.json()["city"] == "Panorama - SP"
        cleared = client.patch(
            "/v1/tenants/" + tenant_id, json={"city": None, "expected_version": 2}, headers=headers
        )
        assert cleared.json()["city"] is None
        assert db.items[("TENANT#" + tenant_id, "META")]["settings"] == {
            "timezone": "UTC",
            "other": None,
        }
        old = client.post("/v1/tenants", json={"name": "Old"}, headers=headers)
        assert old.json()["city"] is None


def test_lead_rate_is_configurable_and_untrusted_forwarded_ip_cannot_bypass():
    app, db = lead_app()
    app.state.settings = Settings(app_env="test", lead_rate_limit_per_minute=2)
    with TestClient(app) as client:
        assert (
            client.post("/v1/leads", json=LEAD, headers={"X-Forwarded-For": "1.1.1.1"}).status_code
            == 201
        )
        assert (
            client.post("/v1/leads", json=LEAD, headers={"X-Forwarded-For": "2.2.2.2"}).status_code
            == 201
        )
        assert (
            client.post("/v1/leads", json=LEAD, headers={"X-Forwarded-For": "3.3.3.3"}).status_code
            == 429
        )
    assert len(db.items) == 2


def test_lead_redis_failure_returns_unavailable_without_persisting():
    from redis.exceptions import ConnectionError

    class BrokenRedis:
        def pipeline(self, **kwargs):
            raise ConnectionError("redis unavailable")

    app, db = lead_app()
    app.state.redis_client = BrokenRedis()
    with TestClient(app) as client:
        assert client.post("/v1/leads", json=LEAD).status_code == 503
    assert not db.items
