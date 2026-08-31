from limnopulse_api.adapters.dynamodb import DynamoKeyBuilder
from limnopulse_api.api.dependencies import require_tenant_access
from limnopulse_api.api.openapi_contract import iter_effective_api_routes
from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app


def dependency_calls(dependant):
    yield dependant.call
    for dependency in dependant.dependencies:
        yield from dependency_calls(dependency)


def test_every_mounted_tenant_route_requires_membership() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    routes = [
        route
        for route in iter_effective_api_routes(app)
        if "{tenant_id}" in route.path_format
    ]
    assert len(routes) == 25
    for route in routes:
        assert require_tenant_access in set(dependency_calls(route.dependant))


def test_legacy_tenant_keys_cannot_cross_partitions() -> None:
    keys = DynamoKeyBuilder()
    assert keys.pond("tnt_a", "pond_1")["PK"] == "TENANT#tnt_a"
    assert keys.device("tnt_a", "dev_1") != keys.device("tnt_b", "dev_1")
    assert keys.membership("sub_1", "tnt_a")["SK"] == "TENANT#tnt_a"
