import re
from datetime import UTC, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

import pytest
from pydantic import ValidationError

from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
    ensure_asset_transition,
    ensure_site_transition,
)
from limnopulse_api.domain.deployments import (
    Deployment,
    DeploymentStatus,
    ensure_deployment_transition,
)
from limnopulse_api.domain.devices_v2 import (
    ActuatorProfile,
    CapabilityAvailability,
    CapabilityDeclaration,
    CapabilityOperation,
    CapabilityProvenance,
    CapabilitySupport,
    CommandRiskClass,
    CompatibilityLevel,
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ProbeProfile,
    ensure_component_transition,
    ensure_device_transition,
)
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
    is_canonical_id,
    new_actuator_profile_id,
    new_asset_id,
    new_capability_id,
    new_component_id,
    new_deployment_id,
    new_device_id,
    new_device_integration_id,
    new_integration_account_id,
    new_probe_profile_id,
    new_site_id,
    require_canonical_id,
    require_device_id,
    require_safe_id,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
    ensure_device_integration_transition,
    ensure_integration_account_transition,
)
from limnopulse_api.domain.v2_common import (
    IDEMPOTENCY_RECEIPT_TTL,
    IdempotencyReceipt,
    QuotaDimension,
    V2VersionedEntity,
    format_utc_timestamp,
    new_idempotency_receipt,
)

NOW = datetime(2026, 8, 25, 12, 0, 0, 123456, tzinfo=UTC)
LATER = NOW + timedelta(minutes=5)
HEX64 = "a" * 64
DEVICE_ID = "dev_" + "1" * 32


def stamps(**overrides):
    return {"created_at": NOW, "updated_at": NOW, "version": 1, **overrides}


def probe_component(**overrides) -> DeviceComponent:
    values = {
        "tenant_id": "tnt_a",
        "component_id": "cmp_" + "2" * 32,
        "device_id": DEVICE_ID,
        "kind": ComponentKind.PROBE,
        "name": "Probe",
        "probe_profile": ProbeProfile(probe_profile_id="prb_" + "3" * 32),
        **stamps(),
    }
    values.update(overrides)
    return DeviceComponent(**values)


def capability(**overrides) -> CapabilityDeclaration:
    values = {
        "tenant_id": "tnt_a",
        "component_id": "cmp_1",
        "capability_id": "cap_1",
        "capability_key": "water.temperature",
        "operation": CapabilityOperation.READ,
        "support": CapabilitySupport.SUPPORTED,
        "availability": CapabilityAvailability.AVAILABLE,
        "provenance": CapabilityProvenance.MODEL_PROFILE,
        **stamps(),
    }
    values.update(overrides)
    return CapabilityDeclaration(**values)


def deployment(**overrides) -> Deployment:
    values = {
        "tenant_id": "tnt_a",
        "deployment_id": "dep_1",
        "component_id": "cmp_1",
        "site_id": "site_1",
        "asset_id": "ast_1",
        "started_at": NOW,
        **stamps(),
    }
    values.update(overrides)
    return Deployment(**values)


# --- identifiers -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("factory", "prefix"),
    [
        (new_site_id, "site"),
        (new_asset_id, "ast"),
        (new_device_id, "dev"),
        (new_component_id, "cmp"),
        (new_probe_profile_id, "prb"),
        (new_actuator_profile_id, "act"),
        (new_integration_account_id, "ia"),
        (new_device_integration_id, "di"),
        (new_deployment_id, "dep"),
        (new_capability_id, "cap"),
    ],
)
def test_generated_ids_have_frozen_prefix_and_32_lower_hex(factory, prefix) -> None:
    value = factory()

    assert re.fullmatch(rf"{prefix}_[0-9a-f]{{32}}", value)
    assert is_canonical_id(value, prefix)
    assert require_canonical_id(value, prefix) == value
    assert factory() != value


def test_new_component_id_has_canonical_shape() -> None:
    value = new_component_id()
    assert value.startswith("cmp_")
    assert len(value) == 36
    int(value[4:], 16)


@pytest.mark.parametrize(
    "value",
    [
        "site_" + "A" * 32,
        "site_" + "1" * 31,
        "site_" + "1" * 33,
        "ast_" + "1" * 32,
        "site-" + "1" * 32,
        " site_" + "1" * 32,
        "site_" + "1" * 32 + "\n",
        "",
    ],
)
def test_canonical_id_validation_is_strict(value: str) -> None:
    assert not is_canonical_id(value, "site")
    with pytest.raises(ValueError):
        require_canonical_id(value, "site")


@pytest.mark.parametrize("prefix", ["site_", "Site", "", "si-te", "site#", "s1"])
def test_canonical_id_rejects_malformed_prefix(prefix: str) -> None:
    value = new_site_id()
    with pytest.raises(ValueError, match="prefix"):
        is_canonical_id(value, prefix)
    with pytest.raises(ValueError, match="prefix"):
        require_canonical_id(value, prefix)


def test_canonical_id_rejects_non_string_values() -> None:
    assert not is_canonical_id(None, "site")  # type: ignore[arg-type]


def _expected_legacy_id(prefix: str, discriminator: str) -> str:
    return f"{prefix}_{uuid5(NAMESPACE_URL, f'limnopulse/v4/{discriminator}').hex}"


def test_compatibility_ids_follow_frozen_uuid5_discriminators() -> None:
    assert default_site_id("tnt_a") == _expected_legacy_id("site", "default-site/tnt_a")
    assert asset_id_for_legacy_pond("tnt_a", "pond_1") == _expected_legacy_id(
        "ast", "legacy-pond/tnt_a/pond_1"
    )
    assert default_component_id_for_legacy_device(
        "tnt_a", "local-device-001"
    ) == _expected_legacy_id("cmp", "legacy-device-component/tnt_a/local-device-001")
    assert default_probe_profile_id_for_legacy_device(
        "tnt_a", "local-device-001"
    ) == _expected_legacy_id("prb", "legacy-device-probe/tnt_a/local-device-001")
    assert deployment_id_for_legacy_device("tnt_a", "local-device-001") == _expected_legacy_id(
        "dep", "legacy-device-deployment/tnt_a/local-device-001"
    )


def test_compatibility_id_vectors_are_pinned() -> None:
    tenant, pond, device = "tnt_local_001", "pond_local_001", "local-device-001"

    assert default_site_id(tenant) == "site_293ccec4d8405a9e980851d03af283db"
    assert asset_id_for_legacy_pond(tenant, pond) == "ast_751707aef3455ab2ba60733d238b594f"
    assert default_component_id_for_legacy_device(tenant, device) == (
        "cmp_32c73f5ac8dc596e9446956875639204"
    )
    assert default_probe_profile_id_for_legacy_device(tenant, device) == (
        "prb_7b5a5a2967795c3da2dc0820ea0bc51b"
    )
    assert deployment_id_for_legacy_device(tenant, device) == (
        "dep_84781af86ce4514b8a8397586b8ec53b"
    )


def test_compatibility_ids_are_stable_and_tenant_scoped() -> None:
    assert default_site_id("tnt_a") == default_site_id("tnt_a")
    assert default_site_id("tnt_a") != default_site_id("tnt_b")
    assert asset_id_for_legacy_pond("tnt_a", "p") != asset_id_for_legacy_pond("tnt_b", "p")
    for builder, prefix in (
        (default_component_id_for_legacy_device, "cmp"),
        (default_probe_profile_id_for_legacy_device, "prb"),
        (deployment_id_for_legacy_device, "dep"),
    ):
        first = builder("tnt_a", "local-device-001")
        assert first == builder("tnt_a", "local-device-001")
        assert first != builder("tnt_b", "local-device-001")
        assert first != builder("tnt_a", "local-device-002")
        assert is_canonical_id(first, prefix)
    assert is_canonical_id(default_site_id("tnt_a"), "site")
    assert is_canonical_id(asset_id_for_legacy_pond("tnt_a", "p"), "ast")


@pytest.mark.parametrize("value", ["tnt_a", "pond_local_001", "local-device-001", "a.b:c-d_e"])
def test_safe_ids_accept_historical_identifiers(value: str) -> None:
    assert require_safe_id(value) == value


@pytest.mark.parametrize("value", ["", "a#b", "-lead", "a b", "x" * 129, "ação"])
def test_safe_ids_reject_key_breaking_values(value: str) -> None:
    with pytest.raises(ValueError):
        require_safe_id(value)


@pytest.mark.parametrize("value", [DEVICE_ID, "local-device-001", "sensor.42"])
def test_device_ids_accept_canonical_and_historical_values(value: str) -> None:
    assert require_device_id(value) == value


@pytest.mark.parametrize("value", ["dev_1", "dev_" + "A" * 32, "dev#1", "", "dev_" + "1" * 33])
def test_device_ids_reject_non_canonical_dev_prefix_and_hash(value: str) -> None:
    with pytest.raises(ValueError):
        require_device_id(value)


# --- versioned entity window ------------------------------------------------------------------


def test_versioned_entity_is_frozen_with_literal_schema_version() -> None:
    site = Site(tenant_id="tnt_a", site_id="site_1", name="Farm", **stamps())

    assert site.schema_version == 2
    assert site.status is SiteStatus.ACTIVE
    with pytest.raises(ValidationError):
        site.name = "Other"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        Site(tenant_id="tnt_a", site_id="site_1", name="Farm", schema_version=1, **stamps())


def test_versioned_entity_rejects_naive_and_non_utc_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        Site(
            tenant_id="tnt_a",
            site_id="site_1",
            name="Farm",
            **stamps(created_at=datetime(2026, 8, 25)),
        )
    with pytest.raises(ValidationError, match="UTC"):
        Site(
            tenant_id="tnt_a",
            site_id="site_1",
            name="Farm",
            **stamps(created_at=NOW.astimezone(timezone(timedelta(hours=-3)))),
        )


def test_versioned_entity_requires_ordered_window_and_positive_version() -> None:
    with pytest.raises(ValidationError, match="created_at"):
        Site(
            tenant_id="tnt_a",
            site_id="site_1",
            name="Farm",
            **stamps(created_at=LATER, updated_at=NOW),
        )
    with pytest.raises(ValidationError):
        Site(tenant_id="tnt_a", site_id="site_1", name="Farm", **stamps(version=0))


def test_versioned_entity_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        Site(tenant_id="tnt_a", site_id="site_1", name="Farm", extra=1, **stamps())


@pytest.mark.parametrize("field", ["tenant_id", "site_id"])
def test_entity_ids_reject_hash_separator(field: str) -> None:
    values = {"tenant_id": "tnt_a", "site_id": "site_1", "name": "Farm", **stamps()}
    values[field] = "bad#id"
    with pytest.raises(ValidationError):
        Site(**values)


def test_v2_versioned_entity_base_is_shared() -> None:
    for model in (
        Site,
        Asset,
        PondProfile,
        DeviceV2,
        DeviceComponent,
        CapabilityDeclaration,
        IntegrationAccount,
        DeviceIntegration,
        Deployment,
    ):
        assert issubclass(model, V2VersionedEntity)
        assert model.model_config["extra"] == "forbid"
        assert model.model_config["frozen"] is True


# --- enums ----------------------------------------------------------------------------------


def test_frozen_enum_values() -> None:
    assert [item.value for item in SiteStatus] == ["active", "archived"]
    assert [item.value for item in AssetType] == ["pond"]
    assert [item.value for item in AssetStatus] == ["active", "archived"]
    assert [item.value for item in DeviceStatus] == [
        "provisioning",
        "active",
        "suspended",
        "decommissioned",
    ]
    assert [item.value for item in ComponentKind] == ["probe", "actuator", "hybrid"]
    assert [item.value for item in ComponentStatus] == ["active", "replaced", "decommissioned"]
    assert [item.value for item in CapabilityOperation] == [
        "read",
        "write",
        "command",
        "calibration",
    ]
    assert [item.value for item in CapabilitySupport] == ["supported", "unsupported", "unknown"]
    assert [item.value for item in CapabilityAvailability] == [
        "available",
        "unavailable",
        "unknown",
    ]
    assert [item.value for item in CapabilityProvenance] == [
        "model_profile",
        "integration",
        "discovered",
        "instance_override",
    ]
    assert [item.value for item in CommandRiskClass] == ["r0", "r1", "r2", "r3", "r4"]
    assert [item.value for item in IntegrationAccountStatus] == [
        "pending",
        "active",
        "error",
        "revoked",
    ]
    assert [item.value for item in DeviceIntegrationStatus] == [
        "provisioning",
        "active",
        "error",
        "decommissioned",
    ]
    assert [item.value for item in DeploymentStatus] == ["active", "ended"]
    assert [item.value for item in CompatibilityLevel] == ["certified", "compatible", "custom"]


def test_future_quota_dimensions_are_representable() -> None:
    assert [item.value for item in QuotaDimension] == [
        "sites",
        "assets",
        "devices",
        "components",
        "notification_destinations",
    ]
    assert QuotaDimension("notification_destinations") is QuotaDimension.NOTIFICATION_DESTINATIONS


# --- Site / Asset ----------------------------------------------------------------------------


def test_asset_with_pond_profile_round_trips() -> None:
    asset = Asset(
        tenant_id="tnt_a",
        asset_id="ast_1",
        site_id="site_1",
        asset_type=AssetType.POND,
        name="Pond",
        **stamps(),
    )
    profile = PondProfile(tenant_id="tnt_a", asset_id="ast_1", description="North", **stamps())
    bundle = AssetWithPondProfile(asset=asset, pond_profile=profile)

    assert AssetWithPondProfile.model_validate(bundle.model_dump(mode="json")) == bundle
    assert bundle.asset.status is AssetStatus.ACTIVE
    assert bundle.pond_profile.legacy_pond_id is None


def test_asset_with_pond_profile_requires_matching_asset() -> None:
    asset = Asset(
        tenant_id="tnt_a",
        asset_id="ast_1",
        site_id="site_1",
        asset_type=AssetType.POND,
        name="Pond",
        **stamps(),
    )
    with pytest.raises(ValidationError):
        AssetWithPondProfile(
            asset=asset,
            pond_profile=PondProfile(tenant_id="tnt_a", asset_id="ast_2", **stamps()),
        )
    with pytest.raises(ValidationError):
        AssetWithPondProfile(
            asset=asset,
            pond_profile=PondProfile(tenant_id="tnt_b", asset_id="ast_1", **stamps()),
        )


# --- Device / Component / Capability ---------------------------------------------------------


def test_device_v2_defaults() -> None:
    device = DeviceV2(tenant_id="tnt_a", device_id=DEVICE_ID, name="Gateway", **stamps())

    assert device.status is DeviceStatus.ACTIVE
    assert device.compatibility_level is CompatibilityLevel.CUSTOM
    assert device.model_profile_key is None


def test_device_v2_accepts_historical_device_id() -> None:
    device = DeviceV2(tenant_id="tnt_a", device_id="local-device-001", name="Gw", **stamps())
    assert device.device_id == "local-device-001"


def test_device_v2_rejects_non_canonical_dev_prefixed_id() -> None:
    with pytest.raises(ValidationError):
        DeviceV2(tenant_id="tnt_a", device_id="dev_1", name="Gw", **stamps())


def test_device_v2_rejects_legacy_pond_binding() -> None:
    with pytest.raises(ValidationError):
        DeviceV2(
            tenant_id="tnt_a",
            device_id="dev_" + "1" * 32,
            name="Gateway",
            pond_id="pond_1",
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            version=1,
        )


@pytest.mark.parametrize(
    "field",
    ["pond_id", "auth_type", "thing_name", "certificate_arn", "shadow_name", "redis_key"],
)
def test_device_v2_rejects_legacy_and_provider_identity_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        DeviceV2(
            tenant_id="tnt_a",
            device_id=DEVICE_ID,
            name="Gateway",
            **{field: "value"},
            **stamps(),
        )


def test_component_kind_requires_matching_profiles() -> None:
    probe = ProbeProfile(probe_profile_id="prb_1", measurement_metric_keys=("temperature",))
    actuator = ActuatorProfile(actuator_profile_id="act_1", actuator_type="aerator")

    assert probe_component().probe_profile is not None
    assert (
        probe_component(
            kind=ComponentKind.ACTUATOR, probe_profile=None, actuator_profile=actuator
        ).actuator_profile
        == actuator
    )
    assert (
        probe_component(
            kind=ComponentKind.HYBRID, probe_profile=probe, actuator_profile=actuator
        ).kind
        is ComponentKind.HYBRID
    )

    invalid = [
        {"kind": ComponentKind.PROBE, "probe_profile": None},
        {"kind": ComponentKind.PROBE, "probe_profile": probe, "actuator_profile": actuator},
        {"kind": ComponentKind.ACTUATOR, "probe_profile": None, "actuator_profile": None},
        {"kind": ComponentKind.ACTUATOR, "probe_profile": probe, "actuator_profile": actuator},
        {"kind": ComponentKind.HYBRID, "probe_profile": probe, "actuator_profile": None},
        {"kind": ComponentKind.HYBRID, "probe_profile": None, "actuator_profile": actuator},
    ]
    for overrides in invalid:
        with pytest.raises(ValidationError):
            probe_component(**overrides)


def test_profiles_reject_extra_fields() -> None:
    with pytest.raises(ValidationError):
        ProbeProfile(probe_profile_id="prb_1", serial="x")
    with pytest.raises(ValidationError):
        ActuatorProfile(actuator_profile_id="act_1", actuator_type="pump", relay=1)


def test_component_rejects_legacy_fields() -> None:
    with pytest.raises(ValidationError):
        probe_component(pond_id="pond_1")


def test_capability_includes_full_declaration() -> None:
    declaration = capability(
        operation=CapabilityOperation.COMMAND,
        risk_class=CommandRiskClass.R2,
        parameter_schema={"type": "object", "properties": {"speed": {"type": "number"}}},
        result_schema={"type": "object"},
        provider_implementation_key="vendor.cloud.aerator_v1",
        verified_at=NOW,
    )

    dumped = declaration.model_dump(mode="json")
    assert dumped["support"] == "supported"
    assert dumped["availability"] == "available"
    assert dumped["provenance"] == "model_profile"
    assert dumped["risk_class"] == "r2"
    assert dumped["parameter_schema"] == {
        "type": "object",
        "properties": {"speed": {"type": "number"}},
    }
    assert dumped["provider_implementation_key"] == "vendor.cloud.aerator_v1"
    assert CapabilityDeclaration.model_validate(dumped) == declaration


def test_capability_risk_is_required_for_command_and_forbidden_for_read() -> None:
    with pytest.raises(ValidationError, match="risk_class"):
        capability(operation=CapabilityOperation.COMMAND)
    with pytest.raises(ValidationError, match="risk_class"):
        capability(operation=CapabilityOperation.READ, risk_class=CommandRiskClass.R0)
    assert capability(operation=CapabilityOperation.WRITE).risk_class is None
    assert (
        capability(
            operation=CapabilityOperation.CALIBRATION, risk_class=CommandRiskClass.R1
        ).risk_class
        is CommandRiskClass.R1
    )


@pytest.mark.parametrize("key", ["temperature", "water.temperature", "do_mg_l", "ph.v2", "a1.b_2"])
def test_capability_key_accepts_frozen_shape(key: str) -> None:
    assert capability(capability_key=key).capability_key == key


@pytest.mark.parametrize(
    "key", ["", "Temperature", "1temp", "water..temp", "water.", "water-temp", "a._b", "a#b"]
)
def test_capability_key_rejects_invalid_shape(key: str) -> None:
    with pytest.raises(ValidationError):
        capability(capability_key=key)


def test_capability_schemas_are_deep_copied() -> None:
    schema = {"type": "object", "properties": {"speed": {"type": "number"}}}
    declaration = capability(parameter_schema=schema, result_schema=schema)

    schema["properties"]["speed"]["type"] = "string"
    assert declaration.parameter_schema == {
        "type": "object",
        "properties": {"speed": {"type": "number"}},
    }
    assert declaration.result_schema is not declaration.parameter_schema


def test_capability_schema_must_be_json_serializable() -> None:
    with pytest.raises(ValidationError):
        capability(parameter_schema={"bad": {1, 2}})


def test_capability_verified_at_must_be_utc() -> None:
    with pytest.raises(ValidationError):
        capability(verified_at=datetime(2026, 8, 25))


# --- Integrations ----------------------------------------------------------------------------


def test_integration_models_validate_provider_key_and_identity_hash() -> None:
    account = IntegrationAccount(
        tenant_id="tnt_a",
        integration_account_id="ia_1",
        provider_key="vendor.cloud",
        display_name="Vendor",
        **stamps(),
    )
    integration = DeviceIntegration(
        tenant_id="tnt_a",
        device_integration_id="di_1",
        device_id=DEVICE_ID,
        integration_account_id="ia_1",
        provider_key="native.https",
        external_identity_hash=HEX64,
        **stamps(),
    )

    assert account.status is IntegrationAccountStatus.PENDING
    assert integration.status is DeviceIntegrationStatus.PROVISIONING
    with pytest.raises(ValidationError):
        IntegrationAccount.model_validate({**account.model_dump(), "provider_key": "Vendor.Cloud"})
    with pytest.raises(ValidationError):
        DeviceIntegration.model_validate(
            {**integration.model_dump(), "external_identity_hash": "abc"}
        )
    with pytest.raises(ValidationError):
        DeviceIntegration.model_validate({**integration.model_dump(), "thing_name": "x"})


@pytest.mark.parametrize("key", ["vendor", "vendor.cloud", "native_https.v2", "a1_b.c_d"])
def test_provider_key_accepts_frozen_shape(key: str) -> None:
    account = IntegrationAccount(
        tenant_id="tnt_a",
        integration_account_id="ia_1",
        provider_key=key,
        display_name="Vendor",
        **stamps(),
    )
    assert account.provider_key == key


@pytest.mark.parametrize("key", ["", "Vendor", "1vendor", "vendor.", "vendor-cloud", "a#b"])
def test_provider_key_rejects_invalid_shape(key: str) -> None:
    with pytest.raises(ValidationError):
        IntegrationAccount(
            tenant_id="tnt_a",
            integration_account_id="ia_1",
            provider_key=key,
            display_name="Vendor",
            **stamps(),
        )


# --- Deployment ------------------------------------------------------------------------------


def test_deployment_is_component_scoped_and_half_open() -> None:
    active = deployment()
    ended = deployment(ended_at=LATER, status=DeploymentStatus.ENDED, updated_at=LATER, version=2)

    assert active.status is DeploymentStatus.ACTIVE
    assert active.ended_at is None
    assert ended.ended_at == LATER
    assert "device_id" not in Deployment.model_fields
    assert "pond_id" not in Deployment.model_fields


def test_deployment_status_matches_ended_at() -> None:
    with pytest.raises(ValidationError):
        deployment(ended_at=LATER)
    with pytest.raises(ValidationError):
        deployment(status=DeploymentStatus.ENDED)


def test_deployment_requires_ended_after_started() -> None:
    with pytest.raises(ValidationError, match="ended_at"):
        deployment(ended_at=NOW, status=DeploymentStatus.ENDED)
    with pytest.raises(ValidationError, match="ended_at"):
        deployment(ended_at=NOW - timedelta(seconds=1), status=DeploymentStatus.ENDED)


def test_deployment_interval_must_be_utc() -> None:
    with pytest.raises(ValidationError):
        deployment(started_at=datetime(2026, 8, 25))
    with pytest.raises(ValidationError):
        deployment(started_at=NOW.astimezone(timezone(timedelta(hours=2))))
    with pytest.raises(ValidationError):
        deployment(
            ended_at=datetime(2026, 8, 26),
            status=DeploymentStatus.ENDED,
        )


# --- lifecycle transitions -------------------------------------------------------------------


ALLOWED_TRANSITIONS = [
    (ensure_site_transition, SiteStatus.ACTIVE, SiteStatus.ARCHIVED),
    (ensure_asset_transition, AssetStatus.ACTIVE, AssetStatus.ARCHIVED),
    (ensure_device_transition, DeviceStatus.PROVISIONING, DeviceStatus.ACTIVE),
    (ensure_device_transition, DeviceStatus.PROVISIONING, DeviceStatus.DECOMMISSIONED),
    (ensure_device_transition, DeviceStatus.ACTIVE, DeviceStatus.SUSPENDED),
    (ensure_device_transition, DeviceStatus.SUSPENDED, DeviceStatus.ACTIVE),
    (ensure_device_transition, DeviceStatus.ACTIVE, DeviceStatus.DECOMMISSIONED),
    (ensure_device_transition, DeviceStatus.SUSPENDED, DeviceStatus.DECOMMISSIONED),
    (ensure_component_transition, ComponentStatus.ACTIVE, ComponentStatus.REPLACED),
    (ensure_component_transition, ComponentStatus.ACTIVE, ComponentStatus.DECOMMISSIONED),
    (
        ensure_integration_account_transition,
        IntegrationAccountStatus.PENDING,
        IntegrationAccountStatus.ACTIVE,
    ),
    (
        ensure_integration_account_transition,
        IntegrationAccountStatus.ACTIVE,
        IntegrationAccountStatus.ERROR,
    ),
    (
        ensure_integration_account_transition,
        IntegrationAccountStatus.ERROR,
        IntegrationAccountStatus.ACTIVE,
    ),
    (
        ensure_integration_account_transition,
        IntegrationAccountStatus.ACTIVE,
        IntegrationAccountStatus.REVOKED,
    ),
    (
        ensure_device_integration_transition,
        DeviceIntegrationStatus.PROVISIONING,
        DeviceIntegrationStatus.ACTIVE,
    ),
    (
        ensure_device_integration_transition,
        DeviceIntegrationStatus.ACTIVE,
        DeviceIntegrationStatus.ERROR,
    ),
    (
        ensure_device_integration_transition,
        DeviceIntegrationStatus.ERROR,
        DeviceIntegrationStatus.ACTIVE,
    ),
    (
        ensure_device_integration_transition,
        DeviceIntegrationStatus.ACTIVE,
        DeviceIntegrationStatus.DECOMMISSIONED,
    ),
    (ensure_deployment_transition, DeploymentStatus.ACTIVE, DeploymentStatus.ENDED),
]

FORBIDDEN_TRANSITIONS = [
    (ensure_site_transition, SiteStatus.ARCHIVED, SiteStatus.ACTIVE),
    (ensure_asset_transition, AssetStatus.ARCHIVED, AssetStatus.ACTIVE),
    (ensure_device_transition, DeviceStatus.DECOMMISSIONED, DeviceStatus.ACTIVE),
    (ensure_device_transition, DeviceStatus.DECOMMISSIONED, DeviceStatus.SUSPENDED),
    (ensure_device_transition, DeviceStatus.DECOMMISSIONED, DeviceStatus.PROVISIONING),
    (ensure_device_transition, DeviceStatus.ACTIVE, DeviceStatus.PROVISIONING),
    (ensure_component_transition, ComponentStatus.REPLACED, ComponentStatus.ACTIVE),
    (ensure_component_transition, ComponentStatus.DECOMMISSIONED, ComponentStatus.ACTIVE),
    (ensure_component_transition, ComponentStatus.REPLACED, ComponentStatus.DECOMMISSIONED),
    (
        ensure_integration_account_transition,
        IntegrationAccountStatus.REVOKED,
        IntegrationAccountStatus.ACTIVE,
    ),
    (
        ensure_integration_account_transition,
        IntegrationAccountStatus.ACTIVE,
        IntegrationAccountStatus.PENDING,
    ),
    (
        ensure_device_integration_transition,
        DeviceIntegrationStatus.DECOMMISSIONED,
        DeviceIntegrationStatus.ACTIVE,
    ),
    (
        ensure_device_integration_transition,
        DeviceIntegrationStatus.ACTIVE,
        DeviceIntegrationStatus.PROVISIONING,
    ),
    (ensure_deployment_transition, DeploymentStatus.ENDED, DeploymentStatus.ACTIVE),
]


@pytest.mark.parametrize(("check", "current", "target"), ALLOWED_TRANSITIONS)
def test_allowed_lifecycle_transitions(check, current, target) -> None:
    assert check(current, target) is None


@pytest.mark.parametrize(("check", "current", "target"), FORBIDDEN_TRANSITIONS)
def test_forbidden_lifecycle_transitions_raise_conflict(check, current, target) -> None:
    with pytest.raises(ConflictError):
        check(current, target)


@pytest.mark.parametrize(
    ("check", "enum"),
    [
        (ensure_site_transition, SiteStatus),
        (ensure_asset_transition, AssetStatus),
        (ensure_device_transition, DeviceStatus),
        (ensure_component_transition, ComponentStatus),
        (ensure_integration_account_transition, IntegrationAccountStatus),
        (ensure_device_integration_transition, DeviceIntegrationStatus),
        (ensure_deployment_transition, DeploymentStatus),
    ],
)
def test_same_state_transition_is_a_no_op(check, enum) -> None:
    for status in enum:
        assert check(status, status) is None


# --- idempotency receipt ---------------------------------------------------------------------


def receipt(**overrides) -> IdempotencyReceipt:
    values = {
        "tenant_id": "tnt_a",
        "scope": "sites.create",
        "key_hash": HEX64,
        "request_hash": "b" * 64,
        "response_snapshot_json": '{"site_id":"site_1"}',
        "created_at": NOW,
        "expires_at": NOW + timedelta(hours=24),
    }
    values.update(overrides)
    return IdempotencyReceipt(**values)


def test_idempotency_receipt_expires_exactly_after_24_hours() -> None:
    assert IDEMPOTENCY_RECEIPT_TTL == timedelta(hours=24)
    assert receipt().expires_at - receipt().created_at == IDEMPOTENCY_RECEIPT_TTL
    with pytest.raises(ValidationError, match="24"):
        receipt(expires_at=NOW + timedelta(hours=23))
    with pytest.raises(ValidationError):
        receipt(created_at=datetime(2026, 8, 25), expires_at=datetime(2026, 8, 26))


def test_new_idempotency_receipt_derives_expiry() -> None:
    issued = new_idempotency_receipt(
        tenant_id="tnt_a",
        scope="sites.create",
        key_hash=HEX64,
        request_hash="b" * 64,
        response_snapshot_json="{}",
        created_at=NOW,
    )
    assert issued.expires_at == NOW + timedelta(hours=24)


@pytest.mark.parametrize(
    "overrides",
    [
        {"key_hash": "raw-idempotency-key"},
        {"request_hash": "B" * 64},
        {"scope": "sites#create"},
        {"scope": ""},
        {"tenant_id": "tnt#a"},
        {"response_snapshot_json": "not json"},
        {"extra": "x"},
    ],
)
def test_idempotency_receipt_rejects_invalid_values(overrides) -> None:
    with pytest.raises(ValidationError):
        receipt(**overrides)


def test_format_utc_timestamp_is_fixed_six_digits() -> None:
    assert format_utc_timestamp(datetime(2026, 8, 25, 12, 30, 5, tzinfo=UTC)) == (
        "2026-08-25T12:30:05.000000Z"
    )
    assert (
        format_utc_timestamp(
            datetime(2026, 8, 25, 9, 30, 5, 42, tzinfo=timezone(timedelta(hours=-3)))
        )
        == "2026-08-25T12:30:05.000042Z"
    )
    with pytest.raises(ValueError):
        format_utc_timestamp(datetime(2026, 8, 25))
