import re
from uuid import NAMESPACE_URL, uuid4, uuid5

# Historical identifiers stay readable; "#" is excluded because IDs are interpolated into keys.
SAFE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_CANONICAL_SUFFIX = r"_[0-9a-f]{32}"


def new_tenant_id() -> str:
    return f"tnt_{uuid4().hex}"


def new_pond_id() -> str:
    return f"pond_{uuid4().hex}"


def new_device_id() -> str:
    return f"dev_{uuid4().hex}"


def new_alert_rule_id() -> str:
    return f"rule_{uuid4().hex}"


def new_site_id() -> str:
    return f"site_{uuid4().hex}"


def new_asset_id() -> str:
    return f"ast_{uuid4().hex}"


def new_component_id() -> str:
    return f"cmp_{uuid4().hex}"


def new_probe_profile_id() -> str:
    return f"prb_{uuid4().hex}"


def new_actuator_profile_id() -> str:
    return f"act_{uuid4().hex}"


def new_integration_account_id() -> str:
    return f"ia_{uuid4().hex}"


def new_device_integration_id() -> str:
    return f"di_{uuid4().hex}"


def new_deployment_id() -> str:
    return f"dep_{uuid4().hex}"


def new_capability_id() -> str:
    return f"cap_{uuid4().hex}"


def is_canonical_id(value: str, prefix: str) -> bool:
    if not isinstance(value, str):
        return False
    return re.fullmatch(re.escape(prefix) + _CANONICAL_SUFFIX, value) is not None


def require_canonical_id(value: str, prefix: str) -> str:
    if not is_canonical_id(value, prefix):
        raise ValueError(f"identifier must match {prefix}_<32 lower-hex>")
    return value


def require_safe_id(value: str) -> str:
    if not isinstance(value, str) or SAFE_ID_PATTERN.fullmatch(value) is None:
        raise ValueError("identifier must match [A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
    return value


def require_device_id(value: str) -> str:
    if isinstance(value, str) and value.startswith("dev_"):
        return require_canonical_id(value, "dev")
    return require_safe_id(value)


def _legacy_v2_id(prefix: str, discriminator: str) -> str:
    return f"{prefix}_{uuid5(NAMESPACE_URL, f'limnopulse/v4/{discriminator}').hex}"


def default_site_id(tenant_id: str) -> str:
    return _legacy_v2_id("site", f"default-site/{tenant_id}")


def asset_id_for_legacy_pond(tenant_id: str, pond_id: str) -> str:
    return _legacy_v2_id("ast", f"legacy-pond/{tenant_id}/{pond_id}")


def default_component_id_for_legacy_device(tenant_id: str, device_id: str) -> str:
    return _legacy_v2_id("cmp", f"legacy-device-component/{tenant_id}/{device_id}")


def default_probe_profile_id_for_legacy_device(tenant_id: str, device_id: str) -> str:
    return _legacy_v2_id("prb", f"legacy-device-probe/{tenant_id}/{device_id}")


def deployment_id_for_legacy_device(tenant_id: str, device_id: str) -> str:
    return _legacy_v2_id("dep", f"legacy-device-deployment/{tenant_id}/{device_id}")
