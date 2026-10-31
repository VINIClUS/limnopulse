"""Strict `/v2` lifecycle bodies: `{status, expected_version}` with no other field.

Each status is the target's own enum, so an IntegrationAccount can never be sent a
DeviceIntegration status (or the reverse)."""

from typing import Literal

from limnopulse_api.api.v2.schemas.common import V2ExpectedVersionRequest


class V2IntegrationAccountStatusUpdate(V2ExpectedVersionRequest):
    status: Literal["pending", "active", "error", "revoked"]


class V2DeviceIntegrationStatusUpdate(V2ExpectedVersionRequest):
    status: Literal["provisioning", "active", "error", "decommissioned"]
