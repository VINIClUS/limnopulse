"""Dual-write of canonical observations: `observation_numeric` first, then legacy `water_quality`.

Each target has its own default-off switch (D9). Legacy eligibility is D7, decided by
`is_legacy_eligible` once each Asset's legacy pond is resolved; ineligible observations are
counted, never raised. Any failure raises `TelemetryWriteError` and nothing is reported as
written, so the caller must not acknowledge the input and must redeliver it.
"""

from collections.abc import Awaitable, Sequence
from dataclasses import dataclass

from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.telemetry_v2 import MAX_OBSERVATIONS_PER_BATCH, CanonicalObservation
from limnopulse_api.domain.telemetry_v2_schema import build_legacy_points, is_legacy_eligible
from limnopulse_api.repositories.telemetry_v2 import LegacyObservationWriter, ObservationWriter
from limnopulse_api.repositories.v2_contracts import SiteAssetReader


@dataclass(frozen=True)
class TelemetryWriteOutcome:
    v2_points: int
    legacy_points: int
    legacy_excluded: int


class ObservationWriteService:
    def __init__(
        self,
        *,
        writer: ObservationWriter | None,
        legacy_writer: LegacyObservationWriter | None,
        site_assets: SiteAssetReader,
        v2_write_enabled: bool,
        legacy_dual_write_enabled: bool,
    ) -> None:
        if v2_write_enabled and writer is None:
            raise ValueError("an observation writer is required when v2 writes are enabled")
        if legacy_dual_write_enabled and legacy_writer is None:
            raise ValueError("a legacy writer is required when legacy dual-write is enabled")
        self.writer = writer
        self.legacy_writer = legacy_writer
        self.site_assets = site_assets
        self.v2_write_enabled = v2_write_enabled
        self.legacy_dual_write_enabled = legacy_dual_write_enabled

    async def write(self, observations: Sequence[CanonicalObservation]) -> TelemetryWriteOutcome:
        if len(observations) > MAX_OBSERVATIONS_PER_BATCH:
            raise TelemetryWriteError(
                f"telemetry batch exceeds {MAX_OBSERVATIONS_PER_BATCH} observations",
                retryable=False,
            )
        v2_points = 0
        if self.v2_write_enabled and self.writer is not None:
            await _guarded(self.writer.write_observations(observations))
            v2_points = len(observations)
        legacy_points = legacy_excluded = 0
        if self.legacy_dual_write_enabled and self.legacy_writer is not None:
            legacy_pond_by_asset = await self._legacy_pond_by_asset(observations)
            points = build_legacy_points(observations, legacy_pond_by_asset)
            legacy_excluded = sum(
                not is_legacy_eligible(o, legacy_pond_by_asset) for o in observations
            )
            if points:
                await _guarded(self.legacy_writer.write_legacy_points(points))
            legacy_points = len(points)
        return TelemetryWriteOutcome(
            v2_points=v2_points, legacy_points=legacy_points, legacy_excluded=legacy_excluded
        )

    async def _legacy_pond_by_asset(
        self, observations: Sequence[CanonicalObservation]
    ) -> dict[str, str]:
        """Resolve each distinct Asset once for this call; a missing Asset or pond is omitted.

        Only legacy default Components can be dual-written (D7), so native probes never make
        legacy dual-write depend on the Asset store.
        """
        resolved: set[tuple[str, str]] = set()
        legacy_pond_by_asset: dict[str, str] = {}
        for o in observations:
            if o.resolved.component_id != default_component_id_for_legacy_device(
                o.resolved.tenant_id, o.resolved.device_id
            ):
                continue
            key = (o.resolved.tenant_id, o.resolved.asset_id)
            if key in resolved:
                continue
            resolved.add(key)
            try:
                asset = await self.site_assets.get_asset(*key)
            except Exception as exc:
                raise TelemetryWriteError("Asset lookup failed", retryable=True) from exc
            if asset is not None and asset.pond_profile.legacy_pond_id is not None:
                legacy_pond_by_asset[o.resolved.asset_id] = asset.pond_profile.legacy_pond_id
        return legacy_pond_by_asset


async def _guarded(write: Awaitable[None]) -> None:
    """Writers classify transport failures; anything unclassified is a bug and must not loop."""
    try:
        await write
    except TelemetryWriteError:
        raise
    except Exception as exc:
        raise TelemetryWriteError("telemetry write failed unexpectedly", retryable=False) from exc
