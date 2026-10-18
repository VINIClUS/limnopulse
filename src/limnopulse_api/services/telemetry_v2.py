"""Canonical observation reads: selector ownership is checked before any InfluxDB call."""

from collections.abc import Callable
from datetime import datetime, timedelta

from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.telemetry_v2 import QualityState, StoredObservation
from limnopulse_api.repositories.telemetry_v2 import ObservationReader, ObservationSelector
from limnopulse_api.repositories.v2_contracts import ComponentReader, DeviceReader, SiteAssetReader

DEFAULT_OBSERVATION_WINDOW = timedelta(hours=1)
MAX_OBSERVATION_RANGE = timedelta(days=31)
# A latest-by-observed_at data view, not a health or freshness view (D11, ADR-006).
LATEST_OBSERVATION_LOOKBACK = timedelta(hours=24)


class ObservationWindowError(ValueError):
    """The resolved `[start, stop)` window is empty, reversed or longer than 31 days."""


class ObservationReadService:
    def __init__(
        self,
        *,
        reader: ObservationReader,
        site_assets: SiteAssetReader,
        devices: DeviceReader,
        components: ComponentReader,
        clock: Callable[[], datetime],
    ) -> None:
        self.reader = reader
        self.site_assets = site_assets
        self.devices = devices
        self.components = components
        self.clock = clock

    async def query_observations(
        self,
        *,
        tenant_id: str,
        selector: ObservationSelector,
        metric_keys: frozenset[str] | None,
        quality_states: frozenset[QualityState] | None,
        start: datetime | None,
        stop: datetime | None,
        limit: int,
    ) -> list[StoredObservation]:
        if stop is None:
            stop = self.clock()
        if start is None:
            try:
                start = stop - DEFAULT_OBSERVATION_WINDOW
            except OverflowError:
                raise ObservationWindowError(
                    "stop is too early to derive a default start"
                ) from None
        if stop <= start:
            raise ObservationWindowError("stop must be after start")
        if stop - start > MAX_OBSERVATION_RANGE:
            raise ObservationWindowError("the observation range is at most 31 days")
        await self._require_selector(tenant_id, selector)
        return await self.reader.query_observations(
            tenant_id=tenant_id,
            selector=selector,
            metric_keys=metric_keys,
            quality_states=quality_states,
            start=start,
            stop=stop,
            limit=limit,
        )

    async def query_latest_observations(
        self,
        *,
        tenant_id: str,
        selector: ObservationSelector,
        metric_keys: frozenset[str] | None,
    ) -> list[StoredObservation]:
        await self._require_selector(tenant_id, selector)
        return await self.reader.query_latest_observations(
            tenant_id=tenant_id,
            selector=selector,
            metric_keys=metric_keys,
            lookback=LATEST_OBSERVATION_LOOKBACK,
            now=self.clock(),
        )

    async def _require_selector(self, tenant_id: str, selector: ObservationSelector) -> None:
        if selector.kind == "asset":
            entity = await self.site_assets.get_asset(tenant_id, selector.entity_id)
        elif selector.kind == "device":
            entity = await self.devices.get_device(tenant_id, selector.entity_id)
        else:
            entity = await self.components.get_component(tenant_id, selector.entity_id)
        if entity is None:
            raise NotFoundError(f"{selector.kind} not found")
