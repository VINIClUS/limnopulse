"""P2-04 read service: selector ownership (404) before any InfluxDB call; windows from the clock."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from limnopulse_api.core.errors import NotFoundError, TelemetryQueryError
from limnopulse_api.domain.telemetry_v2 import QualityState
from limnopulse_api.repositories.telemetry_v2 import ObservationSelector
from limnopulse_api.services.telemetry_v2 import ObservationReadService

TENANT = "tnt_a"
OTHER_TENANT = "tnt_other"
ASSET_ID = "ast_" + "1" * 32
COMPONENT_ID = "cmp_" + "2" * 32
DEVICE_ID = "dev_" + "3" * 32
OTHER_ASSET_ID = "ast_" + "4" * 32
NOW = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
SENTINEL = [object()]


class FakeReader:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def query_observations(self, **kwargs: object) -> list:
        self.calls.append(("query_observations", kwargs))
        if self.error is not None:
            raise self.error
        return SENTINEL

    async def query_latest_observations(self, **kwargs: object) -> list:
        self.calls.append(("query_latest_observations", kwargs))
        if self.error is not None:
            raise self.error
        return SENTINEL


class FakeEntities:
    """Three separate tenant-scoped lookups sharing one call log, so dispatch is visible.

    Each role only has its own method: wiring the wrong reader to a selector kind fails.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.site_assets = _Lookup(
            self.calls, "get_asset", {(TENANT, ASSET_ID), (OTHER_TENANT, OTHER_ASSET_ID)}
        )
        self.devices = _Lookup(self.calls, "get_device", {(TENANT, DEVICE_ID)})
        self.components = _Lookup(self.calls, "get_component", {(TENANT, COMPONENT_ID)})


class _Lookup:
    def __init__(self, calls: list, method: str, known: set[tuple[str, str]]) -> None:
        self._calls = calls
        self._method = method
        self._known = known

    def __getattr__(self, name: str) -> object:
        if name != self._method:
            raise AttributeError(f"{self._method} reader has no {name}")

        async def lookup(tenant_id: str, entity_id: str) -> object | None:
            self._calls.append((name, tenant_id, entity_id))
            if (tenant_id, entity_id) in self._known:
                return SimpleNamespace(tenant_id=tenant_id, entity_id=entity_id)
            return None

        return lookup


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now
        self.calls = 0

    def __call__(self) -> datetime:
        self.calls += 1
        return self.now


def build(
    reader: FakeReader | None = None, clock: Clock | None = None
) -> tuple[ObservationReadService, FakeReader, FakeEntities, Clock]:
    reader = reader or FakeReader()
    entities = FakeEntities()
    clock = clock or Clock(NOW)
    service = ObservationReadService(
        reader=reader,
        site_assets=entities.site_assets,
        devices=entities.devices,
        components=entities.components,
        clock=clock,
    )
    return service, reader, entities, clock


async def query(
    service: ObservationReadService,
    selector: ObservationSelector,
    *,
    start: datetime | None = None,
    stop: datetime | None = None,
    metric_keys: frozenset[str] | None = None,
    quality_states: frozenset[QualityState] | None = None,
    limit: int = 500,
    tenant_id: str = TENANT,
) -> list:
    return await service.query_observations(
        tenant_id=tenant_id,
        selector=selector,
        metric_keys=metric_keys,
        quality_states=quality_states,
        start=start,
        stop=stop,
        limit=limit,
    )


async def latest(
    service: ObservationReadService,
    selector: ObservationSelector,
    *,
    metric_keys: frozenset[str] | None = None,
    tenant_id: str = TENANT,
) -> list:
    return await service.query_latest_observations(
        tenant_id=tenant_id, selector=selector, metric_keys=metric_keys
    )


SELECTORS = [
    (ObservationSelector("asset", ASSET_ID), "get_asset"),
    (ObservationSelector("device", DEVICE_ID), "get_device"),
    (ObservationSelector("component", COMPONENT_ID), "get_component"),
]
MISSING = [
    ObservationSelector("asset", "ast_" + "9" * 32),
    ObservationSelector("device", "dev_" + "9" * 32),
    ObservationSelector("component", "cmp_" + "9" * 32),
]


def test_constructor_is_keyword_only() -> None:
    entities = FakeEntities()
    with pytest.raises(TypeError):
        ObservationReadService(
            FakeReader(), entities.site_assets, entities.devices, entities.components, Clock(NOW)
        )  # type: ignore[misc]


@pytest.mark.asyncio
@pytest.mark.parametrize(("selector", "lookup"), SELECTORS)
async def test_query_checks_the_selector_entity_in_the_tenant_then_reads(
    selector: ObservationSelector, lookup: str
) -> None:
    service, reader, entities, _ = build()
    start = NOW - timedelta(hours=3)
    stop = NOW - timedelta(hours=1)
    metric_keys = frozenset({"water.ph"})
    states = frozenset({QualityState.VALID})

    result = await query(
        service,
        selector,
        start=start,
        stop=stop,
        metric_keys=metric_keys,
        quality_states=states,
        limit=17,
    )

    assert result is SENTINEL
    assert entities.calls == [(lookup, TENANT, selector.entity_id)]
    assert reader.calls == [
        (
            "query_observations",
            {
                "tenant_id": TENANT,
                "selector": selector,
                "metric_keys": metric_keys,
                "quality_states": states,
                "start": start,
                "stop": stop,
                "limit": 17,
            },
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("selector", "lookup"), SELECTORS)
async def test_latest_checks_the_selector_entity_then_reads_24h_to_now(
    selector: ObservationSelector, lookup: str
) -> None:
    service, reader, entities, _ = build()

    result = await latest(service, selector, metric_keys=frozenset({"water.temperature"}))

    assert result is SENTINEL
    assert entities.calls == [(lookup, TENANT, selector.entity_id)]
    assert reader.calls == [
        (
            "query_latest_observations",
            {
                "tenant_id": TENANT,
                "selector": selector,
                "metric_keys": frozenset({"water.temperature"}),
                "lookback": timedelta(hours=24),
                "now": NOW,
            },
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", MISSING)
async def test_missing_selector_entity_raises_not_found_with_zero_reader_calls(
    selector: ObservationSelector,
) -> None:
    service, reader, _, _ = build()
    with pytest.raises(NotFoundError):
        await query(service, selector)
    with pytest.raises(NotFoundError):
        await latest(service, selector)
    assert reader.calls == []


@pytest.mark.asyncio
async def test_entity_of_another_tenant_is_not_found_with_zero_reader_calls() -> None:
    service, reader, entities, _ = build()
    selector = ObservationSelector("asset", OTHER_ASSET_ID)

    with pytest.raises(NotFoundError):
        await query(service, selector)
    with pytest.raises(NotFoundError):
        await latest(service, selector)

    assert reader.calls == []
    assert {call[1] for call in entities.calls} == {TENANT}


@pytest.mark.asyncio
async def test_default_window_is_the_hour_before_now() -> None:
    service, reader, _, _ = build()

    await query(service, ObservationSelector("asset", ASSET_ID))

    kwargs = reader.calls[0][1]
    assert (kwargs["start"], kwargs["stop"]) == (NOW - timedelta(hours=1), NOW)
    assert kwargs["metric_keys"] is None and kwargs["quality_states"] is None


@pytest.mark.asyncio
async def test_default_start_is_one_hour_before_a_supplied_stop() -> None:
    service, reader, _, _ = build()
    stop = NOW - timedelta(days=2)

    await query(service, ObservationSelector("asset", ASSET_ID), stop=stop)

    kwargs = reader.calls[0][1]
    assert (kwargs["start"], kwargs["stop"]) == (stop - timedelta(hours=1), stop)


@pytest.mark.asyncio
async def test_default_stop_is_now_for_a_supplied_start() -> None:
    service, reader, _, _ = build()
    start = NOW - timedelta(hours=5)

    await query(service, ObservationSelector("asset", ASSET_ID), start=start)

    kwargs = reader.calls[0][1]
    assert (kwargs["start"], kwargs["stop"]) == (start, NOW)


@pytest.mark.asyncio
async def test_clock_is_read_per_request_not_at_construction() -> None:
    clock = Clock(NOW)
    service, reader, _, _ = build(clock=clock)
    later = NOW + timedelta(minutes=7)
    clock.now = later

    await query(service, ObservationSelector("asset", ASSET_ID))
    await latest(service, ObservationSelector("asset", ASSET_ID))

    assert reader.calls[0][1]["stop"] == later
    assert reader.calls[1][1]["now"] == later


@pytest.mark.asyncio
async def test_lookup_precedes_the_read_on_the_happy_path() -> None:
    order: list[str] = []
    service, reader, entities, _ = build()
    lookup = entities.site_assets.get_asset

    async def recording_lookup(tenant_id: str, entity_id: str) -> object | None:
        order.append("lookup")
        return await lookup(tenant_id, entity_id)

    original = reader.query_observations

    async def recording_read(**kwargs: object) -> list:
        order.append("read")
        return await original(**kwargs)

    entities.site_assets.get_asset = recording_lookup  # type: ignore[method-assign]
    reader.query_observations = recording_read  # type: ignore[method-assign]

    await query(service, ObservationSelector("asset", ASSET_ID))

    assert order == ["lookup", "read"]


@pytest.mark.asyncio
async def test_reader_errors_propagate() -> None:
    service, _, _, _ = build(reader=FakeReader(error=TelemetryQueryError("down")))
    with pytest.raises(TelemetryQueryError):
        await query(service, ObservationSelector("asset", ASSET_ID))
    with pytest.raises(TelemetryQueryError):
        await latest(service, ObservationSelector("asset", ASSET_ID))


@pytest.mark.asyncio
async def test_default_start_overflow_is_a_window_error_with_zero_reads() -> None:
    from limnopulse_api.services.telemetry_v2 import ObservationWindowError

    service, reader, _, _ = build()
    with pytest.raises(ObservationWindowError):
        await query(
            service, ObservationSelector("asset", ASSET_ID), stop=datetime(1, 1, 1, tzinfo=UTC)
        )
    assert reader.calls == []
