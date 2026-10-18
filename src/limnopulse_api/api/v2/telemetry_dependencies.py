"""Dependencies of the unmounted `/v2` telemetry routers; P2-08 sets `app.state.observation_reader`."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated

from fastapi import Depends, HTTPException, Request

from limnopulse_api.repositories.telemetry_v2 import ObservationReader


def get_observation_reader(request: Request) -> ObservationReader:
    reader = getattr(request.app.state, "observation_reader", None)
    if reader is None:
        raise HTTPException(status_code=503, detail="service unavailable")
    return reader


def _utc_now() -> datetime:
    return datetime.now(UTC)


def get_clock() -> Callable[[], datetime]:
    return _utc_now


ObservationReaderDep = Annotated[ObservationReader, Depends(get_observation_reader)]
ClockDep = Annotated[Callable[[], datetime], Depends(get_clock)]
