from pydantic import BaseModel, ConfigDict, Field

from limnopulse_api.domain.alert_events import AlertEvent


class AlertEventResponse(AlertEvent):
    pass


class AlertEventListResponse(BaseModel):
    items: list[AlertEventResponse]


class AlertEventTransitionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1)
