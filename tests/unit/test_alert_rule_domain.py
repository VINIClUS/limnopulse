import re
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from limnopulse_api.domain.alerts import (
    AlertAggregation,
    AlertChannel,
    AlertMetric,
    AlertOperator,
    AlertRule,
    AlertSeverity,
)
from limnopulse_api.domain.ids import new_alert_rule_id


def make_rule(**updates: object) -> AlertRule:
    now = datetime(2026, 7, 15, tzinfo=UTC)
    values: dict[str, object] = {
        "tenant_id": "tnt_1",
        "rule_id": "rule_1",
        "pond_id": "pond_1",
        "device_id": "dev_1",
        "metric": AlertMetric.DO_MG_L,
        "name": "Low oxygen",
        "operator": AlertOperator.LESS_THAN,
        "threshold": 5.0,
        "aggregation": AlertAggregation.MIN,
        "window": "5m",
        "duration": "3m",
        "severity": AlertSeverity.CRITICAL,
        "channels": (AlertChannel.EMAIL, AlertChannel.TELEGRAM),
        "cooldown_seconds": 1_800,
        "enabled": True,
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }
    values.update(updates)
    return AlertRule.model_validate(values)


@pytest.mark.parametrize("duration", ["59s", "25h", "0m", "1d", "five-minutes"])
def test_alert_rule_rejects_duration_outside_bounds_or_format(duration: str) -> None:
    with pytest.raises(ValidationError):
        make_rule(window=duration)


@pytest.mark.parametrize("duration", ["60s", "1m", "5m", "1h", "24h"])
def test_alert_rule_accepts_canonical_duration_within_bounds(duration: str) -> None:
    assert make_rule(duration=duration).duration == duration


def test_alert_rule_rejects_duplicate_or_empty_channels() -> None:
    with pytest.raises(ValidationError):
        make_rule(channels=())
    with pytest.raises(ValidationError):
        make_rule(channels=(AlertChannel.EMAIL, AlertChannel.EMAIL))


def test_alert_rule_rejects_non_finite_threshold_and_invalid_cooldown() -> None:
    with pytest.raises(ValidationError):
        make_rule(threshold=float("inf"))
    with pytest.raises(ValidationError):
        make_rule(cooldown_seconds=59)
    with pytest.raises(ValidationError):
        make_rule(cooldown_seconds=86_401)


def test_alert_rule_is_immutable() -> None:
    rule = make_rule()

    with pytest.raises(ValidationError):
        rule.threshold = 4.5


def test_alert_rule_defaults_to_first_evaluation_revision() -> None:
    assert make_rule().evaluation_revision == 1


@pytest.mark.parametrize("metric", [AlertMetric.BATTERY_V, AlertMetric.RSSI])
def test_device_metrics_require_device_id(metric: AlertMetric) -> None:
    with pytest.raises(ValidationError, match="device_id is required"):
        make_rule(metric=metric, device_id=None)


def test_new_alert_rule_id_has_canonical_prefix() -> None:
    assert re.fullmatch(r"rule_[0-9a-f]{32}", new_alert_rule_id())


def test_alert_rule_enums_match_phase_3a_contract() -> None:
    assert {metric.value for metric in AlertMetric} == {
        "temp_c",
        "ph",
        "do_mg_l",
        "turbidity_ntu",
        "salinity_ppt",
        "battery_v",
        "rssi",
    }
    assert {operator.value for operator in AlertOperator} == {"<", "<=", ">", ">="}
    assert {aggregation.value for aggregation in AlertAggregation} == {
        "min",
        "max",
        "mean",
        "last",
    }
    assert {severity.value for severity in AlertSeverity} == {"warning", "critical"}
    assert {channel.value for channel in AlertChannel} == {"email", "telegram"}
