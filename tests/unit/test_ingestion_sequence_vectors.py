"""P3-00 state rules: the sequence truth table and the pure `build_ingest_marks` vectors."""

from datetime import datetime, timedelta
from itertools import permutations
from types import MappingProxyType
from typing import Any

import pytest
from pydantic import ValidationError

from limnopulse_api.domain.ingestion import (
    ComponentIngestState,
    ComponentMark,
    ConnectorMark,
    DeviceMark,
    IngestMarks,
    IngressFence,
    IngressFenceReason,
    SequenceEvent,
    build_ingest_marks,
    classify_component_sequence,
)
from limnopulse_api.domain.telemetry_v2 import QualityFlag, QuarantinedObservation, QuarantineReason
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    DEVICE,
    OBSERVED,
    OTHER_COMPONENT,
    OTHER_DEVICE,
    RECEIVED,
    TENANT,
    canonical_observation,
    envelope,
)

T0 = OBSERVED
E = SequenceEvent
INGESTED = RECEIVED + timedelta(seconds=30)
THIRD_COMPONENT = "cmp_" + "9" * 32


def t(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def state(observed_at: datetime | None = T0, sequence: int | None = 5) -> ComponentIngestState:
    return ComponentIngestState(
        tenant_id=TENANT,
        component_id=COMPONENT,
        device_id=DEVICE,
        last_received_at=RECEIVED,
        last_ingested_at=RECEIVED,
        last_observed_at=observed_at,
        last_observed_sequence=sequence,
    )


# Truth table ---------------------------------------------------------------------------------

TRUTH_TABLE: list[tuple[str, ComponentIngestState | None, list[tuple[datetime, int]], tuple]] = [
    ("no state", None, [(t(1), 9)], (E.FIRST,)),
    ("state without sequence", state(sequence=None, observed_at=None), [(t(1), 9)], (E.FIRST,)),
    ("contiguous", state(), [(t(1), 6)], (E.CONTIGUOUS,)),
    ("gap", state(), [(t(1), 8)], (E.GAP,)),
    ("regression equal", state(), [(t(1), 5)], (E.REGRESSION,)),
    ("regression lower", state(), [(t(1), 0)], (E.REGRESSION,)),
    ("duplicate", state(), [(t(0), 5)], (E.DUPLICATE,)),
    ("late same instant", state(), [(t(0), 6)], (E.LATE,)),
    ("late older instant", state(), [(t(-1), 9)], (E.LATE,)),
    ("late never regression", state(), [(t(-1), 1)], (E.LATE,)),
    ("first then contiguous", None, [(t(1), 9), (t(2), 10)], (E.FIRST, E.CONTIGUOUS)),
    ("prev advances", state(), [(t(1), 6), (t(2), 7), (t(3), 9)], (E.CONTIGUOUS,) * 2 + (E.GAP,)),
    ("late does not advance", state(), [(t(-1), 4), (t(1), 6)], (E.LATE, E.CONTIGUOUS)),
    ("regression advances", state(), [(t(1), 3), (t(2), 4)], (E.REGRESSION, E.CONTIGUOUS)),
    ("duplicate then next", state(), [(t(0), 5), (t(1), 6)], (E.DUPLICATE, E.CONTIGUOUS)),
    ("sorted by sequence too", state(), [(t(1), 7), (t(1), 6)], (E.CONTIGUOUS, E.LATE)),
]


@pytest.mark.parametrize(
    ("name", "current", "pairs", "expected"), TRUTH_TABLE, ids=[row[0] for row in TRUTH_TABLE]
)
def test_sequence_truth_table(
    name: str,
    current: ComponentIngestState | None,
    pairs: list[tuple[datetime, int]],
    expected: tuple[SequenceEvent, ...],
) -> None:
    assert classify_component_sequence(current, pairs) == expected


def test_multi_metric_readings_collapse_to_one_pair() -> None:
    pairs = [(t(1), 6), (t(1), 6), (t(1), 6), (t(2), 7), (t(2), 7)]

    assert classify_component_sequence(state(), pairs) == (E.CONTIGUOUS, E.CONTIGUOUS)


def test_observations_without_a_sequence_produce_no_event() -> None:
    pairs: list[tuple[datetime, int | None]] = [(t(1), None), (t(2), 6), (t(3), None)]

    assert classify_component_sequence(state(), pairs) == (E.CONTIGUOUS,)
    assert classify_component_sequence(state(), [(t(1), None)]) == ()
    assert classify_component_sequence(None, []) == ()


def test_shuffled_input_gives_the_same_events() -> None:
    pairs = [(t(-2), 1), (t(1), 6), (t(1), 6), (t(2), 9), (t(3), 2), (t(4), 3)]
    expected = classify_component_sequence(state(), pairs)

    assert expected == (E.LATE, E.CONTIGUOUS, E.GAP, E.REGRESSION, E.CONTIGUOUS)
    for order in permutations(pairs):
        assert classify_component_sequence(state(), list(order)) == expected


# Marks builder -------------------------------------------------------------------------------


def quarantined(
    reason: QuarantineReason, component_id: str | None = COMPONENT
) -> QuarantinedObservation:
    return QuarantinedObservation(
        reason=reason,
        tenant_id=TENANT,
        device_id=DEVICE,
        component_id=component_id,
        metric_key="temperature",
        observed_at=OBSERVED,
        received_at=RECEIVED,
        source_event_id=None,
        detail="rejected",
        source_excerpt_json="{}",
        truncated=False,
    )


def outcome(accepted: tuple = (), rejected: tuple = ()) -> PipelineOutcome:
    return PipelineOutcome(
        written=None,  # type: ignore[arg-type]
        quarantined=rejected,
        counts=MappingProxyType({}),
        accepted=accepted,
    )


def test_marks_take_per_component_maxima_from_eligible_accepted_observations() -> None:
    accepted = (
        canonical_observation(observed_at=t(1), sequence=5),
        canonical_observation(observed_at=t(2), sequence=3, metric_key="ph"),
        canonical_observation(observed_at=t(2), sequence=8, metric_key="orp"),
        canonical_observation(observed_at=t(9), sequence=99, flags=[QualityFlag.CLOCK_SKEW_FUTURE]),
        canonical_observation(component_id=OTHER_COMPONENT, observed_at=t(5), sequence=None),
        canonical_observation(
            component_id=THIRD_COMPONENT,
            observed_at=t(-500),
            sequence=4,
            flags=[QualityFlag.CLOCK_SKEW_PAST_EXTREME],
        ),
        canonical_observation(
            component_id=THIRD_COMPONENT,
            observed_at=t(1),
            sequence=1,
            flags=[QualityFlag.DELAYED, QualityFlag.REPLAY],
        ),
    )
    rejected = (
        quarantined(QuarantineReason.METRIC_UNKNOWN),
        quarantined(QuarantineReason.VALUE_INVALID),
        quarantined(QuarantineReason.DEPLOYMENT_UNRESOLVED),
    )

    marks = build_ingest_marks(envelope(), outcome(accepted, rejected), ingested_at=INGESTED)

    assert marks is not None
    assert (marks.tenant_id, marks.device_id, marks.integration_account_id) == (
        TENANT,
        DEVICE,
        ACCOUNT,
    )
    by_component = {mark.component_id: mark for mark in marks.component_marks}
    assert len(by_component) == len(marks.component_marks) == 3
    assert by_component == {
        COMPONENT: ComponentMark(
            component_id=COMPONENT,
            received_at=RECEIVED,
            ingested_at=INGESTED,
            observed_at=t(2),
            sequence=8,
        ),
        OTHER_COMPONENT: ComponentMark(
            component_id=OTHER_COMPONENT,
            received_at=RECEIVED,
            ingested_at=INGESTED,
            observed_at=None,
            sequence=None,
        ),
        THIRD_COMPONENT: ComponentMark(
            component_id=THIRD_COMPONENT,
            received_at=RECEIVED,
            ingested_at=INGESTED,
            observed_at=t(1),
            sequence=1,
        ),
    }
    assert marks.device_mark == DeviceMark(received_at=RECEIVED, ingested_at=INGESTED)
    assert marks.connector_mark == ConnectorMark(
        received_at=RECEIVED,
        accepted_at=RECEIVED,
        quarantined_at=RECEIVED,
        quarantine_reason="value_invalid",
        fenced_at=None,
        fence_reason=None,
    )


def test_server_marks_never_use_device_clocks() -> None:
    far_future = RECEIVED + timedelta(days=3650)
    accepted = (
        canonical_observation(
            observed_at=far_future, sequence=1, flags=[QualityFlag.CLOCK_SKEW_FUTURE]
        ),
    )

    marks = build_ingest_marks(envelope(), outcome(accepted), ingested_at=INGESTED)

    assert marks is not None
    (mark,) = marks.component_marks
    assert (mark.received_at, mark.ingested_at, mark.observed_at, mark.sequence) == (
        RECEIVED,
        INGESTED,
        None,
        None,
    )
    assert marks.connector_mark.accepted_at == RECEIVED


def test_quarantine_only_messages_mark_no_component() -> None:
    rejected = (
        quarantined(QuarantineReason.COMPONENT_NOT_OWNED, OTHER_COMPONENT),
        quarantined(QuarantineReason.METRIC_UNKNOWN),
    )

    marks = build_ingest_marks(envelope(), outcome((), rejected), ingested_at=INGESTED)

    assert marks is not None
    assert marks.component_marks == ()
    assert marks.device_mark == DeviceMark(received_at=RECEIVED, ingested_at=INGESTED)
    assert marks.connector_mark == ConnectorMark(
        received_at=RECEIVED,
        quarantined_at=RECEIVED,
        quarantine_reason="metric_unknown",
    )


# "No marks": the plan types `build_ingest_marks(...) -> IngestMarks` with a required
# `connector_mark`, so an empty message carries no component, device or connector mark.
NO_MARKS = IngestMarks(
    tenant_id=TENANT,
    device_id=None,
    integration_account_id=ACCOUNT,
    component_marks=(),
    device_mark=None,
    connector_mark=ConnectorMark(),
)


def test_replay_messages_produce_no_marks() -> None:
    replay = envelope(replay=True, replay_of="ing_" + "5" * 32)
    accepted = (canonical_observation(sequence=1),)
    rejected = (quarantined(QuarantineReason.METRIC_UNKNOWN),)

    assert build_ingest_marks(replay, outcome(accepted, rejected), ingested_at=INGESTED) == (
        NO_MARKS
    )
    fence = IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)
    assert build_ingest_marks(replay, None, fence=fence, ingested_at=INGESTED) == NO_MARKS


@pytest.mark.parametrize("reason", list(IngressFenceReason))
def test_verified_fences_mark_only_the_connector(reason: IngressFenceReason) -> None:
    fence = IngressFence(reason=reason, account_verified=True)

    marks = build_ingest_marks(envelope(), None, fence=fence, ingested_at=INGESTED)

    assert marks == IngestMarks(
        tenant_id=TENANT,
        device_id=None,
        integration_account_id=ACCOUNT,
        component_marks=(),
        device_mark=None,
        connector_mark=ConnectorMark(fenced_at=RECEIVED, fence_reason=reason.value),
    )


def test_unverified_fences_produce_no_marks() -> None:
    fence = IngressFence(reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=False)

    assert build_ingest_marks(envelope(), None, fence=fence, ingested_at=INGESTED) == NO_MARKS


def test_forged_cross_tenant_envelope_never_marks_the_foreign_device() -> None:
    # Tenant A's account, tenant B's device: the fence verifies only the account.
    forged = envelope(device_id=OTHER_DEVICE)
    fence = IngressFence(reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=True)

    marks = build_ingest_marks(forged, None, fence=fence, ingested_at=INGESTED)

    assert marks is not None
    assert (marks.tenant_id, marks.integration_account_id) == (TENANT, ACCOUNT)
    assert marks.device_id is None
    assert marks.device_mark is None and marks.component_marks == ()
    assert OTHER_DEVICE not in marks.model_dump_json()


def test_marks_builder_needs_exactly_one_input() -> None:
    fence = IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)
    with pytest.raises(ValueError):
        build_ingest_marks(envelope(), None, ingested_at=INGESTED)
    with pytest.raises(ValueError):
        build_ingest_marks(envelope(), outcome(), fence=fence, ingested_at=INGESTED)
    with pytest.raises(ValueError):
        build_ingest_marks(envelope(), outcome(), ingested_at=datetime(2026, 10, 30, 12))


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (ComponentMark, {"component_id": COMPONENT, "observed_at": OBSERVED}),
        (ComponentMark, {"component_id": COMPONENT, "sequence": 1}),
        (ConnectorMark, {"quarantined_at": RECEIVED}),
        (ConnectorMark, {"quarantine_reason": "metric_unknown"}),
        (ConnectorMark, {"fenced_at": RECEIVED}),
        (ConnectorMark, {"fence_reason": "account_revoked"}),
    ],
    ids=[
        "observed-only",
        "sequence-only",
        "quarantined-at",
        "quarantine-reason",
        "fenced-at",
        "fence-reason",
    ],
)
def test_paired_marks_are_written_together(model: type, payload: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(payload)
