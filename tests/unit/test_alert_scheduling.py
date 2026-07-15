from datetime import UTC, datetime

from limnopulse_api.domain.alert_scheduling import (
    alert_evaluation_schedule,
    evaluation_bucket,
    next_complete_slot,
)


def test_evaluation_bucket_uses_canonical_sha256_vectors() -> None:
    assert evaluation_bucket("tnt_1", "rule_1") == 29
    assert evaluation_bucket("tnt_alpha", "rule_beta") == 6


def test_next_complete_slot_is_aligned_without_drift() -> None:
    assert next_complete_slot(datetime(2026, 7, 15, 12, 0, 30, tzinfo=UTC)) == datetime(
        2026, 7, 15, 12, 0, 45, tzinfo=UTC
    )
    assert next_complete_slot(datetime(2026, 7, 15, 12, 0, 50, tzinfo=UTC)) == datetime(
        2026, 7, 15, 12, 1, 45, tzinfo=UTC
    )


def test_schedule_attributes_are_sparse_and_lexicographically_ordered() -> None:
    scheduled = alert_evaluation_schedule(
        "tnt_1",
        "rule_1",
        datetime(2026, 7, 15, 12, 0, 30, tzinfo=UTC),
    )

    assert scheduled == {
        "evaluation_bucket": 29,
        "next_evaluation_at": "2026-07-15T12:00:45.000000000Z",
        "GSI1PK": "ALERT_EVALUATION#V1#BUCKET#29",
        "GSI1SK": (
            "2026-07-15T12:00:45.000000000Z#TENANT#tnt_1#RULE#rule_1"
        ),
    }

