from datetime import UTC, datetime, timedelta
from hashlib import sha256


EVALUATION_BUCKET_COUNT = 64
EVALUATION_CADENCE = timedelta(seconds=60)
DEFAULT_ALLOWED_LATENESS = timedelta(seconds=15)
EVALUATION_SEMANTIC_FIELDS = frozenset(
    {"operator", "threshold", "aggregation", "window", "duration", "enabled"}
)
EVALUATION_SCHEDULE_FIELDS = frozenset(
    {
        "evaluation_bucket",
        "next_evaluation_at",
        "GSI1PK",
        "GSI1SK",
        "lease_owner",
        "lease_expires_at",
    }
)


def evaluation_bucket(tenant_id: str, rule_id: str) -> int:
    digest = sha256(f"{tenant_id}\0{rule_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % EVALUATION_BUCKET_COUNT


def next_complete_slot(
    value: datetime,
    *,
    cadence: timedelta = EVALUATION_CADENCE,
    allowed_lateness: timedelta = DEFAULT_ALLOWED_LATENESS,
) -> datetime:
    utc_value = value.astimezone(UTC)
    cadence_seconds = int(cadence.total_seconds())
    lateness_seconds = int(allowed_lateness.total_seconds())
    shifted = int(utc_value.timestamp()) + lateness_seconds
    boundary = ((shifted + cadence_seconds - 1) // cadence_seconds) * cadence_seconds
    return datetime.fromtimestamp(boundary - lateness_seconds, tz=UTC)


def fixed_utc_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(UTC)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc_value.microsecond:06d}000Z"


def alert_evaluation_schedule(
    tenant_id: str,
    rule_id: str,
    scheduled_at: datetime,
) -> dict[str, object]:
    bucket = evaluation_bucket(tenant_id, rule_id)
    due_at = fixed_utc_timestamp(next_complete_slot(scheduled_at))
    return {
        "evaluation_bucket": bucket,
        "next_evaluation_at": due_at,
        "GSI1PK": f"ALERT_EVALUATION#V1#BUCKET#{bucket:02d}",
        "GSI1SK": f"{due_at}#TENANT#{tenant_id}#RULE#{rule_id}",
    }
