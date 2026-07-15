from datetime import datetime
import json
from typing import Any, Mapping


ZERO_TIME = "0001-01-01T00:00:00Z"


def decode_evaluator_state(item: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
    try:
        state = json.loads(str(item["state_json"]))
        revision = int(item["state_revision"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("alert evaluation state is invalid") from exc
    return state, revision


def resolved_evaluator_state(
    item: Mapping[str, Any],
    event_id: str,
    now: datetime,
) -> tuple[dict[str, Any], int] | None:
    state, revision = decode_evaluator_state(item)
    if state.get("Mode") != "active" or state.get("ActiveEventID") != event_id:
        return None
    state.update(
        {
            "Mode": "healthy",
            "ConfirmedSlots": 0,
            "PendingSince": ZERO_TIME,
            "LastBreachSlot": ZERO_TIME,
            "ActiveEventID": "",
            "ActiveStatus": "",
            "ActiveOpenedAt": ZERO_TIME,
            "OpeningOutboxes": None,
            "SuppressionSourceEventID": "",
        }
    )
    updated = {
        **item,
        "state_json": json.dumps(state, separators=(",", ":"), sort_keys=True),
        "state_revision": revision + 1,
        "updated_at": now.isoformat(),
    }
    return updated, revision


def reset_pending_evaluator_state(
    item: Mapping[str, Any],
    now: datetime,
) -> tuple[dict[str, Any], int] | None:
    state, revision = decode_evaluator_state(item)
    if state.get("Mode") != "pending":
        return None
    state.update(
        {
            "Mode": "healthy",
            "ConfirmedSlots": 0,
            "PendingSince": ZERO_TIME,
            "LastBreachSlot": ZERO_TIME,
        }
    )
    updated = {
        **item,
        "state_json": json.dumps(state, separators=(",", ":"), sort_keys=True),
        "state_revision": revision + 1,
        "updated_at": now.isoformat(),
    }
    return updated, revision
