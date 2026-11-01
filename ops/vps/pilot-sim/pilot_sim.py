"""Simulated HTTPS pilot connector for `POST /ingest/v2/observations` (P3 runbook step 4).

Sends one batch of plausible water-quality readings per run (temperature, pH, dissolved oxygen)
for one DeviceIntegration, as a real `https_direct` connector would. The systemd timer in this
directory runs it every 5 minutes.

Configuration comes from the environment (the unit's 0600 EnvironmentFile):

- `PILOT_INGEST_URL`: the ingest endpoint, `https://api.limnopulse.com/ingest/v2/observations`;
- `PILOT_EXTERNAL_DEVICE_ID`: the DeviceIntegration's `external_identity`;
- `PILOT_TOKEN_FILE`: a root-owned 0600 file holding the ingest token (`lpi_...`) and nothing else.

Stdlib only, so it runs on the VPS host without the repository or a virtualenv. It logs the HTTP
status, `ingress_id` and `accepted_observations` only: never the token, the request headers or the
error body. Exits 0 on a 202, 1 otherwise, so systemd marks a failed run.
"""

import json
import os
import random
import sys
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

CONNECTOR_VERSION = "limnopulse-pilot-sim/1"
TIMEOUT_SECONDS = 20

# (metric_key, source_unit, centre, spread): around a healthy tropical pond.
READINGS = (
    ("water.temperature", "degC", 26.0, 1.0),
    ("water.ph", "pH", 7.2, 0.2),
    ("water.dissolved_oxygen", "mg/L", 6.0, 0.5),
)


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set")
    return value


def _read_token(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        token = handle.read().strip()
    if not token.startswith("lpi_"):
        raise SystemExit("the token file does not hold an ingest token")
    return token


def build_batch(external_device_id: str, now: datetime) -> dict:
    observed_at = now.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return {
        "external_device_id": external_device_id,
        "observations": [
            {
                "metric_key": metric_key,
                "value": round(random.gauss(centre, spread / 2), 2),
                "source_unit": unit,
                "observed_at": observed_at,
            }
            for metric_key, unit, centre, spread in READINGS
        ],
    }


def main() -> int:
    url = _required_env("PILOT_INGEST_URL")
    external_device_id = _required_env("PILOT_EXTERNAL_DEVICE_ID")
    token = _read_token(_required_env("PILOT_TOKEN_FILE"))
    body = json.dumps(build_batch(external_device_id, datetime.now(UTC))).encode()
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Idempotency-Key": str(uuid.uuid4()),
            "X-Connector-Version": CONNECTOR_VERSION,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            status = response.status
            payload = json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        # The API redacts its error detail (locations and types, never input values).
        detail = error.read(2048).decode("utf-8", "replace")
        print(f"pilot batch rejected: status={error.code} detail={detail}", file=sys.stderr)
        return 1
    except (urllib.error.URLError, TimeoutError) as error:
        print(f"pilot batch failed in transport: {type(error).__name__}", file=sys.stderr)
        return 1
    print(
        f"pilot batch status={status} ingress_id={payload.get('ingress_id')} "
        f"accepted_observations={payload.get('accepted_observations')}"
    )
    return 0 if status == 202 else 1


if __name__ == "__main__":
    sys.exit(main())
