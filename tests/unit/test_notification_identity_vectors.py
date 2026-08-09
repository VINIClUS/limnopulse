import base64
import hashlib
import json
import struct
from pathlib import Path

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "testdata" / "notification_identity_vectors.json"
)


def _fixture() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _base64url(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).rstrip(b"=").decode()


def test_relay_identity_vectors_are_language_neutral() -> None:
    fixture = _fixture()
    assert fixture["schema_version"] == 1

    for vector in fixture["relay_vectors"]:
        canonical = "\0".join(
            (vector["work_kind"], vector["tenant_id"], vector["item_id"])
        ).encode()
        bucket = struct.unpack(">Q", hashlib.sha256(canonical).digest()[:8])[0] % 64
        sort_key = "#".join(
            (
                vector["work_kind"],
                vector["scheduled_at"],
                _base64url(vector["tenant_id"]),
                _base64url(vector["item_id"]),
            )
        )

        assert bucket == vector["bucket"]
        assert f"NOTIFICATION_RELAY#V1#BUCKET#{bucket:02d}" == vector["gsi_pk"]
        assert sort_key == vector["gsi_sk"]


def test_delivery_identity_vectors_are_language_neutral() -> None:
    fixture = _fixture()

    for vector in fixture["delivery_vectors"]:
        canonical = "\0".join(
            (
                "limnopulse:delivery:v1",
                vector["event_id"],
                vector["kind"],
                vector["channel"],
                vector["recipient_id"],
            )
        ).encode()
        delivery_id = f"del_{hashlib.sha256(canonical).hexdigest()}"

        assert delivery_id == vector["delivery_id"]
