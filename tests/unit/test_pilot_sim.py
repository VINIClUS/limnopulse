"""Exercise the simulated pilot connector against a real local HTTP server (the ingest boundary)."""

from __future__ import annotations

import importlib.util
import json
import socket
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from limnopulse_api.domain.ingestion import IngestObservationBatch

ROOT = Path(__file__).resolve().parents[2]
TOKEN = "lpi_ic_00000000000000000000000000000000_test-secret-value"


def _load_pilot_sim():
    spec = importlib.util.spec_from_file_location(
        "pilot_sim", ROOT / "ops" / "vps" / "pilot-sim" / "pilot_sim.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pilot_sim = _load_pilot_sim()


class _Ingest:
    def __init__(self, status: int, body: dict) -> None:
        self.status = status
        self.body = body
        self.requests: list[tuple[dict[str, str], bytes]] = []


@pytest.fixture
def ingest():
    state = _Ingest(202, {"ingress_id": "ing_test", "accepted_observations": 3, "replayed": False})

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers["Content-Length"])
            state.requests.append((dict(self.headers), self.rfile.read(length)))
            payload = json.dumps(state.body).encode()
            self.send_response(state.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_port}/ingest/v2/observations"
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def configured(tmp_path, monkeypatch):
    token_file = tmp_path / "ingest.token"
    token_file.write_text(TOKEN + "\n", encoding="utf-8")

    def configure(url: str) -> None:
        monkeypatch.setenv("PILOT_INGEST_URL", url)
        monkeypatch.setenv("PILOT_EXTERNAL_DEVICE_ID", "pilot-sim-001")
        monkeypatch.setenv("PILOT_TOKEN_FILE", str(token_file))

    return configure


def test_batch_matches_the_public_ingest_model():
    batch = pilot_sim.build_batch("pilot-sim-001", datetime(2026, 10, 31, 21, 15, 5, tzinfo=UTC))

    parsed = IngestObservationBatch.model_validate_json(json.dumps(batch))

    assert parsed.external_device_id == "pilot-sim-001"
    assert [item.metric_key for item in parsed.observations] == [
        "water.temperature",
        "water.ph",
        "water.dissolved_oxygen",
    ]
    assert {item.component_id for item in parsed.observations} == {None}
    assert batch["observations"][0]["observed_at"] == "2026-10-31T21:15:05.000Z"


def test_accepted_batch_sends_the_ingest_headers_and_never_logs_the_token(
    ingest, configured, capsys
):
    configured(ingest.url)

    assert pilot_sim.main() == 0

    [(headers, body)] = ingest.requests
    assert headers["Authorization"] == f"Bearer {TOKEN}"
    assert headers["Content-Type"] == "application/json"
    assert 8 <= len(headers["Idempotency-Key"]) <= 128
    assert headers["X-Connector-Version"] == pilot_sim.CONNECTOR_VERSION
    IngestObservationBatch.model_validate_json(body)
    out = capsys.readouterr()
    assert "status=202 ingress_id=ing_test accepted_observations=3" in out.out
    assert TOKEN not in out.out + out.err


def test_each_run_uses_a_new_idempotency_key(ingest, configured):
    configured(ingest.url)

    pilot_sim.main()
    pilot_sim.main()

    keys = {headers["Idempotency-Key"] for headers, _ in ingest.requests}
    assert len(keys) == 2


def test_rejected_batch_fails_with_the_redacted_detail(ingest, configured, capsys):
    ingest.status = 401
    ingest.body = {"detail": "unauthorized"}
    configured(ingest.url)

    assert pilot_sim.main() == 1

    err = capsys.readouterr().err
    assert "status=401" in err
    assert "unauthorized" in err
    assert TOKEN not in err


def test_success_other_than_202_is_a_failed_run(ingest, configured):
    ingest.status = 200
    configured(ingest.url)

    assert pilot_sim.main() == 1


def test_transport_failure_is_a_failed_run(configured, capsys):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    configured(f"http://127.0.0.1:{port}/ingest/v2/observations")

    assert pilot_sim.main() == 1

    assert "failed in transport: URLError" in capsys.readouterr().err


def test_missing_configuration_stops_before_any_request(monkeypatch):
    monkeypatch.delenv("PILOT_INGEST_URL", raising=False)

    with pytest.raises(SystemExit, match="PILOT_INGEST_URL is not set"):
        pilot_sim.main()


def test_a_token_file_without_an_ingest_token_is_refused(ingest, configured, tmp_path, monkeypatch):
    configured(ingest.url)
    wrong = tmp_path / "wrong.token"
    wrong.write_text("not-a-token\n", encoding="utf-8")
    monkeypatch.setenv("PILOT_TOKEN_FILE", str(wrong))

    with pytest.raises(SystemExit, match="does not hold an ingest token"):
        pilot_sim.main()

    assert ingest.requests == []
