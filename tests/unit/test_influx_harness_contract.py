from __future__ import annotations

import ast
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "tests/support/influx_harness.py"
INFLUX_CONFTEST = ROOT / "tests/influx/conftest.py"
GO_HARNESS = ROOT / "internal/alertevaluator/influx/integration_harness_test.go"
SCRIPT = ROOT / "scripts/dev/run_local_influxd.sh"
DOCS = ROOT / "docs/verification.md"

ENV_DEFAULTS = {
    "INFLUX_INTEGRATION_URL": "http://127.0.0.1:8086",
    "INFLUX_INTEGRATION_TOKEN": "local-dev-token",
    "INFLUX_INTEGRATION_ORG": "limnopulse",
}
INFLUX_ENV = ("RUN_INFLUX_INTEGRATION", *ENV_DEFAULTS)
TARBALL = "influxdb2-2.7.11_linux_amd64.tar.gz"
TARBALL_SHA256 = "8d7872013cad3524fb728ca8483d0adc30125ad1af262ab826dcf5d1801159cf"

# Replaces influxdb_client.InfluxDBClient in a pytester subprocess so the
# harness can be exercised without a server. Every call is logged as JSON.
FAKE_CLIENT_CONFTEST = r'''
import json
import os

import influxdb_client
import influxdb_client.client.influxdb_client as client_module


def _log(event, **fields):
    with open(os.environ["HARNESS_FAKE_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps({"event": event, **fields}, default=str) + "\n")


def _rules(rules):
    if rules is None:
        return None
    return [getattr(rule, "every_seconds", repr(rule)) for rule in rules]


class FakeBucket:
    def __init__(self, name, retention_rules):
        self.name = name
        self.id = "id-" + str(name)
        self.retention_rules = retention_rules


class FakeBucketsApi:
    def create_bucket(
        self, bucket=None, bucket_name=None, org_id=None, retention_rules=None,
        description=None, org=None,
    ):
        if bucket is not None:
            bucket_name = getattr(bucket, "name", bucket_name)
            retention_rules = getattr(bucket, "retention_rules", retention_rules)
        _log("create_bucket", name=bucket_name, retention_rules=_rules(retention_rules))
        if os.environ.get("HARNESS_FAKE_UNREACHABLE") == "1":
            raise ConnectionRefusedError("fake InfluxDB is unreachable")
        return FakeBucket(bucket_name, retention_rules)

    def delete_bucket(self, bucket):
        bucket_id = getattr(bucket, "id", bucket)
        _log("delete_bucket", id=bucket_id)


class FakeQueryApi:
    def query(self, *args, **kwargs):
        return []


class FakeWriteApi:
    def __init__(self, write_type):
        self.write_type = write_type

    def write(self, bucket, org=None, record=None, write_precision=None, **kwargs):
        lines = record.splitlines() if isinstance(record, str) else list(record)
        if isinstance(record, (bytes, bytearray)):
            lines = record.decode().splitlines()
        _log("write", bucket=bucket, lines=[str(line) for line in lines],
             write_type=self.write_type)

    def flush(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class FakeClient:
    def __init__(self, url=None, token=None, debug=None, timeout=10000,
                 enable_gzip=False, org=None, **kwargs):
        _log("client", url=url, token=token, org=org)
        self.org = org

    def buckets_api(self):
        return FakeBucketsApi()

    def query_api(self, *args, **kwargs):
        return FakeQueryApi()

    def write_api(self, write_options=None, *args, **kwargs):
        write_type = getattr(write_options, "write_type", None)
        return FakeWriteApi(str(write_type))

    def ping(self):
        return os.environ.get("HARNESS_FAKE_UNREACHABLE") != "1"

    def close(self):
        _log("close")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


influxdb_client.InfluxDBClient = FakeClient
client_module.InfluxDBClient = FakeClient
'''

PROBE_TESTS = r'''
import json
import os

from conftest import FakeQueryApi, FakeWriteApi
from tests.support.influx_harness import influx_bucket, influx_clients, write_lines  # noqa: F401


def _seen(name, bucket):
    with open(os.environ["HARNESS_FAKE_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps({"event": "test", "test": name, "bucket": bucket}) + "\n")


def test_first(influx_bucket):
    _seen("first", influx_bucket)
    assert isinstance(influx_bucket, str) and influx_bucket


def test_second(influx_bucket):
    _seen("second", influx_bucket)


def test_failing(influx_bucket):
    _seen("failing", influx_bucket)
    assert False, "deliberate failure"


def test_clients_and_write_lines(influx_bucket):
    _seen("clients", influx_bucket)
    with influx_clients(influx_bucket) as (query_api, write_api):
        assert isinstance(query_api, FakeQueryApi)
        assert isinstance(write_api, FakeWriteApi)
        assert write_api.write_type == "WriteType.synchronous"
    write_lines(influx_bucket, ["m,probe=a v=1 1", "m,probe=b v=2 2"])
'''


def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in INFLUX_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PYTHONPATH", str(ROOT))


def _events(log: Path) -> list[dict[str, object]]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def fake_run(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch):
    _clean_env(monkeypatch)
    log = pytester.path / "fake.jsonl"
    monkeypatch.setenv("HARNESS_FAKE_LOG", str(log))
    pytester.makeconftest(FAKE_CLIENT_CONFTEST)
    # Outside any tests/influx directory: the skip must live in the fixture.
    pytester.mkdir("elsewhere")
    (pytester.path / "elsewhere" / "test_probe.py").write_text(PROBE_TESTS, encoding="utf-8")

    def run(*args: str):
        result = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-rs", *args)
        return result, _events(log)

    return run


def test_harness_module_imports_without_skipping_or_connecting() -> None:
    env = {key: value for key, value in os.environ.items() if key not in INFLUX_ENV}
    env["PYTHONPATH"] = str(ROOT)
    probe = (
        "import tests.support.influx_harness as h\n"
        "assert callable(h.influx_clients) and callable(h.write_lines)\n"
        "assert h.influx_bucket is not None\n"
        "print('imported')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "imported"


@pytest.mark.parametrize("value", [None, "", "0", "true", "yes"])
def test_fixture_skips_unless_run_influx_integration_is_exactly_1(
    fake_run, monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    if value is not None:
        monkeypatch.setenv("RUN_INFLUX_INTEGRATION", value)

    result, events = fake_run("-k", "first")

    result.assert_outcomes(skipped=1)
    assert "RUN_INFLUX_INTEGRATION" in result.stdout.str()
    assert [event for event in events if event["event"] != "test"] == []
    assert not any(event["event"] == "test" for event in events)


def test_fixture_creates_unique_unretained_buckets_and_deletes_each_at_teardown(
    fake_run, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RUN_INFLUX_INTEGRATION", "1")

    result, events = fake_run("-k", "first or second or failing")

    result.assert_outcomes(passed=2, failed=1)
    clients = [event for event in events if event["event"] == "client"]
    assert clients
    for client in clients:
        assert (client["url"], client["token"], client["org"]) == tuple(ENV_DEFAULTS.values())

    created = [event for event in events if event["event"] == "create_bucket"]
    seen = [event for event in events if event["event"] == "test"]
    assert [event["test"] for event in seen] == ["first", "second", "failing"]
    assert [event["name"] for event in created] == [event["bucket"] for event in seen]
    assert len({event["name"] for event in created}) == 3
    for event in created:
        rules = event["retention_rules"]
        assert rules is None or all(every_seconds == 0 for every_seconds in rules), rules

    # A second process must not reuse the first process's bucket names.
    _again, events_again = fake_run("-k", "first or second")
    names_again = {
        event["name"] for event in events_again[len(events) :] if event["event"] == "create_bucket"
    }
    assert len(names_again) == 2
    assert names_again.isdisjoint(event["name"] for event in created)

    deleted = [event["id"] for event in events if event["event"] == "delete_bucket"]
    assert sorted(deleted) == sorted(f"id-{event['name']}" for event in created)
    # Each bucket is deleted after its own test ran and before the next test starts.
    order = [
        f"test:{event['bucket']}" if event["event"] == "test" else f"delete:{event['id']}"
        for event in events
        if event["event"] in ("test", "delete_bucket")
    ]
    for name in (event["name"] for event in created):
        position = order.index(f"test:{name}")
        assert order[position + 1] == f"delete:id-{name}", order


def test_fixture_honors_url_token_and_org_overrides(
    fake_run, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RUN_INFLUX_INTEGRATION", "1")
    monkeypatch.setenv("INFLUX_INTEGRATION_URL", "http://127.0.0.1:18086")
    monkeypatch.setenv("INFLUX_INTEGRATION_TOKEN", "override-token")
    monkeypatch.setenv("INFLUX_INTEGRATION_ORG", "override-org")

    result, events = fake_run("-k", "first")

    result.assert_outcomes(passed=1)
    clients = [event for event in events if event["event"] == "client"]
    assert clients
    for client in clients:
        assert (client["url"], client["token"], client["org"]) == (
            "http://127.0.0.1:18086",
            "override-token",
            "override-org",
        )


def test_fixture_errors_instead_of_skipping_when_opted_in_and_unreachable(
    fake_run, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RUN_INFLUX_INTEGRATION", "1")
    monkeypatch.setenv("HARNESS_FAKE_UNREACHABLE", "1")

    result, events = fake_run("-k", "first")

    result.assert_outcomes(errors=1)
    assert not any(event["event"] == "test" for event in events)


def test_influx_clients_and_write_lines_target_the_fixture_bucket_synchronously(
    fake_run, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RUN_INFLUX_INTEGRATION", "1")

    result, events = fake_run("-k", "clients")

    result.assert_outcomes(passed=1)
    bucket = next(event["bucket"] for event in events if event["event"] == "test")
    writes = [event for event in events if event["event"] == "write"]
    assert writes
    assert {event["bucket"] for event in writes} == {bucket}
    assert [line for event in writes for line in event["lines"]] == [
        "m,probe=a v=1 1",
        "m,probe=b v=2 2",
    ]
    assert all(event["write_type"] == "WriteType.synchronous" for event in writes)
    assert sum(event["event"] == "client" for event in events) == sum(
        event["event"] == "close" for event in events
    )


INFLUX_ITEM = "def test_inside_influx():\n    pass\n"
OTHER_ITEM = "def test_outside_influx():\n    pass\n"


@pytest.mark.parametrize(
    ("value", "outcomes"),
    [(None, {"passed": 1, "skipped": 1}), ("0", {"passed": 1, "skipped": 1}), ("1", {"passed": 2})],
)
def test_influx_conftest_skips_only_tests_influx_items_without_opt_in(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
    outcomes: dict[str, int],
) -> None:
    _clean_env(monkeypatch)
    if value is not None:
        monkeypatch.setenv("RUN_INFLUX_INTEGRATION", value)
    pytester.makeconftest("")
    influx_dir = pytester.mkdir("tests").joinpath("influx")
    influx_dir.mkdir()
    (influx_dir / "conftest.py").write_text(
        INFLUX_CONFTEST.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (influx_dir / "test_inside.py").write_text(INFLUX_ITEM, encoding="utf-8")
    other_dir = pytester.path / "tests" / "other"
    other_dir.mkdir()
    (other_dir / "test_outside.py").write_text(OTHER_ITEM, encoding="utf-8")

    result = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-rs", "tests")

    result.assert_outcomes(**outcomes)
    if "skipped" in outcomes:
        result.stdout.fnmatch_lines(["*test_inside.py*RUN_INFLUX_INTEGRATION*"])


def test_influx_conftest_re_exports_the_harness_fixture() -> None:
    tree = ast.parse(INFLUX_CONFTEST.read_text(encoding="utf-8"))

    imported = {
        alias.asname or alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "tests.support.influx_harness"
        for alias in node.names
    }
    assert "influx_bucket" in imported


def test_default_run_of_tests_influx_skips_everything_cleanly() -> None:
    env = {key: value for key, value in os.environ.items() if key not in INFLUX_ENV}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-rs", "-p", "no:cacheprovider", "tests/influx"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    summary = result.stdout.strip().splitlines()[-1]
    assert re.match(r"\d+ skipped\b", summary), summary
    for outcome in ("passed", "failed", "error", "xpassed", "xfailed", "deselected"):
        assert outcome not in summary, summary


def test_harness_sources_name_the_contract_environment_and_defaults() -> None:
    for path in (HARNESS, GO_HARNESS):
        text = path.read_text(encoding="utf-8")
        for literal in ("RUN_INFLUX_INTEGRATION", *ENV_DEFAULTS, *ENV_DEFAULTS.values()):
            assert re.search(rf"[\"']{re.escape(literal)}[\"']", text), (path, literal)

    go_text = GO_HARNESS.read_text(encoding="utf-8")
    assert "package influx\n" in go_text
    assert re.search(r"func newIntegrationBucket\(t \*testing\.T\) \(", go_text)
    assert re.search(r"func writeLines\(t \*testing\.T,", go_text)
    assert ".BucketsAPI()" in go_text
    assert "CreateBucket" in go_text
    assert "DeleteBucket" in go_text
    assert "t.Cleanup(" in go_text
    assert "func TestInfluxIntegrationHarnessSmoke(t *testing.T)" in go_text


def test_local_runner_pins_the_influxd_release_and_runs_it_on_loopback() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    code = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    ).replace("\\\n", " ")

    assert text.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in code
    assert SCRIPT.stat().st_mode & stat.S_IXUSR
    syntax = subprocess.run(["bash", "-n", str(SCRIPT)], check=False, capture_output=True)
    assert syntax.returncode == 0, syntax.stderr

    assert "2.7.11" in code
    assert TARBALL in code
    assert TARBALL_SHA256 in code
    assert re.search(r"sha256sum\b|shasum -a 256\b", code)
    assert "https://dl.influxdata.com/influxdb/releases/" in code
    assert "${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/influxd" in code

    assert "${INFLUXD_PORT:-8086}" in code
    assert re.search(r"--http-bind-address[= ]\"?127\.0\.0\.1:", code)
    assert "0.0.0.0" not in code
    assert "/health" in code
    assert re.search(r"\.pid\b", code)

    assert re.search(r"curl\b[^\n]*-X POST[^\n]*/api/v2/setup", code)
    assert "conflict" in code
    assert "onboarding has already been completed" in code
    for default in ("local-dev-token", "limnopulse"):
        assert default in code

    lowered = code.lower()
    assert "docker" not in lowered
    for insecure in (" -k ", "--insecure", "--no-check-certificate"):
        assert insecure not in code
    assert re.search(r"\binflux (setup|ping|bucket)\b", code) is None


def test_verification_docs_describe_the_influx_gate() -> None:
    text = DOCS.read_text(encoding="utf-8")
    lowered = text.lower()

    for phrase in (
        "`make verify-influx`",
        "`scripts/dev/run_local_influxd.sh`",
        "`RUN_INFLUX_INTEGRATION=1`",
        "`tests/influx/`",
        "`TestInfluxIntegration`",
        "`tests/support/influx_harness.py`",
        "`influxdb:2.7.11-alpine`",
        "InfluxDB 2.7.11",
        TARBALL_SHA256,
        *(f"`{name}`" for name in ENV_DEFAULTS),
        *(f"`{default}`" for default in ENV_DEFAULTS.values()),
    ):
        assert phrase in text, phrase

    for phrase in (
        "without docker",
        "not part of `make verify`",
        "every flux query",
        "absolute `range` bounds",
        "non-empty",
        "deleted at teardown",
        "throwaway",
    ):
        assert phrase in lowered, phrase


# PATH shims for the runner's behaviour. The fake curl answers like curl:
# -o writes the body, -w prints %{http_code}, -f/--fail suppresses the body
# and exits 22 on HTTP >= 400, --fail-with-body keeps it.
FAKE_CURL = r"""#!/usr/bin/env bash
printf 'curl %s\n' "$*" >> "$FAKE_LOG"
out=""; fmt=""; fail=0; keep_body=0; url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o|--output) out="$2"; shift ;;
    -w|--write-out) fmt="$2"; shift ;;
    -H|--header|-d|--data|--data-raw|--data-binary|-X|--request|-m|--max-time|\
    --connect-timeout|--retry|--retry-delay|--retry-max-time|-u|--user|-A|--user-agent)
      shift ;;
    --fail) fail=1 ;;
    --fail-with-body) fail=1; keep_body=1 ;;
    --*) ;;
    -*) [[ "$1" == *f* ]] && fail=1 ;;
    *) url="$1" ;;
  esac
  shift
done
code=404; body=""
case "$url" in
  https://dl.influxdata.com/*) code=200; body="this is not the influxdb tarball" ;;
  http://127.0.0.1:*/health) code=200; body='{"name":"influxdb","status":"pass"}' ;;
  http://127.0.0.1:*/api/v2/setup) code="$FAKE_SETUP_CODE"; body="$FAKE_SETUP_BODY" ;;
esac
if [ "$fail" = 1 ] && [ "$code" -ge 400 ] && [ "$keep_body" = 0 ]; then
  body=""
fi
if [ -n "$out" ]; then printf '%s' "$body" > "$out"; else printf '%s' "$body"; fi
if [ -n "$fmt" ]; then printf '%s' "${fmt//%\{http_code\}/$code}"; fi
if [ "$fail" = 1 ] && [ "$code" -ge 400 ]; then exit 22; fi
exit 0
"""

FAKE_INFLUXD = """#!/usr/bin/env bash
printf 'influxd %s\\n' "$*" >> "$FAKE_LOG"
exit 1
"""

CONFLICT = '{"code":"conflict","message":"onboarding has already been completed"}'


@pytest.fixture
def runner_env(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("curl", FAKE_CURL), ("influxd", FAKE_INFLUXD)):
        shim = bin_dir / name
        shim.write_text(body, encoding="utf-8")
        shim.chmod(0o755)
    log = tmp_path / "calls.log"
    log.touch()
    env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "HOME": str(tmp_path / "home"),
        "XDG_CACHE_HOME": str(tmp_path / "xdg"),
        "FAKE_LOG": str(log),
        "INFLUXD_PORT": "18086",
    }
    cache = tmp_path / "xdg" / "limnopulse" / "influxd"

    def run(setup_code: int, setup_body: str):
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=tmp_path,
            env={**env, "FAKE_SETUP_CODE": str(setup_code), "FAKE_SETUP_BODY": setup_body},
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return result, log.read_text(encoding="utf-8").splitlines()

    return cache, run


@pytest.fixture
def live_influxd(runner_env):
    cache, run = runner_env
    cache.mkdir(parents=True)
    process = subprocess.Popen(["sleep", "300"])
    (cache / "influxd.pid").write_text(f"{process.pid}\n", encoding="utf-8")
    try:
        yield run
    finally:
        process.kill()
        process.wait()


@pytest.mark.parametrize(
    ("setup_code", "setup_body"),
    [(201, '{"user":{},"org":{},"bucket":{},"auth":{}}'), (422, CONFLICT)],
)
def test_local_runner_reuses_a_running_influxd_and_accepts_setup_or_conflict(
    live_influxd, setup_code: int, setup_body: str
) -> None:
    result, calls = live_influxd(setup_code, setup_body)

    assert result.returncode == 0, result.stdout + result.stderr
    assert not any(call.startswith("influxd") for call in calls), calls
    assert not any("dl.influxdata.com" in call for call in calls), calls
    assert any("http://127.0.0.1:18086/health" in call for call in calls), calls
    setup_calls = [call for call in calls if "/api/v2/setup" in call]
    assert setup_calls, calls
    assert all("-X POST" in call for call in setup_calls), setup_calls
    local_urls = re.findall(r"http://127\.0\.0\.1:(\d+)", "\n".join(calls))
    assert local_urls and set(local_urls) == {"18086"}, calls


def test_local_runner_fails_when_setup_fails_for_another_reason(live_influxd) -> None:
    result, calls = live_influxd(401, '{"code":"unauthorized","message":"unauthorized access"}')

    assert result.returncode != 0, result.stdout + result.stderr
    assert any("/api/v2/setup" in call for call in calls), calls


def test_local_runner_refuses_a_tarball_with_the_wrong_sha256(runner_env) -> None:
    cache, run = runner_env

    result, calls = run(201, "{}")

    assert result.returncode != 0, result.stdout + result.stderr
    assert any(f"dl.influxdata.com/influxdb/releases/{TARBALL}" in call for call in calls), calls
    assert not any(call.startswith("influxd") for call in calls), calls
    assert not any("/api/v2/setup" in call for call in calls), calls
    output = (result.stdout + result.stderr).lower()
    assert "sha256" in output or "checksum" in output, output
    assert not (cache / "influxd.pid").exists()
