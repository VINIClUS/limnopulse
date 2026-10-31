"""P3-10 compose and configuration (D7, D16, D21): the profile-gated telemetry ingest worker in
both compose files, its dedicated production principal, and the P3 entries of the env examples."""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from limnopulse_api.core.config import Settings

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "compose.yaml"
PRODUCTION_COMPOSE = ROOT / "compose.production.yaml"
ENV_EXAMPLE = ROOT / ".env.example"
PRODUCTION_ENV_EXAMPLE = ROOT / ".env.production.example"
WORKER = "telemetry-ingest-worker"
PROFILE = "ingestion"
ENTRYPOINT = ["python", "-m", "limnopulse_api.workers.telemetry_ingest"]
IMAGE_TAG = "main-0000000000000000000000000000000000000000"
API_IMAGE = "ghcr.io/viniclus/limnopulse/api:${IMAGE_TAG}"
WORKER_CREDENTIALS = {
    "AWS_ACCESS_KEY_ID": "${TELEMETRY_INGEST_AWS_ACCESS_KEY_ID}",
    "AWS_SECRET_ACCESS_KEY": "${TELEMETRY_INGEST_AWS_SECRET_ACCESS_KEY}",
}
WORKER_PLACEHOLDERS = (
    "TELEMETRY_INGEST_AWS_ACCESS_KEY_ID",
    "TELEMETRY_INGEST_AWS_SECRET_ACCESS_KEY",
    "TELEMETRY_INGEST_INFLUXDB_TOKEN",
)
# Services on main before P3-10; P3-10 only adds the worker.
LOCAL_SERVICES_BEFORE_P3_10 = {
    "redis",
    "dynamodb-local",
    "elasticmq",
    "influxdb",
    "mqtt-broker",
    "telegraf",
    "alert-evaluator",
    "notification-relay",
    "notification-worker",
    "telegram-bot-api-fake",
    "telegram-notification-worker",
}
PRODUCTION_SERVICES_BEFORE_P3_10 = {"api", "web", "redis", "influxdb"}
HARDENING_KEYS = ("read_only", "tmpfs", "security_opt", "cap_drop", "logging")
# env name -> Settings field for every P3 setting (P3-00 adds the fields; P3-10 documents them).
P3_SETTINGS = {
    "INGEST_V2_ENABLED": "ingest_v2_enabled",
    "SQS_TELEMETRY_INGRESS_QUEUE_URL": "sqs_telemetry_ingress_queue_url",
    "SQS_TELEMETRY_INGRESS_DLQ_URL": "sqs_telemetry_ingress_dlq_url",
    "INGEST_RATE_LIMIT_PER_MINUTE": "ingest_rate_limit_per_minute",
    "INGEST_RATE_LIMIT_BURST": "ingest_rate_limit_burst",
    "INGEST_RATE_LIMITER_MAX_KEYS": "ingest_rate_limiter_max_keys",
    "INGEST_PREAUTH_RATE_LIMIT_PER_MINUTE": "ingest_preauth_rate_limit_per_minute",
    "INGEST_PREAUTH_GLOBAL_PER_MINUTE": "ingest_preauth_global_per_minute",
    "TELEMETRY_INGEST_PROCESSING_DEADLINE_SECONDS": "telemetry_ingest_processing_deadline_seconds",
    "TELEMETRY_INGEST_WORKER_CONCURRENCY": "telemetry_ingest_worker_concurrency",
    "TELEMETRY_INGEST_WORKER_MAX_MESSAGES": "telemetry_ingest_worker_max_messages",
    "TELEMETRY_INGEST_WORKER_WAIT_SECONDS": "telemetry_ingest_worker_wait_seconds",
    "TELEMETRY_INGEST_WORKER_VISIBILITY_SECONDS": "telemetry_ingest_worker_visibility_seconds",
    "TELEMETRY_INGEST_RETRY_BASE_SECONDS": "telemetry_ingest_retry_base_seconds",
    "TELEMETRY_INGEST_RETRY_CAP_SECONDS": "telemetry_ingest_retry_cap_seconds",
    "TELEMETRY_INGEST_DRAIN_SECONDS": "telemetry_ingest_drain_seconds",
}
EMPTY_BY_DEFAULT = ("SQS_TELEMETRY_INGRESS_QUEUE_URL", "SQS_TELEMETRY_INGRESS_DLQ_URL")
INTERPOLATION = re.compile(r"\$\{(?P<name>[A-Z0-9_]+)(?:(?P<op>:?[-?])(?P<arg>[^}]*))?\}")

requires_docker = pytest.mark.skipif(
    shutil.which("docker") is None, reason="the docker CLI renders compose files"
)


@pytest.fixture(autouse=True)
def _isolate_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (*P3_SETTINGS, "SQS_ENDPOINT_URL", "API_V2_ENABLED", "TELEMETRY_V2_WRITE_ENABLED"):
        monkeypatch.delenv(name, raising=False)


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _services(path: Path) -> dict[str, Any]:
    return _load(path)["services"]


def _assignments(path: Path) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        assert name not in assignments, f"duplicate {path.name} entry: {name}"
        assignments[name] = value
    return assignments


def _interpolate(value: str, variables: dict[str, str]) -> str:
    """Compose-style `${VAR}`, `${VAR:-default}` and `${VAR:?error}` interpolation."""

    def replace(match: re.Match[str]) -> str:
        name, op, arg = match.group("name"), match.group("op"), match.group("arg")
        current = variables.get(name)
        if op in (":-", "-") and (current is None or (op == ":-" and current == "")):
            return arg or ""
        if op in (":?", "?") and (current is None or (op == ":?" and current == "")):
            raise AssertionError(f"required variable {name} is unset")
        return current or ""

    return INTERPOLATION.sub(replace, value)


def _environment(service: dict[str, Any]) -> dict[str, str]:
    environment = service.get("environment") or {}
    if isinstance(environment, list):
        environment = dict(item.split("=", 1) for item in environment)
    return {str(key): str(value) for key, value in environment.items()}


def _settings_from(environment: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> Settings:
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    return Settings(_env_file=None)


def _depends_on(service: dict[str, Any]) -> dict[str, dict[str, Any]]:
    depends = service.get("depends_on") or {}
    if isinstance(depends, list):
        return {name: {} for name in depends}
    return depends


# --- Local compose -----------------------------------------------------------------------------


def test_local_worker_is_profile_gated_and_built_from_the_api_dockerfile() -> None:
    worker = _services(COMPOSE)[WORKER]

    assert worker["profiles"] == [PROFILE]
    assert worker["build"] == {"context": ".", "dockerfile": "Dockerfile.api"}
    assert worker["entrypoint"] == ENTRYPOINT
    assert worker["restart"] == "no"
    assert "ports" not in worker


def test_local_worker_depends_on_healthy_elasticmq_dynamodb_and_influxdb() -> None:
    depends = _depends_on(_services(COMPOSE)[WORKER])

    assert set(depends) == {"elasticmq", "dynamodb-local", "influxdb"}
    assert depends["elasticmq"].get("condition") == "service_healthy"


def test_local_compose_only_adds_the_worker() -> None:
    services = _services(COMPOSE)

    assert set(services) == LOCAL_SERVICES_BEFORE_P3_10 | {WORKER}
    for name, service in services.items():
        if name != WORKER:
            assert PROFILE not in (service.get("profiles") or []), name


def test_local_worker_uses_dummy_credentials_and_the_local_endpoints() -> None:
    environment = _environment(_services(COMPOSE)[WORKER])

    assert environment["AWS_ACCESS_KEY_ID"] == "local"
    assert environment["AWS_SECRET_ACCESS_KEY"] == "local"
    assert environment["AWS_EC2_METADATA_DISABLED"] == "true"
    assert environment["APP_ENV"] == "local"
    assert environment["SQS_ENDPOINT_URL"] == "http://elasticmq:9324"
    assert environment["SQS_TELEMETRY_INGRESS_QUEUE_URL"] == (
        "http://elasticmq:9324/000000000000/limnopulse-telemetry-ingress"
    )
    assert environment["DYNAMODB_ENDPOINT_URL"] == "http://dynamodb-local:8000"
    assert environment["INFLUXDB_URL"] == "http://influxdb:8086"
    assert environment["TELEMETRY_V2_WRITE_ENABLED"] == "true"


def test_local_worker_environment_passes_the_worker_start_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _environment(_services(COMPOSE)[WORKER])

    settings = _settings_from(environment, monkeypatch)

    assert settings.telemetry_v2_write_enabled is True
    assert settings.sqs_telemetry_ingress_queue_url.endswith("/limnopulse-telemetry-ingress")
    assert settings.dynamodb_endpoint_url == "http://dynamodb-local:8000"
    assert settings.sqs_endpoint_url == "http://elasticmq:9324"


@requires_docker
def test_local_compose_config_parses_with_and_without_the_ingestion_profile() -> None:
    base = ["docker", "compose", "-f", str(COMPOSE)]

    default = subprocess.run(
        [*base, "config", "--services"], capture_output=True, text=True, check=True, timeout=60
    )
    profiled = subprocess.run(
        [*base, "--profile", PROFILE, "config", "--services"],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )

    assert WORKER not in default.stdout.split()
    assert WORKER in profiled.stdout.split()


# --- Production compose ----------------------------------------------------------------------


def test_production_worker_is_profile_gated_and_reuses_the_api_image() -> None:
    services = _services(PRODUCTION_COMPOSE)
    worker = services[WORKER]

    assert worker["profiles"] == [PROFILE]
    assert worker["image"] == API_IMAGE == services["api"]["image"]
    assert "build" not in worker
    assert worker["entrypoint"] == ENTRYPOINT
    assert worker["restart"] == "on-failure:5"
    assert "ports" not in worker
    assert "influxdb" in _depends_on(worker)


def test_production_compose_only_adds_the_worker() -> None:
    services = _services(PRODUCTION_COMPOSE)

    assert set(services) == PRODUCTION_SERVICES_BEFORE_P3_10 | {WORKER}
    for name, service in services.items():
        if name != WORKER:
            assert "profiles" not in service, name


def test_production_worker_has_the_api_hardening() -> None:
    services = _services(PRODUCTION_COMPOSE)
    worker, api = services[WORKER], services["api"]

    for key in HARDENING_KEYS:
        assert worker.get(key) == api[key], key
    limits = worker["deploy"]["resources"]["limits"]
    assert set(limits) == {"cpus", "memory", "pids"}


def test_production_worker_stays_off_the_public_edge_network() -> None:
    worker = _services(PRODUCTION_COMPOSE)[WORKER]

    networks = worker.get("networks") or {"default": {}}
    assert "edge" not in networks


def test_production_worker_runs_as_its_own_principal() -> None:
    worker = _services(PRODUCTION_COMPOSE)[WORKER]
    environment = _environment(worker)

    # The shared `.env` carries the API principal's AWS keys, so the worker must not load it.
    assert "env_file" not in worker
    for name, value in WORKER_CREDENTIALS.items():
        assert environment[name] == value, name
    raw = json.dumps(worker)
    assert "${AWS_ACCESS_KEY_ID" not in raw
    assert "${AWS_SECRET_ACCESS_KEY" not in raw


def test_production_worker_gets_its_queue_and_a_dedicated_influx_write_token() -> None:
    environment = _environment(_services(PRODUCTION_COMPOSE)[WORKER])

    assert environment["SQS_TELEMETRY_INGRESS_QUEUE_URL"] == "${SQS_TELEMETRY_INGRESS_QUEUE_URL}"
    assert environment["INFLUXDB_TOKEN"] == "${TELEMETRY_INGEST_INFLUXDB_TOKEN}"
    assert environment["INFLUXDB_URL"] == "http://influxdb:8086"
    assert "SQS_ENDPOINT_URL" not in environment
    assert "DYNAMODB_ENDPOINT_URL" not in environment


def test_production_worker_environment_validates_from_the_production_example(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    variables = _assignments(PRODUCTION_ENV_EXAMPLE)
    variables["TELEMETRY_V2_WRITE_ENABLED"] = "true"
    variables["SQS_TELEMETRY_INGRESS_QUEUE_URL"] = (
        "https://sqs.us-east-2.amazonaws.com/000000000000/limnopulse-telemetry-ingress"
    )
    environment = {
        name: _interpolate(value, variables)
        for name, value in _environment(_services(PRODUCTION_COMPOSE)[WORKER]).items()
    }

    settings = _settings_from(environment, monkeypatch)

    assert settings.app_env == "prod"
    assert settings.auth_mode == "cognito"
    assert settings.telemetry_v2_write_enabled is True
    assert settings.sqs_telemetry_ingress_queue_url == variables["SQS_TELEMETRY_INGRESS_QUEUE_URL"]
    assert settings.dynamodb_domain_table == variables["DYNAMODB_DOMAIN_TABLE"]
    assert settings.dynamodb_endpoint_url is None
    assert settings.sqs_endpoint_url is None
    assert environment["AWS_ACCESS_KEY_ID"] == variables["TELEMETRY_INGEST_AWS_ACCESS_KEY_ID"]
    assert environment["AWS_ACCESS_KEY_ID"] != variables["AWS_ACCESS_KEY_ID"]


def test_production_worker_writes_stay_off_until_the_owner_flips_them() -> None:
    variables = _assignments(PRODUCTION_ENV_EXAMPLE)
    environment = {
        name: _interpolate(value, variables)
        for name, value in _environment(_services(PRODUCTION_COMPOSE)[WORKER]).items()
    }

    assert environment.get("TELEMETRY_V2_WRITE_ENABLED", "false") == "false"


@requires_docker
def test_rendered_production_worker_never_receives_the_api_credentials(tmp_path: Path) -> None:
    variables = _assignments(PRODUCTION_ENV_EXAMPLE)
    variables.update(
        {
            "AWS_ACCESS_KEY_ID": "sentinel-api-access-key",
            "AWS_SECRET_ACCESS_KEY": "sentinel-api-secret-key",
            "TELEMETRY_INGEST_AWS_ACCESS_KEY_ID": "sentinel-worker-access-key",
            "TELEMETRY_INGEST_AWS_SECRET_ACCESS_KEY": "sentinel-worker-secret-key",
            "TELEMETRY_INGEST_INFLUXDB_TOKEN": "sentinel-worker-write-token",
            "INFLUXDB_TOKEN": "sentinel-admin-token",
        }
    )
    env_file = tmp_path / ".env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in variables.items()), encoding="utf-8")
    environment = {k: v for k, v in os.environ.items() if k not in variables}
    environment["IMAGE_TAG"] = IMAGE_TAG

    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(env_file),
            "-f",
            str(PRODUCTION_COMPOSE),
            "--profile",
            PROFILE,
            "config",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
        env=environment,
        timeout=60,
    )
    worker = json.loads(result.stdout)["services"][WORKER]
    rendered = json.dumps(worker)

    assert _environment(worker)["AWS_ACCESS_KEY_ID"] == "sentinel-worker-access-key"
    assert _environment(worker)["AWS_SECRET_ACCESS_KEY"] == "sentinel-worker-secret-key"
    assert _environment(worker)["INFLUXDB_TOKEN"] == "sentinel-worker-write-token"
    for api_secret in (
        "sentinel-api-access-key",
        "sentinel-api-secret-key",
        "sentinel-admin-token",
    ):
        assert api_secret not in rendered
    assert worker["entrypoint"] == ENTRYPOINT
    assert worker["image"] == f"ghcr.io/viniclus/limnopulse/api:{IMAGE_TAG}"


@requires_docker
@pytest.mark.parametrize("profile", [None, PROFILE])
def test_production_compose_config_parses_with_and_without_the_profile(
    profile: str | None,
) -> None:
    command = [
        "docker",
        "compose",
        "--env-file",
        str(PRODUCTION_ENV_EXAMPLE),
        "-f",
        str(PRODUCTION_COMPOSE),
    ]
    if profile is not None:
        command += ["--profile", profile]
    environment = {**os.environ, "IMAGE_TAG": IMAGE_TAG}

    result = subprocess.run(
        [*command, "config", "--services"],
        capture_output=True,
        text=True,
        check=True,
        env=environment,
        timeout=60,
    )

    assert (WORKER in result.stdout.split()) is (profile is not None)


# --- Env examples --------------------------------------------------------------------------------


def test_env_example_lists_every_p3_setting_once_at_its_default() -> None:
    assignments = _assignments(ENV_EXAMPLE)
    defaults = Settings(_env_file=None, app_env="test", auth_mode="dev")

    for env_name, field in P3_SETTINGS.items():
        assert env_name in assignments, env_name
        default = getattr(defaults, field)
        if isinstance(default, bool):
            assert assignments[env_name] == str(default).lower(), env_name
        else:
            assert assignments[env_name] == str(default), env_name
    assert assignments["INGEST_V2_ENABLED"] == "false"
    for env_name in EMPTY_BY_DEFAULT:
        assert assignments[env_name] == "", env_name


def test_env_example_keeps_a_single_sqs_endpoint_line() -> None:
    lines = [
        line
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("SQS_ENDPOINT_URL=")
    ]

    assert lines == ["SQS_ENDPOINT_URL=http://localhost:9324"]


def test_env_example_loads_with_ingestion_dark(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    for name in _assignments(ENV_EXAMPLE):
        monkeypatch.delenv(name, raising=False)

    settings = Settings(_env_file=ENV_EXAMPLE)
    defaults = Settings(_env_file=None, app_env="test", auth_mode="dev")

    assert settings.ingest_v2_enabled is False
    for field in P3_SETTINGS.values():
        assert getattr(settings, field) == getattr(defaults, field), field


def test_production_env_example_documents_the_worker_principal_placeholders() -> None:
    assignments = _assignments(PRODUCTION_ENV_EXAMPLE)

    for name in WORKER_PLACEHOLDERS:
        assert assignments[name].startswith("REPLACE_WITH_"), name
    assert assignments["TELEMETRY_INGEST_AWS_ACCESS_KEY_ID"] != assignments["AWS_ACCESS_KEY_ID"]
    assert assignments["INGEST_V2_ENABLED"] == "false"
    assert assignments["SQS_TELEMETRY_INGRESS_QUEUE_URL"].startswith("REPLACE_WITH_")
    assert "SQS_ENDPOINT_URL" not in assignments
    assert "DYNAMODB_ENDPOINT_URL" not in assignments


def test_production_env_example_loads_with_ingestion_dark(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    for name in _assignments(PRODUCTION_ENV_EXAMPLE):
        monkeypatch.delenv(name, raising=False)

    settings = Settings(_env_file=PRODUCTION_ENV_EXAMPLE)

    assert settings.ingest_v2_enabled is False
    assert settings.telemetry_v2_write_enabled is False


# --- README and architecture wiring ------------------------------------------------------------

README = ROOT / "README.md"
ARCHITECTURE = ROOT / "docs/architecture.md"
P3_HEADING = re.compile(r"^(?P<level>#{2,3}) .*\bV4 P3\b.*$", re.MULTILINE)
RUNBOOK_LINK = "docs/p3-ingestion-rollout-runbook.md"
ARCHITECTURE_HEADINGS_BEFORE_P3_10 = (
    "## V4 architecture baseline",
    "## Delivery status",
    "## System view",
    "## Canonical names",
    "## M1 v2 domain foundation (behind disabled rollout flags)",
    "## P2 canonical telemetry (behind disabled rollout switches)",
    "## Authentication and tenant authorization",
    "## Phase 3A: Alert Rule configuration",
    "## Phase 3B: evaluation",
    "## Phase 3C-A: email delivery",
    "## Phase 3C-B: Telegram binding and delivery",
    "## Security boundaries",
)


def _p3_section(text: str) -> str:
    match = P3_HEADING.search(text)
    assert match is not None, "missing a `V4 P3` section heading"
    level = len(match.group("level"))
    following = re.search(rf"^#{{1,{level}}} ", text[match.end() :], flags=re.MULTILINE)
    end = match.end() + following.start() if following else len(text)
    return text[match.start() : end]


def _assert_documents_p3_wiring(section: str) -> None:
    for needle in (
        "INGEST_V2_ENABLED",
        "API_V2_ENABLED",
        "TELEMETRY_V2_WRITE_ENABLED",
        "SQS_TELEMETRY_INGRESS_QUEUE_URL",
        "/ingest/v2/observations",
        WORKER,
        f"--profile {PROFILE}",
        "telemetry_ingest",
        "TELEMETRY_INGEST_AWS_ACCESS_KEY_ID",
    ):
        assert needle in section, needle
    lowered = section.lower()
    assert "ships dark" in lowered
    assert "rollback" in lowered
    assert "exit 2" in lowered or "exits 2" in lowered or "exit code 2" in lowered


def test_readme_documents_the_p3_wiring_and_links_the_runbook() -> None:
    section = _p3_section(README.read_text(encoding="utf-8"))

    _assert_documents_p3_wiring(section)
    assert "scripts/dev/seed_ingest_local.py" in section
    assert RUNBOOK_LINK in section


def test_architecture_documents_the_p3_wiring_and_keeps_its_pins_and_sections() -> None:
    architecture = ARCHITECTURE.read_text(encoding="utf-8")

    assert "**Version:** 1.4" in architecture
    assert "**Updated:** 2026-08-25" in architecture
    _assert_documents_p3_wiring(_p3_section(architecture))
    headings = re.findall(r"^## .+$", architecture, flags=re.MULTILINE)
    remaining = iter(headings)
    for expected in ARCHITECTURE_HEADINGS_BEFORE_P3_10:
        assert expected in remaining, f"existing section missing or reordered: {expected}"
