"""Load and validate `ops/workers/roles.yaml`, the ADR-020 normative role table."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
ROLES_PATH = ROOT / "ops/workers/roles.yaml"

ADR_ROLES = (
    "alert-evaluator",
    "notification-relay",
    "email-worker",
    "ses-feedback-worker",
    "telegram-worker",
    "worker-monitor",
    "limnopulse-admin",
    "telemetry-ingest",
)
EXIT_CODES = {
    "success": 0,
    "runtime_failure": 1,
    "partial_run": 2,
    "configuration_error": 78,
}
AWS_CREDENTIALS = ("aws-access-key-id", "aws-secret-access-key")
LIFECYCLES = {"continuous", "oneshot", "cli", "compose"}
GO_LIFECYCLES = {"continuous", "oneshot", "cli"}
UNIT_LIFECYCLES = {"continuous", "oneshot"}
COMMON_FIELDS = (
    "name",
    "entrypoint",
    "lifecycle",
    "work",
    "iam_principal",
    "unit_health",
    "queues",
    "alarms",
)
LIFECYCLE_FIELDS = {
    "oneshot": (
        "schedule_seconds",
        "timeout_start_seconds",
        "units",
        "binary",
        "env_file",
        "credentials",
        "enable_variable",
        "rollback",
    ),
    "continuous": (
        "single_instance",
        "stall_after_seconds",
        "watchdog_seconds",
        "timeout_stop_seconds",
        "units",
        "binary",
        "env_file",
        "credentials",
        "enable_variable",
        "rollback",
    ),
    "cli": ("binary", "unit_health_reason", "rollback"),
    "compose": (
        "compose_service",
        "compose_profile",
        "enable_variable",
        "unit_health_reason",
    ),
}
UNIT_ONLY_FIELDS = ("units", "env_file", "credentials")
QUEUE_ALARMS = ("queue-age", "queue-depth", "queue-dlq")
SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
KEBAB = re.compile(r"^[a-z][a-z0-9-]*$")


def load_roles(path: Path = ROLES_PATH) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def snake(name: str) -> str:
    return name.replace("-", "_")


def roles_by_name(table: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {role["name"]: role for role in table["roles"]}


def validate_roles(table: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    problems += _validate_top_level(table)
    if not isinstance(table.get("roles"), list):
        return problems + ["roles must be a list"]
    paths = table.get("paths", {})
    declared = _declared_shared_lifecycles(table, problems)
    shared_roles = {role for roles in declared for role in roles}
    names = [role.get("name") for role in table["roles"]]
    if tuple(names) != ADR_ROLES:
        problems.append(f"roles must be exactly the ADR-020 roles in order: {names}")
    for role in table["roles"]:
        problems += _validate_role(role, paths, table, shared_roles)
    problems += _validate_uniqueness(table["roles"], declared)
    return problems


def _validate_top_level(table: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if table.get("schema_version") != 1:
        problems.append("schema_version must be 1")
    if table.get("exit_codes") != EXIT_CODES:
        problems.append(f"exit_codes must be exactly {EXIT_CODES}")
    for key in (
        "paths",
        "unit_health",
        "bundle",
        "activation",
        "deploy",
        "shared_lifecycles",
        "standalone_alarms",
    ):
        if key not in table:
            problems.append(f"missing top-level field: {key}")
    health = table.get("unit_health", {})
    if (health.get("namespace"), health.get("metric"), health.get("dimension")) != (
        "Limnopulse/Workers",
        "UnitHealthy",
        "Role",
    ):
        problems.append("unit_health must publish Limnopulse/Workers UnitHealthy by Role")
    if health.get("published_when") != "unit_file_enabled":
        problems.append("unit_health must publish only for enabled units")
    alarms = {alarm.get("name") for alarm in table.get("standalone_alarms", [])}
    if "ses-events-routing-dlq" not in alarms:
        problems.append("the SES routing DLQ needs its own standalone alarm")
    return problems


def _declared_shared_lifecycles(
    table: dict[str, Any], problems: list[str]
) -> list[frozenset[str]]:
    declared: list[frozenset[str]] = []
    for entry in table.get("shared_lifecycles") or []:
        roles = entry.get("roles") or []
        if len(roles) < 2:
            problems.append(f"a shared lifecycle needs at least two roles: {entry}")
        if not str(entry.get("justification") or "").strip():
            problems.append(f"shared lifecycle {roles} has no justification")
        declared.append(frozenset(roles))
    return declared


def _validate_role(
    role: dict[str, Any],
    paths: dict[str, str],
    table: dict[str, Any],
    shared_roles: set[str],
) -> list[str]:
    name = role.get("name", "<unnamed>")
    problems = [
        f"{name}: missing field {field}" for field in COMMON_FIELDS if field not in role
    ]
    lifecycle = role.get("lifecycle")
    if lifecycle not in LIFECYCLES:
        return problems + [f"{name}: unknown lifecycle {lifecycle!r}"]
    problems += [
        f"{name}: missing field {field}"
        for field in LIFECYCLE_FIELDS[lifecycle]
        if field not in role
    ]
    if problems:
        return problems
    if not KEBAB.match(name):
        problems.append(f"{name}: role names are kebab-case")
    if not SNAKE.match(role["iam_principal"]):
        problems.append(f"{name}: iam_principal must be an OpenTofu identifier")
    for queue in role["queues"]:
        if not SNAKE.match(str(queue.get("queue"))) or queue.get("access") not in {
            "consume",
            "produce",
        }:
            problems.append(f"{name}: invalid queue entry {queue}")
    if lifecycle in GO_LIFECYCLES:
        problems += _validate_go_role(name, role, paths)
    if lifecycle in UNIT_LIFECYCLES:
        problems += _validate_unit_role(name, role, paths, shared_roles)
    else:
        problems += [
            f"{name}: a {lifecycle} role has no {field}"
            for field in UNIT_ONLY_FIELDS
            if field in role
        ]
        if role["unit_health"] != "none":
            problems.append(f"{name}: a {lifecycle} role publishes no unit health")
        if not str(role.get("unit_health_reason") or "").strip():
            problems.append(f"{name}: unit_health none needs a reason")
        if "unit-health" in role["alarms"]:
            problems.append(f"{name}: a role without unit health has no unit-health alarm")
    if lifecycle == "oneshot":
        if role["schedule_seconds"] != 60:
            problems.append(f"{name}: one-shot roles run every 60 s")
        if not 0 < role["timeout_start_seconds"] < role["schedule_seconds"]:
            problems.append(f"{name}: a one-shot run must end before the next tick")
        if role["units"].get("timer") != f"limnopulse-{name}.timer":
            problems.append(f"{name}: timer must be limnopulse-{name}.timer")
    if lifecycle == "continuous":
        if "timer" in role["units"]:
            problems.append(f"{name}: a continuous role has no timer")
        if not 0 < role["stall_after_seconds"] < role["watchdog_seconds"]:
            problems.append(f"{name}: the stall exit must fire before the systemd watchdog")
        if role["timeout_stop_seconds"] <= 0:
            problems.append(f"{name}: timeout_stop_seconds must be positive")
        if not isinstance(role["single_instance"], bool):
            problems.append(f"{name}: single_instance must be a boolean")
        if not any(queue["access"] == "consume" for queue in role["queues"]):
            problems.append(f"{name}: a continuous worker consumes a queue")
    if lifecycle in {"continuous", "compose"}:
        missing = [alarm for alarm in QUEUE_ALARMS if alarm not in role["alarms"]]
        if missing:
            problems.append(f"{name}: consumer queue alarms missing: {missing}")
    if lifecycle == "compose":
        if role["compose_profile"] not in table.get("deploy", {}).get(
            "compose_profiles", []
        ):
            problems.append(f"{name}: its Compose profile must be in the deploy profile list")
        if role.get("rollback") is not None:
            problems.append(f"{name}: a Compose role is not in the workers bundle")
    if name == "ses-feedback-worker" and role.get("single_instance") is not True:
        problems.append("ses-feedback-worker runs as a single instance")
    return problems


def _validate_go_role(name: str, role: dict[str, Any], paths: dict[str, str]) -> list[str]:
    problems: list[str] = []
    if role["entrypoint"] != f"cmd/{name}":
        problems.append(f"{name}: entrypoint must be cmd/{name}")
    if role["binary"] != f"{paths.get('current')}/bin/{name}":
        problems.append(f"{name}: binary must be {paths.get('current')}/bin/{name}")
    rollback = role["rollback"]
    epoch, floor = rollback.get("epoch"), rollback.get("floor")
    if not (
        type(epoch) is int and type(floor) is int and epoch >= 1 and 0 <= floor <= epoch
    ):
        problems.append(f"{name}: rollback needs integers with 0 <= floor <= epoch >= 1")
    return problems


def _validate_unit_role(
    name: str, role: dict[str, Any], paths: dict[str, str], shared_roles: set[str]
) -> list[str]:
    problems: list[str] = []
    if name not in shared_roles and role["units"].get("service") != (
        f"limnopulse-{name}.service"
    ):
        problems.append(f"{name}: service must be limnopulse-{name}.service")
    if role["env_file"] != f"{paths.get('config')}/{name}.env":
        problems.append(f"{name}: env_file must be {paths.get('config')}/{name}.env")
    credentials = role["credentials"]
    if len(set(credentials)) != len(credentials) or not all(
        KEBAB.match(credential) for credential in credentials
    ):
        problems.append(f"{name}: credential names must be unique kebab-case")
    if not set(AWS_CREDENTIALS) <= set(credentials):
        problems.append(f"{name}: a unit receives its AWS keys through LoadCredential=")
    if role["enable_variable"] != f"{snake(name)}_enabled":
        problems.append(f"{name}: enable_variable must be {snake(name)}_enabled")
    if role["unit_health"] != "systemd":
        problems.append(f"{name}: a unit role publishes systemd unit health")
    if "unit-health" not in role["alarms"]:
        problems.append(f"{name}: a unit role has a unit-health alarm")
    return problems


def _validate_uniqueness(
    roles: list[dict[str, Any]], declared: list[frozenset[str]]
) -> list[str]:
    problems: list[str] = []
    for field in ("name", "iam_principal"):
        counts = Counter(role.get(field) for role in roles)
        problems += [
            f"duplicate {field}: {value}" for value, count in counts.items() if count > 1
        ]
    timers = Counter(role.get("units", {}).get("timer") for role in roles)
    problems += [
        f"duplicate unit: {timer}" for timer, count in timers.items() if timer and count > 1
    ]
    processes: dict[str, set[str]] = {}
    for role in roles:
        process = role.get("units", {}).get("service") or role.get("compose_service")
        if process:
            processes.setdefault(process, set()).add(role.get("name"))
    for process, members in processes.items():
        if len(members) > 1 and frozenset(members) not in declared:
            problems.append(
                f"undeclared shared lifecycle: {sorted(members)} share {process}"
            )
    for shared in declared:
        if not any(frozenset(members) == shared for members in processes.values()):
            problems.append(f"declared shared lifecycle {sorted(shared)} shares no process")
    return problems
