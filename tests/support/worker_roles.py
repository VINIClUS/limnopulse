"""Load and validate `ops/workers/roles.yaml`, the ADR-020 normative role table."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
ROLES_PATH = ROOT / "ops/workers/roles.yaml"
OPENTOFU_DIR = ROOT / "infra/opentofu"
PRODUCTION_COMPOSE = ROOT / "compose.production.yaml"

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
EXPECTED_PATHS = {
    "workers_root": "/opt/limnopulse/workers",
    "releases": "/opt/limnopulse/workers/releases",
    "current": "/opt/limnopulse/workers/current",
    "state": "/opt/limnopulse/workers/state",
    "config": "/etc/limnopulse/workers",
    "credentials": "/etc/limnopulse/workers/credentials",
    "credential_source": "/etc/limnopulse/workers/credentials/{role}/{credential}",
    "units": "/etc/systemd/system",
}
EXPECTED_BUNDLE_FILES = {
    "format": 1,
    "checksum_manifest": "SHA256SUMS",
    "metadata": "bundle.env",
    "role_table": "roles.json",
    "binaries": "bin",
    "units": "systemd",
}
ACTIVATION_EXIT_CODES = {"success": 0, "failure": 1, "floor_refused": 3, "usage": 78}
FORCED_COMMAND = "^main-[0-9a-f]{40} [0-9a-f]{64}$"
GO_UNIT_ENVIRONMENT = {"GOTRACEBACK": "crash"}
SES_CONSUMERS = frozenset({"email-worker", "ses-feedback-worker"})
SES_ROUTING_ALARM = {
    "name": "ses-events-routing-dlq",
    "queue": "ses_events_routing_dlq",
    "gate_variable": "email_delivery",
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
        "restart",
        "liveness_file",
        "healthcheck",
        "configuration_error",
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


def load_credential_lines(table: dict[str, Any], role: dict[str, Any]) -> list[str]:
    """Render a unit role's `LoadCredential=` values from the table's credential source."""
    source = table["paths"]["credential_source"]
    return [
        f"{credential}:{source.format(role=role['name'], credential=credential)}"
        for credential in role["credentials"]
    ]


def opentofu_names(directory: Path = OPENTOFU_DIR) -> tuple[set[str], set[str]]:
    """Return the SQS queue resource names and the variable names OpenTofu declares."""
    source = "\n".join(path.read_text(encoding="utf-8") for path in directory.glob("*.tf"))
    queues = set(re.findall(r'^resource "aws_sqs_queue" "(\w+)"', source, flags=re.MULTILINE))
    variables = set(re.findall(r'^variable "(\w+)"', source, flags=re.MULTILINE))
    return queues, variables


def compose_services(path: Path = PRODUCTION_COMPOSE) -> dict[str, dict[str, Any]]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["services"]


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
    problems += _validate_queues(table, declared)
    problems += _validate_compose(table["roles"])
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
    if health.get("treat_missing_data") != "breaching":
        problems.append("unit_health alarms must treat missing data as breaching")
    stale = health.get("oneshot_stale_after_intervals")
    if type(stale) is not int or stale < 1:
        problems.append("unit_health oneshot_stale_after_intervals must be a positive integer")
    if health.get("oneshot_success_exit_codes") != [EXIT_CODES["success"]]:
        problems.append("unit_health counts only exit 0 as a successful one-shot run")
    if table.get("paths") != EXPECTED_PATHS:
        problems.append(f"paths must be exactly {EXPECTED_PATHS}")
    bundle = table.get("bundle", {})
    if {key: bundle.get(key) for key in EXPECTED_BUNDLE_FILES} != EXPECTED_BUNDLE_FILES:
        problems.append(f"bundle layout must be {EXPECTED_BUNDLE_FILES}")
    activation = table.get("activation", {})
    if activation.get("exit_codes") != ACTIVATION_EXIT_CODES:
        problems.append(f"activation exit_codes must be exactly {ACTIVATION_EXIT_CODES}")
    expected_sudoers = (
        f"limnopulse-deploy ALL=(root) NOPASSWD: {activation.get('helper')}"
    )
    if activation.get("sudoers") != expected_sudoers:
        problems.append(f"activation sudoers must be exactly {expected_sudoers!r}")
    if table.get("deploy", {}).get("forced_command") != FORCED_COMMAND:
        problems.append(f"deploy forced_command must be exactly {FORCED_COMMAND!r}")
    if table.get("go_unit_environment") != GO_UNIT_ENVIRONMENT:
        problems.append(f"go_unit_environment must be exactly {GO_UNIT_ENVIRONMENT}")
    alarms = table.get("standalone_alarms", [])
    if SES_ROUTING_ALARM not in alarms:
        problems.append(f"the SES routing DLQ needs its own standalone alarm: {SES_ROUTING_ALARM}")
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
        if SES_CONSUMERS <= set(roles):
            problems.append(f"the SES consumers never share a lifecycle: {roles}")
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
        if role["restart"] != "unless-stopped":
            problems.append(f"{name}: a Compose worker restarts unless-stopped")
        if role["configuration_error"] != "park":
            problems.append(f"{name}: a Compose worker parks after a configuration error")
        if role["healthcheck"] != f"{role['entrypoint']} healthcheck":
            problems.append(f"{name}: healthcheck must be {role['entrypoint']} healthcheck")
        if not str(role["liveness_file"]).startswith("/tmp/"):
            problems.append(f"{name}: liveness_file lives under the container's /tmp")
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


def _validate_queues(table: dict[str, Any], declared: list[frozenset[str]]) -> list[str]:
    problems: list[str] = []
    queues, variables = opentofu_names()
    consumers: dict[str, set[str]] = {}
    for role in table["roles"]:
        name = role.get("name")
        for queue in role.get("queues", []):
            if queue.get("queue") not in queues:
                problems.append(f"{name}: queue {queue.get('queue')} is not an OpenTofu SQS queue")
            if queue.get("queue") == SES_ROUTING_ALARM["queue"]:
                problems.append(f"{name}: no role reads the SES routing DLQ")
            if queue.get("access") == "consume":
                consumers.setdefault(queue.get("queue"), set()).add(name)
        if role.get("lifecycle") == "compose" and role.get("enable_variable") not in variables:
            problems.append(
                f"{name}: enable_variable {role.get('enable_variable')} is not an OpenTofu variable"
            )
    for queue, members in consumers.items():
        if len(members) > 1 and frozenset(members) not in declared:
            problems.append(f"queue {queue} has more than one consumer role: {sorted(members)}")
    for alarm in table.get("standalone_alarms", []):
        if alarm.get("queue") not in queues:
            problems.append(f"{alarm.get('name')}: queue {alarm.get('queue')} is not an OpenTofu SQS queue")
        if alarm.get("gate_variable") not in variables:
            problems.append(
                f"{alarm.get('name')}: gate_variable {alarm.get('gate_variable')} is not an OpenTofu variable"
            )
    return problems


def _validate_compose(roles: list[dict[str, Any]]) -> list[str]:
    problems: list[str] = []
    services = compose_services()
    for role in roles:
        if role.get("lifecycle") != "compose":
            continue
        name, service = role.get("name"), services.get(role.get("compose_service"))
        if service is None:
            problems.append(
                f"{name}: compose_service {role.get('compose_service')} is not in compose.production.yaml"
            )
        elif role.get("compose_profile") not in service.get("profiles", []):
            problems.append(
                f"{name}: compose.production.yaml does not put it in profile {role.get('compose_profile')}"
            )
    return problems
