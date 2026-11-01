"""ADR-020 S0: `ops/workers/roles.yaml` is the normative worker role table."""

from __future__ import annotations

import copy
import re

import pytest

from tests.support.worker_roles import (
    ADR_ROLES,
    EXIT_CODES,
    FORCED_COMMAND,
    ROOT,
    load_credential_lines,
    load_roles,
    roles_by_name,
    validate_roles,
)

WORKERPROC = ROOT / "internal/workerproc"


@pytest.fixture
def table():
    return load_roles()


def problems_after(table, mutate) -> list[str]:
    mutated = copy.deepcopy(table)
    mutate(mutated)
    return validate_roles(mutated)


def test_roles_table_is_valid(table):
    assert validate_roles(table) == []


def test_every_adr_role_is_listed_once_in_adr_order(table):
    assert tuple(role["name"] for role in table["roles"]) == ADR_ROLES


def test_no_shared_lifecycle_exists_today(table):
    assert table["shared_lifecycles"] == []


def test_the_ses_consumers_are_separate_processes_principals_and_alarms(table):
    roles = roles_by_name(table)
    email, feedback = roles["email-worker"], roles["ses-feedback-worker"]
    assert email["units"]["service"] != feedback["units"]["service"]
    assert email["iam_principal"] != feedback["iam_principal"]
    assert {queue["queue"] for queue in email["queues"]} == {"notification_jobs"}
    assert {queue["queue"] for queue in feedback["queues"]} == {"ses_events"}
    for role in (email, feedback):
        assert {"unit-health", "queue-age", "queue-depth", "queue-dlq"} <= set(role["alarms"])
    assert feedback["single_instance"] is True
    assert table["standalone_alarms"] == [
        {
            "name": "ses-events-routing-dlq",
            "queue": "ses_events_routing_dlq",
            "gate_variable": "email_delivery",
        }
    ]


def test_each_unit_loads_credentials_from_its_own_directory(table):
    roles = roles_by_name(table)
    assert load_credential_lines(table, roles["email-worker"]) == [
        "aws-access-key-id:/etc/limnopulse/workers/credentials/email-worker/aws-access-key-id",
        "aws-secret-access-key:/etc/limnopulse/workers/credentials/email-worker/aws-secret-access-key",
    ]
    sources = [
        line.split(":", 1)[1]
        for role in table["roles"]
        if "credentials" in role
        for line in load_credential_lines(table, role)
    ]
    assert len(sources) == len(set(sources))


def test_the_forced_command_accepts_only_a_full_tag_and_bundle_digest():
    forced = re.compile(FORCED_COMMAND)
    assert forced.match(f"main-{'a' * 40} {'0' * 64}")
    for refused in (
        f"main-{'a' * 7}",
        f"main-{'a' * 40}",
        f"main-{'a' * 40} {'0' * 63}",
        f"main-{'A' * 40} {'0' * 64}",
        f"main-{'a' * 40} {'0' * 64}; true",
    ):
        assert not forced.match(refused), refused


def test_the_evaluator_and_relay_are_timer_driven_one_shots(table):
    roles = roles_by_name(table)
    for name in ("alert-evaluator", "notification-relay"):
        role = roles[name]
        assert role["lifecycle"] == "oneshot"
        assert role["units"]["timer"] == f"limnopulse-{name}.timer"
        assert role["schedule_seconds"] == 60


def test_the_deploy_starts_the_ingest_profile(table):
    ingest = roles_by_name(table)["telemetry-ingest"]
    assert ingest["compose_profile"] in table["deploy"]["compose_profiles"]


def test_the_ingest_worker_parks_on_configuration_errors(table):
    ingest = roles_by_name(table)["telemetry-ingest"]
    assert ingest["restart"] == "unless-stopped"
    assert ingest["configuration_error"] == "park"
    assert ingest["healthcheck"] == f"{ingest['entrypoint']} healthcheck"


@pytest.mark.parametrize(
    ("role", "field"),
    (
        ("email-worker", "iam_principal"),
        ("email-worker", "credentials"),
        ("alert-evaluator", "units"),
        ("worker-monitor", "enable_variable"),
        ("telemetry-ingest", "unit_health_reason"),
        ("telemetry-ingest", "liveness_file"),
        ("limnopulse-admin", "rollback"),
    ),
)
def test_a_missing_field_is_rejected(table, role, field):
    def drop(mutated):
        del roles_by_name(mutated)[role][field]

    assert f"{role}: missing field {field}" in problems_after(table, drop)


def test_an_undeclared_shared_lifecycle_is_rejected(table):
    def share(mutated):
        roles = roles_by_name(mutated)
        roles["ses-feedback-worker"]["units"]["service"] = roles["email-worker"]["units"][
            "service"
        ]

    assert any(
        problem.startswith("undeclared shared lifecycle") for problem in problems_after(table, share)
    )


def share_email_and_telegram(mutated, justification: str) -> None:
    roles = roles_by_name(mutated)
    roles["telegram-worker"]["units"]["service"] = roles["email-worker"]["units"]["service"]
    mutated["shared_lifecycles"] = [
        {"roles": ["email-worker", "telegram-worker"], "justification": justification}
    ]


def test_a_declared_shared_lifecycle_needs_a_justification(table):
    problems = problems_after(table, lambda t: share_email_and_telegram(t, " "))
    assert "shared lifecycle ['email-worker', 'telegram-worker'] has no justification" in (
        problems
    )
    assert not any(problem.startswith("undeclared shared lifecycle") for problem in problems)


def test_a_justified_shared_lifecycle_is_accepted(table):
    def share(mutated):
        share_email_and_telegram(mutated, "test fixture: one process serves both lanes")

    assert problems_after(table, share) == []


def test_the_ses_consumers_cannot_declare_a_shared_lifecycle(table):
    def share(mutated):
        roles = roles_by_name(mutated)
        roles["ses-feedback-worker"]["units"]["service"] = roles["email-worker"]["units"][
            "service"
        ]
        mutated["shared_lifecycles"] = [
            {
                "roles": ["email-worker", "ses-feedback-worker"],
                "justification": "test fixture: one process serves both queues",
            }
        ]

    assert any(
        problem.startswith("the SES consumers never share a lifecycle")
        for problem in problems_after(table, share)
    )


def test_a_declared_lifecycle_that_shares_nothing_is_rejected(table):
    def declare(mutated):
        mutated["shared_lifecycles"] = [
            {"roles": ["email-worker", "telegram-worker"], "justification": "stale entry"}
        ]

    assert "declared shared lifecycle ['email-worker', 'telegram-worker'] shares no process" in (
        problems_after(table, declare)
    )


def test_a_duplicate_unit_is_rejected(table):
    def duplicate(mutated):
        roles = roles_by_name(mutated)
        roles["worker-monitor"]["units"]["timer"] = roles["alert-evaluator"]["units"]["timer"]

    assert "duplicate unit: limnopulse-alert-evaluator.timer" in problems_after(table, duplicate)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    (
        (
            lambda t: t["exit_codes"].update(configuration_error=2),
            "exit_codes must be exactly",
        ),
        (
            lambda t: roles_by_name(t)["email-worker"].update(stall_after_seconds=120),
            "email-worker: the stall exit must fire before the systemd watchdog",
        ),
        (
            lambda t: roles_by_name(t)["alert-evaluator"].update(timeout_start_seconds=60),
            "alert-evaluator: a one-shot run must end before the next tick",
        ),
        (
            lambda t: roles_by_name(t)["telegram-worker"].update(iam_principal="email_worker"),
            "duplicate iam_principal: email_worker",
        ),
        (
            lambda t: roles_by_name(t)["telemetry-ingest"].update(unit_health="systemd"),
            "telemetry-ingest: a compose role publishes no unit health",
        ),
        (
            lambda t: roles_by_name(t)["ses-feedback-worker"].update(single_instance=False),
            "ses-feedback-worker runs as a single instance",
        ),
        (
            lambda t: roles_by_name(t)["ses-feedback-worker"]["rollback"].update(floor=2),
            "ses-feedback-worker: rollback needs integers with 0 <= floor <= epoch >= 1",
        ),
        (
            lambda t: t["deploy"].update(compose_profiles=[]),
            "telemetry-ingest: its Compose profile must be in the deploy profile list",
        ),
        (
            lambda t: t.update(standalone_alarms=[]),
            "the SES routing DLQ needs its own standalone alarm",
        ),
        (
            lambda t: t["standalone_alarms"][0].update(gate_variable="telegram_delivery"),
            "the SES routing DLQ needs its own standalone alarm",
        ),
        (
            lambda t: t["standalone_alarms"][0].update(queue="ses_events_dlq"),
            "the SES routing DLQ needs its own standalone alarm",
        ),
        (
            lambda t: roles_by_name(t)["ses-feedback-worker"]["queues"].append(
                {"queue": "ses_events_routing_dlq", "access": "consume"}
            ),
            "ses-feedback-worker: no role reads the SES routing DLQ",
        ),
        (
            lambda t: roles_by_name(t)["ses-feedback-worker"]["queues"].append(
                {"queue": "notification_jobs", "access": "consume"}
            ),
            "queue notification_jobs has more than one consumer role",
        ),
        (
            lambda t: roles_by_name(t)["email-worker"]["queues"][0].update(queue="email_jobs"),
            "email-worker: queue email_jobs is not an OpenTofu SQS queue",
        ),
        (
            lambda t: roles_by_name(t)["telemetry-ingest"].update(enable_variable="ingest_on"),
            "telemetry-ingest: enable_variable ingest_on is not an OpenTofu variable",
        ),
        (
            lambda t: roles_by_name(t)["telemetry-ingest"].update(compose_service="ingest"),
            "telemetry-ingest: compose_service ingest is not in compose.production.yaml",
        ),
        (
            lambda t: roles_by_name(t)["telemetry-ingest"].update(compose_profile="workers"),
            "telemetry-ingest: compose.production.yaml does not put it in profile workers",
        ),
        (
            lambda t: roles_by_name(t)["telemetry-ingest"].update(restart="on-failure:5"),
            "telemetry-ingest: a Compose worker restarts unless-stopped",
        ),
        (
            lambda t: roles_by_name(t)["telemetry-ingest"].update(configuration_error="restart"),
            "telemetry-ingest: a Compose worker parks after a configuration error",
        ),
        (
            lambda t: t["unit_health"].update(treat_missing_data="notBreaching"),
            "unit_health alarms must treat missing data as breaching",
        ),
        (
            lambda t: t["unit_health"].update(oneshot_stale_after_intervals=0),
            "unit_health oneshot_stale_after_intervals must be a positive integer",
        ),
        (
            lambda t: t["unit_health"].update(oneshot_success_exit_codes=[0, 2]),
            "unit_health counts only exit 0 as a successful one-shot run",
        ),
        (
            lambda t: t["paths"].update(credential_source="/etc/limnopulse/workers/{credential}"),
            "paths must be exactly",
        ),
        (
            lambda t: t["bundle"].update(checksum_manifest="MD5SUMS"),
            "bundle layout must be",
        ),
        (
            lambda t: t["activation"]["exit_codes"].update(floor_refused=1),
            "activation exit_codes must be exactly",
        ),
        (
            lambda t: t["activation"].update(sudoers="limnopulse-deploy ALL=(ALL) NOPASSWD: ALL"),
            "activation sudoers must be exactly",
        ),
        (
            lambda t: t["deploy"].update(forced_command="^main-[0-9a-f]+$"),
            "deploy forced_command must be exactly",
        ),
        (
            lambda t: t.update(go_unit_environment={}),
            "go_unit_environment must be exactly",
        ),
    ),
)
def test_contract_violations_are_rejected(table, mutation, expected):
    problems = problems_after(table, mutation)
    assert any(problem.startswith(expected) for problem in problems), problems


def test_go_exit_codes_match_the_table():
    source = (WORKERPROC / "exitcode.go").read_text(encoding="utf-8")
    constants = dict(re.findall(r"^\s+(Exit\w+)\s+= (\d+)$", source, flags=re.MULTILINE))
    assert {name: int(value) for name, value in constants.items()} == {
        "ExitSuccess": EXIT_CODES["success"],
        "ExitFailure": EXIT_CODES["runtime_failure"],
        "ExitPartial": EXIT_CODES["partial_run"],
        "ExitConfiguration": EXIT_CODES["configuration_error"],
    }


def test_go_credential_names_cover_every_role_credential(table):
    source = (WORKERPROC / "credentials.go").read_text(encoding="utf-8")
    declared = set(re.findall(r'^\s+Credential\w+\s+= "([a-z0-9-]+)"$', source, flags=re.MULTILINE))
    used = {credential for role in table["roles"] for credential in role.get("credentials", [])}
    assert used == declared
