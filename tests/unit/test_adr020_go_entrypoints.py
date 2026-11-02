"""ADR-020 S1: one Go executable per worker role, checked against ops/workers/roles.yaml."""

import re
from pathlib import Path

import pytest

from tests.support.worker_roles import load_roles, roles_by_name

ROOT = Path(__file__).resolve().parents[2]
MODULE = "github.com/VINIClUS/limnopulse/"
# The roles S1 gives an entrypoint; worker-monitor is S2's.
S1_ROLES = (
    "alert-evaluator",
    "notification-relay",
    "email-worker",
    "ses-feedback-worker",
    "telegram-worker",
    "limnopulse-admin",
)
ONESHOT_ROLES = ("alert-evaluator", "notification-relay")
CONTINUOUS_ROLES = ("email-worker", "ses-feedback-worker", "telegram-worker")
SES_PACKAGES = (
    "github.com/aws/aws-sdk-go-v2/service/sesv2",
    MODULE + "internal/notifications/worker/ses",
)
FEEDBACK_PACKAGE = MODULE + "internal/notifications/feedback"
IMPORT_BLOCK = re.compile(r"^import \((.*?)^\)", re.MULTILINE | re.DOTALL)
SINGLE_IMPORT = re.compile(r'^import (?:\w+ )?"([^"]+)"', re.MULTILINE)
QUOTED = re.compile(r'"([^"]+)"')


def _production_files(directory: Path) -> list[Path]:
    return sorted(
        path for path in directory.glob("*.go") if not path.name.endswith("_test.go")
    )


def _production_source(directory: Path) -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in _production_files(directory))


def _imports(directory: Path) -> set[str]:
    found: set[str] = set()
    for path in _production_files(directory):
        source = path.read_text(encoding="utf-8")
        for block in IMPORT_BLOCK.findall(source):
            found.update(QUOTED.findall(block))
        found.update(SINGLE_IMPORT.findall(source))
    return found


def _closure(entrypoint: str) -> set[str]:
    """Every package the entrypoint links, following this module's packages only."""
    seen: set[str] = set()
    pending = [MODULE + entrypoint]
    while pending:
        package = pending.pop()
        if package in seen:
            continue
        seen.add(package)
        if package.startswith(MODULE):
            pending.extend(_imports(ROOT / package.removeprefix(MODULE)))
    return seen


def _role(name: str) -> dict:
    return roles_by_name(load_roles())[name]


def _main(name: str) -> str:
    return (ROOT / "cmd" / name / "main.go").read_text(encoding="utf-8")


@pytest.mark.parametrize("name", S1_ROLES)
def test_each_role_has_its_own_executable(name: str) -> None:
    role = _role(name)
    assert role["entrypoint"] == f"cmd/{name}"
    main = _main(name)
    assert re.search(r"^package main$", main, re.MULTILINE)
    assert "func main() {" in main
    # The supervision moved to systemd: no role multiplexes consumers in one process.
    for directory in (ROOT / "cmd" / name, ROOT / "internal/notifications/worker/consumer"):
        source = _production_source(directory)
        assert "worker.Supervisor" not in source
        assert "Consumers:" not in source


@pytest.mark.parametrize("name", CONTINUOUS_ROLES)
def test_continuous_roles_run_under_workerproc_continuous_with_their_stall_limit(
    name: str,
) -> None:
    role = _role(name)
    assert role["lifecycle"] == "continuous"
    main = _main(name)
    assert f"const stallAfter = {role['stall_after_seconds']} * time.Second" in main
    assert "workerproc.Continuous{StallAfter: stallAfter}" in main
    assert "consumer.Role[" in main
    # Continuous owns SIGTERM/SIGINT; a second handler would race its stop.
    assert "signal.Notify" not in main
    consumer = _production_source(ROOT / "internal/notifications/worker/consumer")
    assert "supervisor.Run(ctx" in consumer
    assert "workercmd.AWSLoaderFromEnv(lookup)" in consumer


@pytest.mark.parametrize("name", ONESHOT_ROLES)
def test_oneshot_roles_have_no_loop_sleep_or_supervisor(name: str) -> None:
    assert _role(name)["lifecycle"] == "oneshot"
    main = _production_source(ROOT / "cmd" / name)
    for forbidden in ("for {", "time.Sleep", "workerproc.Continuous", "ReceiveMessage"):
        assert forbidden not in main
    assert "signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)" in main
    assert "workercmd.AWSLoaderFromEnv(deps.LookupEnv)" in main


def test_alert_evaluator_is_run_only_and_the_backfills_moved_to_the_admin() -> None:
    evaluator = _main("alert-evaluator")
    admin = _production_source(ROOT / "cmd" / "limnopulse-admin")
    assert 'args[0] != "run"' in evaluator
    for command in (
        "backfill-schedule",
        "backfill-active-alert-index",
        "backfill-relay",
        "backfill-telegram",
    ):
        assert f'"{command}"' not in evaluator
        assert f'case "{command}":' in admin
    assert _role("limnopulse-admin")["lifecycle"] == "cli"
    assert "workercmd.AWSLoaderFromEnv(deps.LookupEnv)" in admin


def test_secrets_come_from_their_credentials_when_loaded() -> None:
    assert "influxdb-token" in _role("alert-evaluator")["credentials"]
    assert "redis-password" in _role("telegram-worker")["credentials"]
    evaluator = _main("alert-evaluator")
    telegram = _main("telegram-worker")
    assert (
        "workercmd.SecretLookup(deps.LookupEnv, creds, workerproc.CredentialInfluxDBToken, "
        '"INFLUXDB_TOKEN")' in evaluator
    )
    assert "alertevaluator.LoadRunConfig(args[1:], lookup)" in evaluator
    assert (
        "Secret: &consumer.Secret{Credential: workerproc.CredentialRedisPassword, "
        'Key: "REDIS_PASSWORD"}' in telegram
    )
    # AWS clients load only through workercmd, which applies the LoadCredential= keys.
    for directory in [ROOT / "cmd" / name for name in S1_ROLES] + [
        ROOT / "internal/notifications/worker/compose",
        ROOT / "internal/notifications/feedback/compose",
        ROOT / "internal/notifications/relay/compose",
        ROOT / "internal/notifications/telegramworker/compose",
    ]:
        assert "LoadDefaultConfig" not in _production_source(directory), directory
    assert "creds.AWSLoadOptions()" in _production_source(ROOT / "internal/workercmd")


@pytest.mark.parametrize(
    "loader",
    (
        "internal/alertevaluator/config.go",
        "internal/notifications/relay/config/config.go",
        "internal/notifications/worker/config/config.go",
        "internal/notifications/feedback/config/config.go",
        "internal/notifications/telegramworker/config/config.go",
        "cmd/limnopulse-admin/main.go",
    ),
)
def test_flag_errors_are_configuration_errors_and_help_is_not(loader: str) -> None:
    source = (ROOT / loader).read_text(encoding="utf-8")
    assert "workerflag.Parse(fs, " in source
    assert "fs.Parse(" not in source
    assert "flag.ExitOnError" not in source
    workerflag = _production_source(ROOT / "internal/workerflag")
    assert "workerproc.ParseFlags(fs, args)" in workerflag
    assert "return &Help{Usage: usage.String()}" in workerflag
    # A help request prints the usage and exits 0; everything else fails through workercmd.
    assert "errors.As(err, &help)" in _production_source(ROOT / "internal/workercmd")


def test_admin_commands_parse_through_the_shared_helper() -> None:
    for name in ("notifications.go", "alerts.go"):
        source = (ROOT / "cmd/limnopulse-admin" / name).read_text(encoding="utf-8")
        assert "if err := parse(fs, args); err != nil {" in source
        assert "ParseFlags" not in source
        assert "fs.Parse(" not in source


def test_email_and_ses_feedback_are_separate_processes_with_separate_loaders() -> None:
    email = _main("email-worker")
    feedback = _main("ses-feedback-worker")
    assert "workerconfig.LoadEmail(args, lookup)" in email
    assert "Run: workercompose.Run" in email
    assert "feedbackconfig.Load(args, lookup)" in feedback
    assert "Run: feedbackcompose.Run" in feedback

    email_load = (ROOT / "internal/notifications/worker/config/config.go").read_text(
        encoding="utf-8"
    )
    assert 'return load("email-worker", args, lookup, false)' in email_load

    email_links = _closure("cmd/email-worker")
    feedback_links = _closure("cmd/ses-feedback-worker")
    assert not any(package.startswith(FEEDBACK_PACKAGE) for package in email_links)
    assert MODULE + "internal/notifications/feedback/compose" in feedback_links
    assert not set(SES_PACKAGES) & feedback_links
    assert MODULE + "internal/notifications/worker/compose" not in feedback_links


def test_telegram_worker_does_not_link_ses() -> None:
    links = _closure("cmd/telegram-worker")
    assert MODULE + "internal/notifications/telegramworker/compose" in links
    assert not set(SES_PACKAGES) & links
    assert set(SES_PACKAGES) <= _closure("cmd/email-worker")
