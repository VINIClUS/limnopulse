import json
from pathlib import Path
from typing import Any

import pytest

from limnopulse_api.core.config import Settings
from limnopulse_api.migrations.default_site_asset import (
    MigrationConflict,
    MigrationSummary,
    empty_summary,
)
from scripts.migrations import m1_backfill_default_site_assets as cli
from scripts.migrations.m1_backfill_default_site_assets import main

SUMMARY_KEYS = [
    "apply",
    "tenants_requested",
    "tenants_completed",
    "default_sites_created",
    "assets_created",
    "devices_projected",
    "components_created",
    "deployments_created",
    "legacy_links_updated",
    "would_create_default_sites",
    "would_create_assets",
    "would_project_devices",
    "would_create_components",
    "would_create_deployments",
    "would_update_legacy_links",
    "already_migrated",
    "schema_conflicts",
    "conflicts",
    "scope_completed",
    "result",
]


class FakeMigrator:
    def __init__(self, result: str = "success") -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    def run(
        self, tenants: list[str], *, apply: bool, page_size: int, deadline_seconds: int
    ) -> MigrationSummary:
        self.calls.append(
            {
                "tenants": list(tenants),
                "apply": apply,
                "page_size": page_size,
                "deadline_seconds": deadline_seconds,
            }
        )
        summary = empty_summary(apply=apply, tenants_requested=len(tenants), result=self.result)
        if self.result == "schema_conflict":
            summary = summary.model_copy(
                update={
                    "schema_conflicts": 1,
                    "conflicts": [
                        MigrationConflict(
                            tenant_id=tenants[0], kind="asset", key="TENANT#t/ASSET#a"
                        )
                    ],
                }
            )
        return summary


@pytest.fixture
def migrator(monkeypatch: pytest.MonkeyPatch) -> FakeMigrator:
    fake = FakeMigrator()
    monkeypatch.setattr(cli, "build_migrator", lambda: fake)
    return fake


def invoked_tenants(fake: FakeMigrator) -> list[str]:
    assert len(fake.calls) == 1
    return fake.calls[0]["tenants"]


def test_cli_requires_explicit_tenant_scope(capsys, migrator: FakeMigrator) -> None:
    assert main([]) == 1
    captured = capsys.readouterr()
    assert "--tenant or --tenant-file is required" in captured.err
    assert captured.out == ""
    assert migrator.calls == []


def test_cli_has_no_all_or_discovery_flag(capsys, migrator: FakeMigrator) -> None:
    assert main(["--all"]) == 1
    assert "unrecognized arguments: --all" in capsys.readouterr().err
    assert migrator.calls == []


def test_cli_deduplicates_tenants_and_maps_conflict_to_exit_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    fake = FakeMigrator("schema_conflict")
    monkeypatch.setattr(cli, "build_migrator", lambda: fake)
    tenant_file = tmp_path / "tenants.txt"
    tenant_file.write_text("tnt_1\ntnt_1\n", encoding="utf-8")

    assert main(["--tenant-file", str(tenant_file), "--apply"]) == 2

    assert invoked_tenants(fake) == ["tnt_1"]
    output = json.loads(capsys.readouterr().out)
    assert output["conflicts"] == [
        {"tenant_id": "tnt_1", "kind": "asset", "key": "TENANT#t/ASSET#a"}
    ]


def test_cli_merges_flags_and_file_preserving_first_seen_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, migrator: FakeMigrator
) -> None:
    monkeypatch.chdir(tmp_path)
    tenant_file = tmp_path / "tenants.txt"
    tenant_file.write_text("tnt_c\r\ntnt_a\n", encoding="utf-8")

    assert main(["--tenant", "tnt_a", "--tenant", "tnt_b", "--tenant-file", str(tenant_file)]) == 0

    assert invoked_tenants(migrator) == ["tnt_a", "tnt_b", "tnt_c"]


def test_cli_defaults_to_dry_run_and_prints_one_stable_json_object(
    capsys, migrator: FakeMigrator
) -> None:
    assert main(["--tenant", "tnt_1"]) == 0

    assert migrator.calls == [
        {"tenants": ["tnt_1"], "apply": False, "page_size": 25, "deadline_seconds": 300}
    ]
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert list(json.loads(out)) == SUMMARY_KEYS
    assert json.loads(out)["apply"] is False


def test_cli_passes_apply_page_size_and_deadline(migrator: FakeMigrator) -> None:
    assert (
        main(["--tenant", "tnt_1", "--apply", "--page-size", "100", "--deadline-seconds", "1"]) == 0
    )
    assert migrator.calls[0] == {
        "tenants": ["tnt_1"],
        "apply": True,
        "page_size": 100,
        "deadline_seconds": 1,
    }


@pytest.mark.parametrize(
    "argv",
    [
        ["--page-size", "0"],
        ["--page-size", "101"],
        ["--page-size", "ten"],
        ["--deadline-seconds", "0"],
        ["--deadline-seconds", "3601"],
        ["--tenant", ""],
        ["--tenant", "tnt#1"],
        ["--tenant", "tnt\x001"],
    ],
)
def test_cli_usage_errors_exit_one_without_running(
    argv: list[str], capsys, migrator: FakeMigrator
) -> None:
    full = argv if "--tenant" in argv else ["--tenant", "tnt_1", *argv]
    assert main(full) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error" in captured.err
    assert migrator.calls == []


@pytest.mark.parametrize(
    "content",
    ["tnt_1\n\ntnt_2\n", "tnt_1\n   \n", "# comment\n", "tnt_1\x00\n", ""],
)
def test_cli_rejects_invalid_tenant_files(
    content: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
    migrator: FakeMigrator,
) -> None:
    monkeypatch.chdir(tmp_path)
    tenant_file = tmp_path / "tenants.txt"
    tenant_file.write_text(content, encoding="utf-8")

    assert main(["--tenant-file", str(tenant_file)]) == 1
    assert capsys.readouterr().out == ""
    assert migrator.calls == []


def test_cli_missing_tenant_file_exits_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, migrator: FakeMigrator
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["--tenant-file", str(tmp_path / "missing.txt")]) == 1
    assert "cannot read --tenant-file" in capsys.readouterr().err
    assert migrator.calls == []


@pytest.mark.parametrize(
    ("result", "code"),
    [
        ("success", 0),
        ("schema_conflict", 2),
        ("deadline_exceeded", 1),
        ("dynamodb_error", 1),
        ("failed", 1),
    ],
)
def test_cli_exit_code_follows_summary_result(
    result: str, code: int, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setattr(cli, "build_migrator", lambda: FakeMigrator(result))
    assert main(["--tenant", "tnt_1"]) == code
    assert json.loads(capsys.readouterr().out)["result"] == result


def test_cli_injected_factory_wins(capsys) -> None:
    fake = FakeMigrator()
    assert main(["--tenant", "tnt_1"], migrator_factory=lambda: fake) == 0
    assert invoked_tenants(fake) == ["tnt_1"]


def test_cli_configuration_failure_exits_one_with_json(capsys) -> None:
    def broken() -> FakeMigrator:
        raise RuntimeError("bad settings")

    assert main(["--tenant", "tnt_1", "--apply"], migrator_factory=broken) == 1
    captured = capsys.readouterr()
    output = json.loads(captured.out)
    assert output["result"] == "configuration_error"
    assert output["scope_completed"] is False
    assert output["tenants_requested"] == 1
    assert "configuration error: RuntimeError" in captured.err


def test_build_migrator_mirrors_application_client_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("AWS_REGION", "us-east-2")
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "http://localhost:8001")
    monkeypatch.setenv("DYNAMODB_DOMAIN_TABLE", "MigrationTable")
    monkeypatch.setattr(
        cli.boto3, "client", lambda service, **kwargs: calls.append((service, kwargs)) or object()
    )

    migrator = cli.build_migrator()

    assert calls == [
        (
            "dynamodb",
            {
                "region_name": "us-east-2",
                "endpoint_url": "http://localhost:8001",
                "aws_access_key_id": "local",
                "aws_secret_access_key": "local",
            },
        )
    ]
    assert migrator.store.table_name == "MigrationTable"


def test_production_client_kwargs_never_inject_local_credentials() -> None:
    settings = Settings.model_construct(
        app_env="prod", aws_region="sa-east-1", dynamodb_endpoint_url=None
    )
    assert cli.dynamodb_client_kwargs(settings) == {"region_name": "sa-east-1"}


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A working directory with a sibling directory outside it."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "tenants.txt").write_text("tnt_1\n", encoding="utf-8")
    monkeypatch.chdir(cwd)
    return cwd


def _assert_rejected(argv: list[str], capsys, migrator: FakeMigrator, message: str) -> None:
    assert main(argv) == 1
    captured = capsys.readouterr()
    assert message in captured.err
    assert captured.out == ""
    assert migrator.calls == []


def test_tenant_file_absolute_path_outside_cwd_is_rejected(
    workdir: Path, capsys, migrator: FakeMigrator
) -> None:
    outside = workdir.parent / "outside" / "tenants.txt"
    _assert_rejected(["--tenant-file", str(outside)], capsys, migrator, cli.TENANT_FILE_OUTSIDE_CWD)


def test_tenant_file_parent_traversal_is_rejected(
    workdir: Path, capsys, migrator: FakeMigrator
) -> None:
    _assert_rejected(
        ["--tenant-file", "../outside/tenants.txt"],
        capsys,
        migrator,
        cli.TENANT_FILE_OUTSIDE_CWD,
    )


def test_tenant_file_symlink_escaping_cwd_is_rejected(
    workdir: Path, capsys, migrator: FakeMigrator
) -> None:
    (workdir / "link.txt").symlink_to(workdir.parent / "outside" / "tenants.txt")
    _assert_rejected(["--tenant-file", "link.txt"], capsys, migrator, cli.TENANT_FILE_OUTSIDE_CWD)


def test_tenant_file_directory_is_rejected(workdir: Path, capsys, migrator: FakeMigrator) -> None:
    (workdir / "tenants").mkdir()
    _assert_rejected(["--tenant-file", "tenants"], capsys, migrator, "must be a regular file")


def test_tenant_file_larger_than_one_mib_is_rejected(
    workdir: Path, capsys, migrator: FakeMigrator
) -> None:
    (workdir / "big.txt").write_text("tnt_1\n" * 200_000, encoding="utf-8")
    _assert_rejected(["--tenant-file", "big.txt"], capsys, migrator, "at most 1 MiB")


def test_tenant_file_relative_inside_cwd_is_read(workdir: Path, migrator: FakeMigrator) -> None:
    (workdir / "scope").mkdir()
    (workdir / "scope" / "tenants.txt").write_text("tnt_1\ntnt_2\n", encoding="utf-8")
    (workdir / "inner-link.txt").symlink_to(workdir / "scope" / "tenants.txt")

    assert main(["--tenant-file", "scope/tenants.txt"]) == 0
    assert main(["--tenant-file", "inner-link.txt"]) == 0
    assert migrator.calls[0]["tenants"] == ["tnt_1", "tnt_2"]
    assert migrator.calls[1]["tenants"] == ["tnt_1", "tnt_2"]
