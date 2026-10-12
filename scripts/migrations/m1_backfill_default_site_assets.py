"""Backfill default Sites, Assets and legacy Device projections for explicit tenants (M1-06).

Dry-run is the default; `--apply` writes. There is no `--all` and no tenant discovery: every
tenant comes from a repeatable `--tenant` or a `--tenant-file` (one ID per line). Prints ONE
JSON object with a stable key order on stdout.

Exit codes: 0 every scoped tenant completed; 2 schema/ownership conflict; 1 configuration,
DynamoDB, deadline or unclassified failure (1 takes precedence over 2).

Run from the repository root:
    python -m scripts.migrations.m1_backfill_default_site_assets --tenant tnt_x \
        --page-size 25 --deadline-seconds 300 [--apply]
"""

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, NoReturn, Protocol

import boto3

from limnopulse_api.core.config import Settings
from limnopulse_api.migrations.default_site_asset import (
    MAX_DEADLINE_SECONDS,
    MAX_PAGE_SIZE,
    MIN_DEADLINE_SECONDS,
    MIN_PAGE_SIZE,
    DefaultSiteAssetMigrator,
    MigrationSummary,
    empty_summary,
    exit_code_for,
    normalize_tenant_ids,
)

TENANT_SCOPE_REQUIRED = "--tenant or --tenant-file is required"


class Migrator(Protocol):
    def run(
        self, tenants: Sequence[str], *, apply: bool, page_size: int, deadline_seconds: int
    ) -> MigrationSummary: ...


class _UsageError(Exception):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    """Usage errors exit 1 (configuration failure), never argparse's default 2."""

    def error(self, message: str) -> NoReturn:
        raise _UsageError(message)


def _bounded_int(minimum: int, maximum: int) -> Callable[[str], int]:
    def parse(value: str) -> int:
        try:
            parsed = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"must be an integer in {minimum}..{maximum}")
        if not minimum <= parsed <= maximum:
            raise argparse.ArgumentTypeError(f"must be an integer in {minimum}..{maximum}")
        return parsed

    return parse


def build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="python -m scripts.migrations.m1_backfill_default_site_assets",
        description=(
            "Backfill deterministic default Site/Asset and legacy Device projections for "
            "explicitly named tenants. Dry-run unless --apply is given."
        ),
    )
    parser.add_argument("--tenant", action="append", default=[], help="repeatable tenant ID")
    parser.add_argument("--tenant-file", type=Path, help="file with one tenant ID per line")
    parser.add_argument("--apply", action="store_true", help="write; default is dry-run")
    parser.add_argument("--page-size", type=_bounded_int(MIN_PAGE_SIZE, MAX_PAGE_SIZE), default=25)
    parser.add_argument(
        "--deadline-seconds",
        type=_bounded_int(MIN_DEADLINE_SECONDS, MAX_DEADLINE_SECONDS),
        default=300,
    )
    return parser


def read_tenant_file(path: Path) -> list[str]:
    """One tenant per line; blank lines, comments, NUL bytes and '#' are rejected."""
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise _UsageError(f"cannot read --tenant-file: {type(exc).__name__}") from exc
    lines = content.splitlines()
    if not lines:
        raise _UsageError("--tenant-file is empty")
    return lines


def resolve_tenants(tenants: Sequence[str], tenant_file: Path | None) -> list[str]:
    values = list(tenants)
    if tenant_file is not None:
        values.extend(read_tenant_file(tenant_file))
    if not values:
        raise _UsageError(TENANT_SCOPE_REQUIRED)
    try:
        return normalize_tenant_ids(values)
    except ValueError as exc:
        raise _UsageError(str(exc)) from exc


def dynamodb_client_kwargs(settings: Settings) -> dict[str, str]:
    """Mirror of the application's DynamoDB client construction (without importing the app)."""
    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if settings.dynamodb_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.dynamodb_endpoint_url
    if settings.app_env in {"local", "test"} and settings.dynamodb_endpoint_url is not None:
        kwargs["aws_access_key_id"] = "local"
        kwargs["aws_secret_access_key"] = "local"
    return kwargs


def build_migrator() -> Migrator:
    settings = Settings()
    client: Any = boto3.client("dynamodb", **dynamodb_client_kwargs(settings))
    return DefaultSiteAssetMigrator(client, table_name=settings.dynamodb_domain_table)


def _emit(summary: MigrationSummary) -> int:
    sys.stdout.write(json.dumps(summary.model_dump(mode="json")) + "\n")
    return exit_code_for(summary.result)


def main(
    argv: Sequence[str] | None = None,
    *,
    migrator_factory: Callable[[], Migrator] | None = None,
) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        tenants = resolve_tenants(args.tenant, args.tenant_file)
    except _UsageError as exc:
        sys.stderr.write(f"{parser.prog}: error: {exc}\n")
        return 1

    factory = migrator_factory or build_migrator
    try:
        migrator = factory()
    except Exception as exc:  # noqa: BLE001 - configuration failures map to exit 1.
        sys.stderr.write(f"configuration error: {type(exc).__name__}\n")
        return _emit(
            empty_summary(
                apply=args.apply, tenants_requested=len(tenants), result="configuration_error"
            )
        )
    summary = migrator.run(
        tenants,
        apply=args.apply,
        page_size=args.page_size,
        deadline_seconds=args.deadline_seconds,
    )
    return _emit(summary)


if __name__ == "__main__":
    sys.exit(main())
