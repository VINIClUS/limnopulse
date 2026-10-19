"""P2-08 rollout switches: defaults, env names, the `/v2` telemetry validator and their docs."""

import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from limnopulse_api.core.config import Settings

ROOT = Path(__file__).resolve().parents[2]
ENV_EXAMPLE = ROOT / ".env.example"
README = ROOT / "README.md"
ARCHITECTURE = ROOT / "docs/architecture.md"

# env name -> (settings field, default as written in `.env.example`, default value)
P2_SWITCHES = {
    "TELEMETRY_V2_WRITE_ENABLED": ("telemetry_v2_write_enabled", "false", False),
    "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED": ("telemetry_legacy_dual_write_enabled", "false", False),
    "API_V2_TELEMETRY_ENABLED": ("api_v2_telemetry_enabled", "false", False),
    "V1_TELEMETRY_READ_SOURCE": ("v1_telemetry_read_source", "legacy", "legacy"),
}
FLAG_ENVIRONMENT = (*P2_SWITCHES, "API_V2_ENABLED", "V1_POND_DEVICE_PROJECTION_ENABLED")
DEV_CLI = "scripts/dev/ingest_reading_v2.py"
REMOVED_README_SENTENCE = "There are no `/v2` telemetry"
KEPT_README_GUARANTEE = "does not change any `/v1` path"
P2_HEADING = r"^(?P<level>#{2,3}) .*\b(?:P2|Phase 2)\b.*$"
HEADING = r"^#{2,3} .+$"
DRY_RUN = re.compile(r"dry[- ]run")
# Top-level headings of `docs/architecture.md` on main before P2-08; P2-08 only adds a section.
ARCHITECTURE_HEADINGS_BEFORE_P2_08 = (
    "## V4 architecture baseline",
    "## Delivery status",
    "## System view",
    "## Canonical names",
    "## M1 v2 domain foundation (behind disabled rollout flags)",
    "## Authentication and tenant authorization",
    "## Phase 3A: Alert Rule configuration",
    "## Phase 3B: evaluation",
    "## Phase 3C-A: email delivery",
    "## Phase 3C-B: Telegram binding and delivery",
    "## Security boundaries",
)


@pytest.fixture(autouse=True)
def _isolate_flag_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in FLAG_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)


def test_p2_switches_default_off() -> None:
    settings = _settings()

    for field, _, default in P2_SWITCHES.values():
        assert getattr(settings, field) == default, field
    assert settings.telemetry_v2_write_enabled is False
    assert settings.telemetry_legacy_dual_write_enabled is False
    assert settings.api_v2_telemetry_enabled is False


@pytest.mark.parametrize(
    ("env_name", "raw", "expected"),
    [
        ("TELEMETRY_V2_WRITE_ENABLED", "true", True),
        ("TELEMETRY_LEGACY_DUAL_WRITE_ENABLED", "true", True),
        ("V1_TELEMETRY_READ_SOURCE", "v2", "v2"),
        ("V1_TELEMETRY_READ_SOURCE", "legacy", "legacy"),
    ],
)
def test_switch_reads_its_exact_env_name(
    monkeypatch: pytest.MonkeyPatch, env_name: str, raw: str, expected: object
) -> None:
    monkeypatch.setenv(env_name, raw)

    settings = _settings()

    field = P2_SWITCHES[env_name][0]
    assert getattr(settings, field) == expected
    for other_name, (other_field, _, default) in P2_SWITCHES.items():
        if other_name != env_name:
            assert getattr(settings, other_field) == default, other_field


def test_api_v2_telemetry_reads_its_env_name_with_api_v2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_V2_ENABLED", "true")
    monkeypatch.setenv("API_V2_TELEMETRY_ENABLED", "true")

    settings = _settings()

    assert settings.api_v2_enabled is True
    assert settings.api_v2_telemetry_enabled is True


@pytest.mark.parametrize("value", ["V2", "Legacy", "projection", "", "v1"])
def test_v1_read_source_rejects_unknown_values(value: str) -> None:
    with pytest.raises(ValidationError):
        _settings(v1_telemetry_read_source=value)


def test_api_v2_telemetry_without_api_v2_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _settings(api_v2_telemetry_enabled=True)

    assert "telemetry" in str(exc_info.value).lower()


def test_api_v2_telemetry_env_without_api_v2_env_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_V2_TELEMETRY_ENABLED", "true")
    monkeypatch.setenv("API_V2_ENABLED", "false")

    with pytest.raises(ValidationError):
        _settings()


def test_settings_validation_errors_never_echo_secret_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "zq9x-influx-token-must-not-leak"
    # Pydantic would echo the head of the env-sourced input; make the token lead it.
    for field in Settings.model_fields:
        if field == "influxdb_token":
            break
        monkeypatch.delenv(field.upper(), raising=False)
    monkeypatch.setenv("INFLUXDB_TOKEN", secret)
    monkeypatch.setenv("API_V2_TELEMETRY_ENABLED", "true")

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    assert "zq9x" not in str(exc_info.value)
    assert "API_V2_TELEMETRY_ENABLED" in str(exc_info.value)


def test_api_v2_telemetry_with_api_v2_is_accepted() -> None:
    settings = _settings(api_v2_enabled=True, api_v2_telemetry_enabled=True)

    assert settings.api_v2_enabled is True
    assert settings.api_v2_telemetry_enabled is True


@pytest.mark.parametrize("api_v2_enabled", [False, True])
@pytest.mark.parametrize("v2_write", [False, True])
@pytest.mark.parametrize("legacy_write", [False, True])
@pytest.mark.parametrize("read_source", ["legacy", "v2"])
def test_write_and_read_switches_are_independent_of_each_other_and_of_api_v2(
    api_v2_enabled: bool, v2_write: bool, legacy_write: bool, read_source: str
) -> None:
    settings = _settings(
        api_v2_enabled=api_v2_enabled,
        telemetry_v2_write_enabled=v2_write,
        telemetry_legacy_dual_write_enabled=legacy_write,
        v1_telemetry_read_source=read_source,
    )

    assert settings.api_v2_enabled is api_v2_enabled
    assert settings.telemetry_v2_write_enabled is v2_write
    assert settings.telemetry_legacy_dual_write_enabled is legacy_write
    assert settings.v1_telemetry_read_source == read_source
    assert settings.api_v2_telemetry_enabled is False


def _env_example_assignments() -> dict[str, str]:
    assignments: dict[str, str] = {}
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        assert name not in assignments, f"duplicate .env.example entry: {name}"
        assignments[name] = value
    return assignments


def test_env_example_documents_every_switch_with_its_default() -> None:
    assignments = _env_example_assignments()

    for env_name, (_, documented_default, _) in P2_SWITCHES.items():
        assert assignments.get(env_name) == documented_default, env_name


def test_env_example_loads_with_every_switch_at_its_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    for name in _env_example_assignments():
        monkeypatch.delenv(name, raising=False)

    settings = Settings(_env_file=ENV_EXAMPLE)

    for field, _, default in P2_SWITCHES.values():
        assert getattr(settings, field) == default, field


def _mask_fenced_code(text: str) -> str:
    """Blank out fenced code blocks, keeping offsets, so `# ` lines inside are not headings."""
    masked: list[str] = []
    fence: str | None = None
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        marker = stripped[:3]
        opens_or_closes = marker in ("```", "~~~") and (fence is None or marker == fence)
        inside = fence is not None or opens_or_closes
        if opens_or_closes:
            fence = marker if fence is None else None
        body = line.rstrip("\r\n")
        masked.append(" " * len(body) + line[len(body) :] if inside else line)
    return "".join(masked)


def _headings(text: str) -> list[str]:
    return re.findall(HEADING, _mask_fenced_code(text), flags=re.MULTILINE)


def _section_span(text: str) -> tuple[int, int]:
    """The first P2 section: its heading through the next heading of the same or higher level."""
    searchable = _mask_fenced_code(text)
    match = re.search(P2_HEADING, searchable, flags=re.MULTILINE)
    assert match is not None, f"missing section heading matching {P2_HEADING!r}"
    level = len(match.group("level"))
    following = re.search(rf"^#{{1,{level}}} ", searchable[match.end() :], flags=re.MULTILINE)
    end = match.end() + following.start() if following else len(text)
    return match.start(), end


def _section(text: str) -> str:
    start, end = _section_span(text)
    return text[start:end]


def _assert_documents_p2_rollout(section: str) -> None:
    for env_name in P2_SWITCHES:
        assert env_name in section, env_name
    assert DEV_CLI in section
    assert "D16" in section
    assert "ships dark" in section.lower()
    assert "rollback" in section.lower()
    assert "restart" in section.lower()
    assert "ALERT_EVALUATOR_TELEMETRY_SOURCE" in section


def test_readme_replaces_the_no_v2_telemetry_sentence() -> None:
    readme = README.read_text(encoding="utf-8")

    assert REMOVED_README_SENTENCE not in readme
    assert KEPT_README_GUARANTEE in readme
    assert "API_V2_TELEMETRY_ENABLED" in readme


def test_readme_p2_section_documents_switches_cli_d16_and_rollback() -> None:
    section = _section(README.read_text(encoding="utf-8"))

    _assert_documents_p2_rollout(section)


def test_readme_p2_section_documents_the_both_switches_off_dry_run() -> None:
    section = " ".join(_section(README.read_text(encoding="utf-8")).split()).lower()

    assert DRY_RUN.search(section), "the README P2 section must describe the CLI dry run"


def test_section_search_ignores_headings_inside_fenced_code() -> None:
    text = (
        "## Intro\n\n```bash\n## P2 not a heading\n# comment\n```\n\n"
        "## P2 telemetry\n\nbody\n\n```sh\n# still the P2 body\n```\n\nmore\n\n## Next\n"
    )

    section = _section(text)

    assert section.startswith("## P2 telemetry")
    assert "# still the P2 body" in section and "more" in section
    assert "## Next" not in section
    assert _headings(text) == ["## Intro", "## P2 telemetry", "## Next"]


def test_architecture_adds_a_p2_section_and_keeps_its_pinned_header() -> None:
    architecture = ARCHITECTURE.read_text(encoding="utf-8")

    assert "**Version:** 1.4" in architecture
    assert "**Updated:** 2026-08-25" in architecture
    _assert_documents_p2_rollout(_section(architecture))


def test_architecture_keeps_every_existing_section_in_order() -> None:
    architecture = ARCHITECTURE.read_text(encoding="utf-8")
    headings = _headings(architecture)

    remaining = iter(headings)
    for expected in ARCHITECTURE_HEADINGS_BEFORE_P2_08:
        assert expected in remaining, f"existing section missing or reordered: {expected}"
    start, _ = _section_span(architecture)
    p2_heading = architecture[start:].splitlines()[0]
    assert p2_heading in headings
    assert p2_heading not in ARCHITECTURE_HEADINGS_BEFORE_P2_08
