import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DYNAMODB_IMPORT_PATH = "github.com/aws/aws-sdk-go-v2/service/dynamodb"
DYNAMODB_IMPORT_MARKER = "__DYNAMODB_IMPORT__"
GO_SCAN_METHOD_CALL = re.compile(r"\.\s*Scan\s*\(")
GO_UNQUALIFIED_SCAN_PAGINATOR_CALL = re.compile(
    r"(?<![.A-Za-z0-9_])NewScanPaginator\s*\("
)
GO_TOKEN = re.compile(
    r"(?P<line_comment>//[^\n]*)"
    r"|(?P<block_comment>/\*.*?\*/)"
    r"|(?P<raw_string>`[^`]*`)"
    r'|(?P<string>"(?:\\.|[^"\\])*")'
    r"|(?P<rune>'(?:\\.|[^'\\])*')",
    re.DOTALL,
)
GO_IMPORT_BLOCK = re.compile(r"\bimport\s*\((?P<body>.*?)\)", re.DOTALL)
GO_DYNAMODB_IMPORT_SPEC = re.compile(
    rf"^\s*(?:(?P<alias>\.|[A-Za-z_][A-Za-z0-9_]*)\s+)?"
    rf"{DYNAMODB_IMPORT_MARKER}\s*$",
    re.MULTILINE,
)
GO_DYNAMODB_SINGLE_IMPORT = re.compile(
    rf"^\s*import\s+(?:(?P<alias>\.|[A-Za-z_][A-Za-z0-9_]*)\s+)?"
    rf"{DYNAMODB_IMPORT_MARKER}\s*$",
    re.MULTILINE,
)


def python_offenders(root: Path) -> list[str]:
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "scan"
            for node in ast.walk(tree)
        ):
            offenders.append(str(path.relative_to(ROOT)))
    return offenders


def _blank_go_token(value: str) -> str:
    return "".join("\n" if char == "\n" else " " for char in value)


def _go_import_structure(source: str) -> str:
    return GO_TOKEN.sub(
        lambda match: (
            DYNAMODB_IMPORT_MARKER
            if match.lastgroup in {"string", "raw_string"}
            and match.group()[1:-1] == DYNAMODB_IMPORT_PATH
            else _blank_go_token(match.group())
        ),
        source,
    )


def _dynamodb_import_aliases(source: str) -> set[str]:
    aliases: set[str] = set()

    def add_alias(match: re.Match[str]) -> None:
        alias = match.group("alias") or "dynamodb"
        if alias != "_":
            aliases.add(alias)

    for match in GO_DYNAMODB_SINGLE_IMPORT.finditer(source):
        add_alias(match)
    for block in GO_IMPORT_BLOCK.finditer(source):
        for match in GO_DYNAMODB_IMPORT_SPEC.finditer(block.group("body")):
            add_alias(match)
    return aliases


def _has_go_scan_call(source: str) -> bool:
    aliases = _dynamodb_import_aliases(_go_import_structure(source))
    code = GO_TOKEN.sub(lambda match: _blank_go_token(match.group()), source)
    qualified_paginator_call = any(
        re.search(rf"\b{re.escape(alias)}\s*\.\s*NewScanPaginator\s*\(", code)
        for alias in aliases - {"."}
    )
    return (
        bool(GO_SCAN_METHOD_CALL.search(code))
        or (
            "." in aliases
            and bool(GO_UNQUALIFIED_SCAN_PAGINATOR_CALL.search(code))
        )
        or qualified_paginator_call
    )


def go_offenders(root: Path) -> list[str]:
    return [
        str(path.relative_to(ROOT))
        for path in root.rglob("*.go")
        if not path.name.endswith("_test.go")
        and _has_go_scan_call(path.read_text(encoding="utf-8"))
    ]


def test_python_offenders_detect_only_scan_attribute_calls(tmp_path, monkeypatch) -> None:
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    root = tmp_path / "src"
    root.mkdir()
    (root / "offender.py").write_text("client . scan ({} )\n", encoding="utf-8")
    (root / "allowed.py").write_text("scan()\nclient.Scan()\n", encoding="utf-8")

    assert python_offenders(root) == ["src/offender.py"]


def test_go_offenders_detect_scan_calls_but_exclude_test_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    root = tmp_path / "internal"
    root.mkdir()
    source = "package store\nfunc f(client Client) { client . Scan (nil) }\n"
    (root / "store.go").write_text(source, encoding="utf-8")
    (root / "store_test.go").write_text(source, encoding="utf-8")

    assert go_offenders(root) == ["internal/store.go"]


def test_go_offenders_detect_scan_paginator_but_exclude_test_files(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    root = tmp_path / "internal"
    root.mkdir()
    source = (
        "package store\n"
        'import "github.com/aws/aws-sdk-go-v2/service/dynamodb"\n'
        "func f(client *dynamodb.Client) { dynamodb.NewScanPaginator(client, nil) }\n"
    )
    (root / "store.go").write_text(source, encoding="utf-8")
    (root / "store_test.go").write_text(source, encoding="utf-8")

    assert go_offenders(root) == ["internal/store.go"]


def test_go_offenders_detect_scan_paginator_import_alias_without_comments(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    root = tmp_path / "internal"
    root.mkdir()
    source = (
        "package store\n"
        "import (\n"
        '    ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"\n'
        ")\n"
        "func f(client *ddb.Client) { ddb.NewScanPaginator(client, nil) }\n"
    )
    allowed = (
        "package store\n"
        "import (\n"
        '    ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"\n'
        ")\n"
        "// ddb.NewScanPaginator(client, nil)\n"
        "/* ddb.NewScanPaginator(client, nil) */\n"
        'var example = "ddb.NewScanPaginator(client, nil)"\n'
    )
    (root / "store.go").write_text(source, encoding="utf-8")
    (root / "allowed.go").write_text(allowed, encoding="utf-8")
    (root / "store_test.go").write_text(source, encoding="utf-8")

    assert go_offenders(root) == ["internal/store.go"]


def test_go_offenders_detect_scan_paginator_dot_import(tmp_path, monkeypatch) -> None:
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    root = tmp_path / "internal"
    root.mkdir()
    source = (
        "package store\n"
        'import . "github.com/aws/aws-sdk-go-v2/service/dynamodb"\n'
        "func f(client *Client) { NewScanPaginator(client, nil) }\n"
    )
    (root / "store.go").write_text(source, encoding="utf-8")
    (root / "store_test.go").write_text(source, encoding="utf-8")

    assert go_offenders(root) == ["internal/store.go"]


def test_go_offenders_ignore_dynamodb_import_fabricated_by_raw_string(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    root = tmp_path / "internal"
    root.mkdir()
    source = (
        "package store\n"
        'import ddb "example.com/not-dynamodb"\n'
        "var example = `\n"
        'import ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"\n'
        "`\n"
        "func f() { ddb.NewScanPaginator(nil, nil) }\n"
    )
    (root / "allowed.go").write_text(source, encoding="utf-8")

    assert go_offenders(root) == []


def test_no_dynamodb_scan_in_application_code() -> None:
    offenders = (
        python_offenders(ROOT / "src")
        + python_offenders(ROOT / "scripts")
        + go_offenders(ROOT / "cmd")
        + go_offenders(ROOT / "internal")
    )
    assert offenders == []
