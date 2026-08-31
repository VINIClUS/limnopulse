import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GO_SCAN_CALL = re.compile(r"\.\s*Scan\s*\(")


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


def go_offenders(root: Path) -> list[str]:
    return [
        str(path.relative_to(ROOT))
        for path in root.rglob("*.go")
        if not path.name.endswith("_test.go")
        and GO_SCAN_CALL.search(path.read_text(encoding="utf-8"))
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


def test_no_dynamodb_scan_in_application_code() -> None:
    offenders = (
        python_offenders(ROOT / "src")
        + python_offenders(ROOT / "scripts")
        + go_offenders(ROOT / "cmd")
        + go_offenders(ROOT / "internal")
    )
    assert offenders == []
