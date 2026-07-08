from pathlib import Path


def test_no_dynamodb_scan_in_application_code() -> None:
    root = Path("src/limnopulse_api")
    offenders = []
    for path in root.rglob("*.py"):
        text = path.read_text()
        if ".scan(" in text or "scan(" in text:
            offenders.append(str(path))
    assert offenders == []
