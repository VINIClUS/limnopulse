from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
import sys

import pytest
from boto3.dynamodb.types import TypeSerializer

from limnopulse_api.adapters.leads import DynamoLeadRepository
from scripts.admin import export_leads
from scripts.admin.export_leads import export_csv, validate_output_name


def test_export_queries_months_all_pages_and_escapes_spreadsheet_formulas():
    class Client:
        def __init__(self):
            self.calls = []

        def query(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return {
                    "Items": [],
                    "LastEvaluatedKey": {"PK": {"S": "LEADS#2026-01"}, "SK": {"S": "cursor"}},
                }
            item = {
                "created_at": "2026-01-31T23:00:00+00:00",
                "name": "=formula",
                "email": "a@example.com",
                "consent": True,
            }
            return (
                {"Items": [{k: TypeSerializer().serialize(v) for k, v in item.items()}]}
                if len(self.calls) == 2
                else {"Items": []}
            )

    client = Client()
    output = StringIO()
    export_csv(
        DynamoLeadRepository("Domain", client),
        datetime(2026, 1, 31, tzinfo=UTC),
        datetime(2026, 2, 2, tzinfo=UTC),
        output,
    )
    assert len(client.calls) == 3
    assert "ExclusiveStartKey" in client.calls[1]
    assert client.calls[2]["ExpressionAttributeValues"][":pk"] == {"S": "LEADS#2026-02"}
    assert "'=formula" in output.getvalue()
    assert "a@example.com" in output.getvalue()


@pytest.mark.parametrize("value", ["../leads.csv", "/tmp/leads.csv", "..\\leads.csv", "."])
def test_output_name_rejects_paths(value):
    with pytest.raises(ValueError, match="simple filename"):
        validate_output_name(value)


def test_main_creates_exclusive_csv_in_current_directory(monkeypatch, tmp_path: Path):
    class Repository:
        def __init__(self, table, client):
            assert table == "TestDomain"
            assert client is not None

        def iter_between(self, start, end):
            assert start == datetime(2026, 1, 1, tzinfo=UTC)
            assert end == datetime(2026, 2, 2, tzinfo=UTC)
            return iter([{"lead_id": "lead_1", "email": "a@example.com"}])

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(export_leads, "DynamoLeadRepository", Repository)
    monkeypatch.setattr(export_leads.boto3, "client", lambda *args, **kwargs: object())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export_leads.py",
            "--start",
            "2026-01-01",
            "--end",
            "2026-02-01",
            "--table",
            "TestDomain",
            "--output",
            "leads.csv",
        ],
    )

    export_leads.main()

    assert (tmp_path / "leads.csv").read_text(encoding="utf-8-sig").count("lead_1") == 1
    with pytest.raises(FileExistsError):
        export_leads.main()


def test_main_rejects_output_path_before_constructing_repository(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export_leads.py",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-01",
            "--output",
            "../leads.csv",
        ],
    )
    with pytest.raises(SystemExit):
        export_leads.main()
