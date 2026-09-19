from datetime import UTC, datetime
from io import StringIO

from boto3.dynamodb.types import TypeSerializer

from limnopulse_api.adapters.leads import DynamoLeadRepository
from scripts.admin.export_leads import export_csv


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
