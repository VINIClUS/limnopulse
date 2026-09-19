from datetime import UTC, datetime
from uuid import uuid4

from anyio import to_thread
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.api.v1.schemas.leads import LeadCreate


class DynamoLeadRepository:
    def __init__(self, table_name, client):
        self.table_name = table_name
        self.client = client

    async def create(self, lead: LeadCreate) -> str:
        now = datetime.now(UTC)
        lead_id = str(uuid4())
        item = {
            "PK": f"LEADS#{now:%Y-%m}",
            "SK": f"{now.isoformat()}#{lead_id}",
            "entity_type": "lead",
            "lead_id": lead_id,
            "created_at": now.isoformat(),
            **lead.model_dump(mode="json"),
        }

        def write():
            self.client.put_item(
                TableName=self.table_name,
                Item={k: TypeSerializer().serialize(v) for k, v in item.items()},
                ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)",
            )

        await to_thread.run_sync(write)
        return lead_id

    def iter_between(self, start: datetime, end: datetime):
        """UTC interval [start, end), partition queries only; stream every page."""
        if start.tzinfo is None or end.tzinfo is None or end <= start:
            raise ValueError("expected timezone-aware start < end")
        start, end = start.astimezone(UTC), end.astimezone(UTC)
        month = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        serializer, deserializer = TypeSerializer(), TypeDeserializer()
        while month < end:
            kwargs = {
                "TableName": self.table_name,
                "KeyConditionExpression": "PK = :pk AND SK BETWEEN :start AND :end",
                "ExpressionAttributeValues": {
                    k: serializer.serialize(v)
                    for k, v in {
                        ":pk": f"LEADS#{month:%Y-%m}",
                        ":start": start.isoformat(),
                        ":end": end.isoformat(),
                    }.items()
                },
            }
            while True:
                page = self.client.query(**kwargs)
                for raw in page.get("Items", []):
                    item = {k: deserializer.deserialize(v) for k, v in raw.items()}
                    if start <= datetime.fromisoformat(item["created_at"]) < end:
                        yield item
                if not page.get("LastEvaluatedKey"):
                    break
                kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
            month = (
                month.replace(year=month.year + 1, month=1)
                if month.month == 12
                else month.replace(month=month.month + 1)
            )
