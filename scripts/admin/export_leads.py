"""Export persisted leads using AWS credentials; no public listing API."""

import argparse
import csv
from datetime import UTC, datetime, timedelta

import boto3

from limnopulse_api.adapters.leads import DynamoLeadRepository

FIELDS = ["lead_id", "created_at", "name", "email", "phone", "property_name", "source", "consent"]


def export_csv(repository, start, end, output):
    writer = csv.DictWriter(output, fieldnames=FIELDS)
    writer.writeheader()
    for item in repository.iter_between(start, end):
        # Prevent spreadsheet formula injection while preserving embedded newlines/commas.
        row = {}
        for field in FIELDS:
            value = item.get(field, "")
            if isinstance(value, str) and value.lstrip().startswith(
                ("=", "+", "-", "@", "\t", "\r", "\n")
            ):
                value = "'" + value
            row[field] = value
        writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Export leads. Dates inclusive, in UTC; AWS credential chain."
    )
    parser.add_argument(
        "--start",
        required=True,
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC),
    )
    parser.add_argument(
        "--end", required=True, type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)
    )
    parser.add_argument("--table", default="LimnopulseDomain")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--endpoint-url")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.end < args.start:
        parser.error("--end must be on or after --start")
    repo = DynamoLeadRepository(
        args.table,
        boto3.client("dynamodb", region_name=args.region, endpoint_url=args.endpoint_url),
    )
    # Exclusive creation avoids overwriting an existing contact export.
    with open(args.output, "x", newline="", encoding="utf-8-sig") as output:
        export_csv(repo, args.start, args.end + timedelta(days=1), output)


if __name__ == "__main__":
    main()
