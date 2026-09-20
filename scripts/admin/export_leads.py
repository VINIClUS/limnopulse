"""Export persisted leads using AWS credentials; no public listing API."""

import argparse
import csv
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3

from limnopulse_api.adapters.leads import DynamoLeadRepository

FIELDS = ["lead_id", "created_at", "name", "email", "phone", "property_name", "source", "consent"]


def validate_output_name(value: str) -> str:
    if (
        not value
        or Path(value).name != value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
    ):
        raise ValueError("--output must be a simple filename in the current directory")
    return value


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
    try:
        output_name = validate_output_name(args.output)
    except ValueError as exc:
        parser.error(str(exc))
    if args.end < args.start:
        parser.error("--end must be on or after --start")
    repo = DynamoLeadRepository(
        args.table,
        boto3.client("dynamodb", region_name=args.region, endpoint_url=args.endpoint_url),
    )
    # Exclusive creation avoids overwriting an existing contact export.
    fd = os.open(output_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", newline="", encoding="utf-8-sig") as output:
        export_csv(repo, args.start, args.end + timedelta(days=1), output)


if __name__ == "__main__":
    main()
