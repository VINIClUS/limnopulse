"""Harness smoke tests against real ElasticMQ 1.6.16, DynamoDB Local 3.3.2 and InfluxDB (P3-01).

They prove the emulator semantics the P3 adapters rely on, the harness fixtures, and the
``ToggleableTcpProxy`` that simulates an InfluxDB outage. The whole directory skips unless
``RUN_INGESTION_INTEGRATION=1`` (``make verify-ingestion``).
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from urllib.parse import urlsplit

import pytest
from botocore.exceptions import ClientError
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from tests.support.ingestion_harness import (
    IngestionQueuePair,
    IngestionTables,
    ToggleableTcpProxy,
    dynamodb_client,
    queue_pair,
    sqs_client,
    table_pair,
)

from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.telemetry_v2_schema import OBSERVATION_MEASUREMENT
from scripts.dev import init_dynamodb
from tests.support.influx_harness import influx_clients, influx_settings
from tests.support.telemetry_v2_vectors import scenario

QueueFactory = Callable[..., IngestionQueuePair]

MESSAGE_ATTRIBUTES = ["All"]
EMULATOR_MAX_MESSAGE_CHARACTERS = 1_048_576


# --- helpers ----------------------------------------------------------------------------------


def _receive(sqs, queue_url: str, *, wait: int = 1) -> list[dict]:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=wait,
        MessageSystemAttributeNames=MESSAGE_ATTRIBUTES,
    )
    return response.get("Messages", [])


def _release(sqs, queue_url: str, message: dict) -> None:
    sqs.change_message_visibility(
        QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0
    )


def _queue_attributes(sqs, queue_url: str) -> dict[str, str]:
    return sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])["Attributes"]


def _error_code(error: ClientError) -> str:
    return error.response["Error"]["Code"]


def _key(pk: str, sk: str = "META") -> dict:
    return {"PK": {"S": pk}, "SK": {"S": sk}}


def _index_shape(index: dict) -> tuple:
    projection = index["Projection"]
    return (
        index["IndexName"],
        json.dumps(index["KeySchema"], sort_keys=True),
        projection["ProjectionType"],
        tuple(sorted(projection.get("NonKeyAttributes", []))),
    )


def _ttl_status(dynamodb, table_name: str) -> str:
    response = dynamodb.describe_time_to_live(TableName=table_name)
    return response["TimeToLiveDescription"]["TimeToLiveStatus"]


def _table_exists(dynamodb, table_name: str) -> bool:
    try:
        dynamodb.describe_table(TableName=table_name)
    except ClientError as error:
        assert _error_code(error) == "ResourceNotFoundException"
        return False
    return True


def _queue_exists(sqs, queue_url: str) -> bool:
    name = queue_url.rstrip("/").rsplit("/", 1)[-1]
    try:
        sqs.get_queue_url(QueueName=name)
    except ClientError as error:
        assert "NonExistentQueue" in _error_code(error) or "QueueDoesNotExist" in _error_code(error)
        return False
    return True


@pytest.fixture
def sqs():
    return sqs_client()


@pytest.fixture
def dynamodb():
    return dynamodb_client()


# --- the gate itself ---------------------------------------------------------------------------


def test_verify_ingestion_also_enables_the_influx_backed_tests() -> None:
    # The reused influx_bucket fixture skips without this flag, which would silently turn the
    # Influx-backed ingestion tests (the proxy outage below, G7 later) into skips.
    assert os.environ.get("RUN_INGESTION_INTEGRATION") == "1"
    assert os.environ.get("RUN_INFLUX_INTEGRATION") == "1", (
        "make verify-ingestion must also set RUN_INFLUX_INTEGRATION=1"
    )


# --- clients -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("factory", "variable", "default"),
    [
        (sqs_client, "INGESTION_SQS_URL", "http://127.0.0.1:9324"),
        (dynamodb_client, "INGESTION_DYNAMODB_URL", "http://127.0.0.1:8001"),
    ],
)
def test_clients_use_the_ingestion_endpoint_variables(
    monkeypatch: pytest.MonkeyPatch, factory, variable: str, default: str
) -> None:
    monkeypatch.delenv(variable, raising=False)
    assert factory().meta.endpoint_url == default

    monkeypatch.setenv(variable, "")
    assert factory().meta.endpoint_url == default

    monkeypatch.setenv(variable, "http://127.0.0.1:65000")
    assert factory().meta.endpoint_url == "http://127.0.0.1:65000"


def test_clients_work_without_any_ambient_aws_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "missing-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    assert sqs_client().list_queues()["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert isinstance(dynamodb_client().list_tables()["TableNames"], list)


# --- SQS (ElasticMQ) ---------------------------------------------------------------------------


def test_queue_pair_factory_creates_a_redrive_wired_pair(
    sqs, ingestion_queue_pair: QueueFactory
) -> None:
    pair = ingestion_queue_pair()
    other = ingestion_queue_pair(max_receive_count=3, visibility_seconds=5)

    assert len({pair.source_url, pair.dlq_url, other.source_url, other.dlq_url}) == 4

    source = _queue_attributes(sqs, pair.source_url)
    assert source["QueueArn"] == pair.source_arn
    assert source["VisibilityTimeout"] == "2"
    redrive = json.loads(source["RedrivePolicy"])
    assert redrive["deadLetterTargetArn"] == pair.dlq_arn
    assert int(redrive["maxReceiveCount"]) == 2
    assert _queue_attributes(sqs, pair.dlq_url)["QueueArn"] == pair.dlq_arn
    assert "RedrivePolicy" not in _queue_attributes(sqs, pair.dlq_url)

    other_source = _queue_attributes(sqs, other.source_url)
    assert other_source["VisibilityTimeout"] == "5"
    other_redrive = json.loads(other_source["RedrivePolicy"])
    assert other_redrive["deadLetterTargetArn"] == other.dlq_arn
    assert int(other_redrive["maxReceiveCount"]) == 3


def test_queue_pair_context_deletes_both_queues_on_exit(sqs) -> None:
    with queue_pair(sqs, max_receive_count=2, visibility_seconds=2) as pair:
        assert _queue_exists(sqs, pair.source_url)
        assert _queue_exists(sqs, pair.dlq_url)

    assert not _queue_exists(sqs, pair.source_url)
    assert not _queue_exists(sqs, pair.dlq_url)


def test_message_moves_to_the_dlq_on_the_receive_after_max_receive_count(
    sqs, ingestion_queue_pair: QueueFactory
) -> None:
    pair = ingestion_queue_pair(max_receive_count=2, visibility_seconds=2)
    body = json.dumps({"schema": "harness", "id": uuid.uuid4().hex})
    sqs.send_message(QueueUrl=pair.source_url, MessageBody=body)

    counts: list[str] = []
    sent: set[str] = set()
    for _ in range(2):
        messages = _receive(sqs, pair.source_url)
        assert len(messages) == 1
        message = messages[0]
        assert message["Body"] == body
        counts.append(message["Attributes"]["ApproximateReceiveCount"])
        sent.add(message["Attributes"]["SentTimestamp"])
        _release(sqs, pair.source_url, message)
    assert counts == ["1", "2"]
    assert len(sent) == 1

    # The receive after maxReceiveCount moves the message instead of returning it.
    assert _receive(sqs, pair.source_url) == []

    dead = _receive(sqs, pair.dlq_url, wait=2)
    assert len(dead) == 1
    assert dead[0]["Body"] == body
    assert dead[0]["Attributes"]["SentTimestamp"] == sent.pop()
    assert _receive(sqs, pair.source_url) == []


def test_change_message_visibility_zero_makes_a_message_visible_again(
    sqs, ingestion_queue_pair: QueueFactory
) -> None:
    pair = ingestion_queue_pair(max_receive_count=5, visibility_seconds=30)
    sqs.send_message(QueueUrl=pair.source_url, MessageBody="visible-again")

    first = _receive(sqs, pair.source_url)
    assert len(first) == 1
    # Still inside its 30 s visibility timeout: hidden.
    assert _receive(sqs, pair.source_url) == []

    _release(sqs, pair.source_url, first[0])
    second = _receive(sqs, pair.source_url)
    assert [m["Body"] for m in second] == ["visible-again"]
    assert second[0]["Attributes"]["ApproximateReceiveCount"] == "2"


def test_sent_timestamp_is_returned_in_epoch_milliseconds(
    sqs, ingestion_queue_pair: QueueFactory
) -> None:
    pair = ingestion_queue_pair()
    before_ms = int(time.time() * 1000) - 5_000
    sqs.send_message(QueueUrl=pair.source_url, MessageBody="timestamped")
    after_ms = int(time.time() * 1000) + 5_000

    messages = _receive(sqs, pair.source_url)
    assert len(messages) == 1
    sent_ms = int(messages[0]["Attributes"]["SentTimestamp"])
    assert before_ms <= sent_ms <= after_ms


def test_emulator_message_limit_is_one_mebi_character(
    sqs, ingestion_queue_pair: QueueFactory
) -> None:
    # ElasticMQ 1.6.16 counts characters and ignores MaximumMessageSize, so the 262,144-byte
    # guard is proven in the P3-05 adapter. This records the emulator limit.
    pair = ingestion_queue_pair()
    sqs.send_message(QueueUrl=pair.source_url, MessageBody="a" * 262_145)
    sqs.send_message(QueueUrl=pair.source_url, MessageBody="a" * EMULATOR_MAX_MESSAGE_CHARACTERS)

    with pytest.raises(ClientError) as raised:
        sqs.send_message(
            QueueUrl=pair.source_url, MessageBody="a" * (EMULATOR_MAX_MESSAGE_CHARACTERS + 1)
        )
    error = raised.value.response["Error"]
    assert "MessageTooLong" in (error.get("Code", ""), error.get("Message", ""))


# --- DynamoDB Local ----------------------------------------------------------------------------


def test_table_request_is_pure_and_matches_the_cli_schemas() -> None:
    domain = init_dynamodb.table_request("LimnopulseDomain", include_alert_indexes=True)
    audit = init_dynamodb.table_request("LimnopulseAudit")

    assert domain == init_dynamodb.table_request("LimnopulseDomain", include_alert_indexes=True)
    assert domain["TableName"] == "LimnopulseDomain"
    assert domain["BillingMode"] == "PAY_PER_REQUEST"
    assert domain["KeySchema"] == [
        {"AttributeName": "PK", "KeyType": "HASH"},
        {"AttributeName": "SK", "KeyType": "RANGE"},
    ]
    assert [index["IndexName"] for index in domain["GlobalSecondaryIndexes"]] == [
        index["IndexName"] for index in init_dynamodb.ALERT_INDEXES
    ]
    assert audit["TableName"] == "LimnopulseAudit"
    assert audit["KeySchema"] == domain["KeySchema"]
    assert "GlobalSecondaryIndexes" not in audit
    assert {d["AttributeName"] for d in audit["AttributeDefinitions"]} == {"PK", "SK"}

    # The returned request is a fresh value: mutating it never leaks into the next call.
    domain["GlobalSecondaryIndexes"].clear()
    assert init_dynamodb.table_request("LimnopulseDomain", include_alert_indexes=True)[
        "GlobalSecondaryIndexes"
    ]


def test_ingestion_tables_mirror_init_dynamodb_with_ttl_disabled(
    dynamodb, ingestion_tables: IngestionTables
) -> None:
    assert ingestion_tables.domain != ingestion_tables.audit

    for name, include_alert_indexes in (
        (ingestion_tables.domain, True),
        (ingestion_tables.audit, False),
    ):
        expected = init_dynamodb.table_request(name, include_alert_indexes=include_alert_indexes)
        table = dynamodb.describe_table(TableName=name)["Table"]

        assert table["TableStatus"] == "ACTIVE"
        assert table["KeySchema"] == expected["KeySchema"]
        assert sorted(
            (d["AttributeName"], d["AttributeType"]) for d in table["AttributeDefinitions"]
        ) == sorted(
            (d["AttributeName"], d["AttributeType"]) for d in expected["AttributeDefinitions"]
        )
        assert sorted(map(_index_shape, table.get("GlobalSecondaryIndexes", []))) == sorted(
            map(_index_shape, expected.get("GlobalSecondaryIndexes", []))
        )
        assert all(
            index["IndexStatus"] == "ACTIVE" for index in table.get("GlobalSecondaryIndexes", [])
        )
        # DynamoDB Local 3.3.2 deletes expired items within seconds once TTL is on.
        assert _ttl_status(dynamodb, name) == "DISABLED"


def test_table_pair_context_deletes_both_tables_on_exit(dynamodb) -> None:
    with table_pair(dynamodb) as tables:
        assert _table_exists(dynamodb, tables.domain)
        assert _table_exists(dynamodb, tables.audit)

    assert not _table_exists(dynamodb, tables.domain)
    assert not _table_exists(dynamodb, tables.audit)


@pytest.mark.parametrize(
    ("kwargs", "status"), [({"enable_ttl": False}, "DISABLED"), ({}, "ENABLED")]
)
def test_ensure_table_enables_ttl_only_when_asked(dynamodb, kwargs: dict, status: str) -> None:
    name = f"it_ttl_{uuid.uuid4().hex}"
    try:
        init_dynamodb.ensure_table(dynamodb, name, **kwargs)
        assert _ttl_status(dynamodb, name) == status
    finally:
        if _table_exists(dynamodb, name):
            dynamodb.delete_table(TableName=name)


def test_conditional_put_with_attribute_not_exists_fails_on_the_second_write(
    dynamodb, ingestion_tables: IngestionTables
) -> None:
    item = {**_key("CONDITIONAL#1"), "value": {"S": "first"}}
    dynamodb.put_item(
        TableName=ingestion_tables.domain,
        Item=item,
        ConditionExpression="attribute_not_exists(PK)",
    )

    with pytest.raises(ClientError) as raised:
        dynamodb.put_item(
            TableName=ingestion_tables.domain,
            Item={**_key("CONDITIONAL#1"), "value": {"S": "second"}},
            ConditionExpression="attribute_not_exists(PK)",
        )
    assert _error_code(raised.value) == "ConditionalCheckFailedException"

    stored = dynamodb.get_item(
        TableName=ingestion_tables.domain, Key=_key("CONDITIONAL#1"), ConsistentRead=True
    )["Item"]
    assert stored["value"] == {"S": "first"}


def test_simple_mark_condition_is_a_monotone_high_water_mark(
    dynamodb, ingestion_tables: IngestionTables
) -> None:
    def mark(value: str) -> bool:
        try:
            dynamodb.update_item(
                TableName=ingestion_tables.domain,
                Key=_key("MARK#1", "INGEST_STATE#CURRENT"),
                UpdateExpression="SET #m = :t",
                ConditionExpression="attribute_not_exists(#m) OR #m < :t",
                ExpressionAttributeNames={"#m": "last_received_at"},
                ExpressionAttributeValues={":t": {"S": value}},
            )
        except ClientError as error:
            assert _error_code(error) == "ConditionalCheckFailedException"
            return False
        return True

    outcomes = [
        mark("2026-01-01T00:00:05.000000Z"),
        mark("2026-01-01T00:00:03.000000Z"),
        mark("2026-01-01T00:00:05.000000Z"),
        mark("2026-01-01T00:00:07.000000Z"),
        mark("2026-01-01T00:00:06.000000Z"),
    ]

    assert outcomes == [True, False, False, True, False]
    stored = dynamodb.get_item(
        TableName=ingestion_tables.domain,
        Key=_key("MARK#1", "INGEST_STATE#CURRENT"),
        ConsistentRead=True,
    )["Item"]
    assert stored["last_received_at"] == {"S": "2026-01-01T00:00:07.000000Z"}


def test_transaction_with_a_failing_condition_check_reports_its_index(
    dynamodb, ingestion_tables: IngestionTables
) -> None:
    with pytest.raises(ClientError) as raised:
        dynamodb.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": ingestion_tables.domain, "Item": _key("TX#written")}},
                {
                    "ConditionCheck": {
                        "TableName": ingestion_tables.domain,
                        "Key": _key("TX#missing"),
                        "ConditionExpression": "attribute_exists(PK)",
                    }
                },
                {"Put": {"TableName": ingestion_tables.audit, "Item": _key("TX#audit")}},
            ]
        )

    assert _error_code(raised.value) == "TransactionCanceledException"
    reasons = [reason["Code"] for reason in raised.value.response["CancellationReasons"]]
    assert reasons == ["None", "ConditionalCheckFailed", "None"]
    # All or nothing: neither Put was applied.
    assert "Item" not in dynamodb.get_item(
        TableName=ingestion_tables.domain, Key=_key("TX#written"), ConsistentRead=True
    )
    assert "Item" not in dynamodb.get_item(
        TableName=ingestion_tables.audit, Key=_key("TX#audit"), ConsistentRead=True
    )


# --- ToggleableTcpProxy ------------------------------------------------------------------------


class _EchoServer:
    """A loopback TCP echo server, so the proxy is tested without any service."""

    def __init__(self) -> None:
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._closed = threading.Event()
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while not self._closed.is_set():
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._echo, args=(conn,), daemon=True).start()

    @staticmethod
    def _echo(conn: socket.socket) -> None:
        with conn:
            while True:
                try:
                    data = conn.recv(65536)
                except OSError:
                    return
                if not data:
                    return
                conn.sendall(data)

    def close(self) -> None:
        self._closed.set()
        self._listener.close()


@pytest.fixture
def echo_server() -> Iterator[_EchoServer]:
    server = _EchoServer()
    try:
        yield server
    finally:
        server.close()


def _round_trip(sock: socket.socket, payload: bytes) -> bytes:
    sock.sendall(payload)
    received = b""
    while len(received) < len(payload):
        chunk = sock.recv(65536)
        if not chunk:
            break
        received += chunk
    return received


def _connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=2)
    sock.settimeout(2)
    return sock


def test_proxy_forwards_bytes_in_both_directions(echo_server: _EchoServer) -> None:
    with ToggleableTcpProxy("127.0.0.1", echo_server.port) as proxy:
        assert proxy.port != echo_server.port
        assert proxy.url == f"http://127.0.0.1:{proxy.port}"
        with _connect(proxy.port) as sock:
            assert _round_trip(sock, b"ping") == b"ping"
            payload = os.urandom(200_000)
            assert _round_trip(sock, payload) == payload


def test_proxy_down_resets_established_sockets_and_refuses_new_ones(
    echo_server: _EchoServer,
) -> None:
    with ToggleableTcpProxy("127.0.0.1", echo_server.port) as proxy:
        established = _connect(proxy.port)
        try:
            assert _round_trip(established, b"warm") == b"warm"

            proxy.down()

            # Reset, not a graceful close: a pooled keep-alive connection cannot survive.
            with pytest.raises(ConnectionResetError):
                established.sendall(b"after-down")
                established.recv(65536)
        finally:
            established.close()

        # Refuse mode: a new connection fails at once instead of waiting out a timeout.
        started = time.monotonic()
        with pytest.raises(ConnectionRefusedError):
            socket.create_connection(("127.0.0.1", proxy.port), timeout=5)
        assert time.monotonic() - started < 1


def test_proxy_up_restores_forwarding_on_the_same_port(echo_server: _EchoServer) -> None:
    with ToggleableTcpProxy("127.0.0.1", echo_server.port) as proxy:
        port = proxy.port
        proxy.down()
        proxy.down()  # idempotent
        proxy.up()
        proxy.up()  # idempotent

        assert proxy.port == port
        with _connect(port) as sock:
            assert _round_trip(sock, b"restored") == b"restored"


def _observation_rows(bucket: str) -> int:
    flux = (
        f'from(bucket: "{bucket}")'
        " |> range(start: 1970-01-01T00:00:00Z, stop: 2100-01-01T00:00:00Z)"
        f' |> filter(fn: (r) => r._measurement == "{OBSERVATION_MEASUREMENT}")'
    )
    with influx_clients(bucket) as (query_api, _write_api):
        return sum(len(table.records) for table in query_api.query(flux))


@pytest.mark.asyncio
async def test_proxy_down_makes_influx_writes_retryable_and_up_restores_them(
    influx_bucket: str,
) -> None:
    settings = influx_settings()
    target = urlsplit(settings.url)
    sample = scenario("legacy_sample")

    with ToggleableTcpProxy(target.hostname or "127.0.0.1", target.port or 8086) as proxy:
        client = InfluxDBClient(
            url=proxy.url, token=settings.token, org=settings.org, timeout=5_000
        )
        try:
            with client.write_api(write_options=SYNCHRONOUS) as write_api:
                writer = InfluxObservationWriter(
                    write_api=write_api, bucket=influx_bucket, org=settings.org
                )
                # Warm the connection pool so a keep-alive socket exists before the outage.
                await writer.write_observations(sample.observations)
                assert _observation_rows(influx_bucket) > 0

                proxy.down()
                started = time.monotonic()
                with pytest.raises(TelemetryWriteError) as raised:
                    await writer.write_observations(sample.observations)
                assert raised.value.retryable is True
                assert time.monotonic() - started < 5

                proxy.up()
                await writer.write_observations(sample.observations)
        finally:
            client.close()

    assert _observation_rows(influx_bucket) > 0
