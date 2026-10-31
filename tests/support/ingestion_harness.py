"""ElasticMQ / DynamoDB Local integration harness for ``tests/ingestion`` (P3-01).

Importing this module never skips or connects. The ``ingestion_queue_pair`` and
``ingestion_tables`` fixtures skip unless ``RUN_INGESTION_INTEGRATION=1``; each test gets its own
uniquely named queues and tables, deleted at teardown. Start the emulators first with
``scripts/dev/run_local_elasticmq.sh`` and ``scripts/dev/run_local_dynamodb.sh``.

The clients carry dummy credentials set here, so neither the workflow nor the environment needs
AWS credential variables. ``ToggleableTcpProxy`` sits in front of InfluxDB to simulate an outage
without stopping the server.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import struct
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any, NamedTuple

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts.dev import init_dynamodb

RUN_ENV = "RUN_INGESTION_INTEGRATION"
SQS_URL_ENV = "INGESTION_SQS_URL"
DYNAMODB_URL_ENV = "INGESTION_DYNAMODB_URL"
DEFAULT_SQS_URL = "http://127.0.0.1:9324"
DEFAULT_DYNAMODB_URL = "http://127.0.0.1:8001"
READY_TIMEOUT_SECONDS = 60

# Emulator-only placeholders; the emulators accept any value.
_REGION = "us-east-1"
_DUMMY_KEY = "local"
# The read timeout covers 20 s long polls; standard retries stay short for a refused port.
_CLIENT_CONFIG = Config(
    connect_timeout=2,
    read_timeout=30,
    retries={"mode": "standard", "max_attempts": 2},
)


class IngestionQueuePair(NamedTuple):
    source_url: str
    dlq_url: str
    source_arn: str
    dlq_arn: str


class IngestionTables(NamedTuple):
    domain: str
    audit: str


def _endpoint(variable: str, default: str) -> str:
    """An empty variable counts as unset."""
    return os.environ.get(variable) or default


def _client(service: str, endpoint_url: str) -> Any:
    return boto3.session.Session().client(
        service,
        region_name=_REGION,
        endpoint_url=endpoint_url,
        aws_access_key_id=_DUMMY_KEY,
        aws_secret_access_key=_DUMMY_KEY,
        config=_CLIENT_CONFIG,
    )


def sqs_client() -> Any:
    return _client("sqs", _endpoint(SQS_URL_ENV, DEFAULT_SQS_URL))


def dynamodb_client() -> Any:
    return _client("dynamodb", _endpoint(DYNAMODB_URL_ENV, DEFAULT_DYNAMODB_URL))


def wait_until_ready(sqs: Any, dynamodb: Any, *, timeout_seconds: float) -> None:
    """Poll ``ListQueues`` and ``ListTables`` until both answer, or raise ``TimeoutError``."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            sqs.list_queues()
            dynamodb.list_tables()
            return
        except (BotoCoreError, ClientError) as error:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"the ingestion emulators did not answer within {timeout_seconds}s "
                    f"({SQS_URL_ENV}={sqs.meta.endpoint_url}, "
                    f"{DYNAMODB_URL_ENV}={dynamodb.meta.endpoint_url}): "
                    f"{type(error).__name__}; start scripts/dev/run_local_elasticmq.sh and "
                    "scripts/dev/run_local_dynamodb.sh first"
                ) from None
        time.sleep(0.2)


_ready_endpoints: set[tuple[str, str]] = set()


def ensure_services_ready() -> None:
    """Wait once per endpoint pair, so a cold-started CI service is never hit too early."""
    sqs, dynamodb = sqs_client(), dynamodb_client()
    key = (sqs.meta.endpoint_url, dynamodb.meta.endpoint_url)
    if key not in _ready_endpoints:
        wait_until_ready(sqs, dynamodb, timeout_seconds=READY_TIMEOUT_SECONDS)
        _ready_endpoints.add(key)


def _unique(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex}"


def _ignore_missing(action: Callable[[], Any], *codes: str) -> None:
    try:
        action()
    except ClientError as error:
        if error.response["Error"]["Code"] not in codes:
            raise


def _queue_arn(sqs: Any, queue_url: str) -> str:
    attributes = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    return attributes["Attributes"]["QueueArn"]


@contextlib.contextmanager
def queue_pair(
    sqs: Any, *, max_receive_count: int = 2, visibility_seconds: int = 2
) -> Iterator[IngestionQueuePair]:
    """Create a unique source queue wired to its own DLQ; delete both on exit."""
    name = _unique("it-")
    missing = ("AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist")
    dlq_url = sqs.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
    try:
        dlq_arn = _queue_arn(sqs, dlq_url)
        source_url = sqs.create_queue(
            QueueName=name,
            Attributes={
                "VisibilityTimeout": str(visibility_seconds),
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_receive_count)}
                ),
            },
        )["QueueUrl"]
        try:
            yield IngestionQueuePair(source_url, dlq_url, _queue_arn(sqs, source_url), dlq_arn)
        finally:
            _ignore_missing(lambda: sqs.delete_queue(QueueUrl=source_url), *missing)
    finally:
        _ignore_missing(lambda: sqs.delete_queue(QueueUrl=dlq_url), *missing)


@contextlib.contextmanager
def table_pair(dynamodb: Any) -> Iterator[IngestionTables]:
    """Create a unique domain/audit table pair with the init_dynamodb schemas and TTL off.

    TTL stays disabled because DynamoDB Local deletes expired items within seconds once it is
    on; expiry is tested by advancing an injected clock instead.
    """
    tables = IngestionTables(domain=_unique("it_domain_"), audit=_unique("it_audit_"))
    try:
        init_dynamodb.ensure_table(
            dynamodb, tables.domain, include_alert_indexes=True, enable_ttl=False
        )
        init_dynamodb.ensure_table(dynamodb, tables.audit, enable_ttl=False)
        yield tables
    finally:
        for name in tables:
            _ignore_missing(
                lambda name=name: dynamodb.delete_table(TableName=name),
                "ResourceNotFoundException",
            )


def _require_integration() -> None:
    if os.environ.get(RUN_ENV) != "1":
        pytest.skip(f"set {RUN_ENV}=1 to run against ElasticMQ and DynamoDB Local")


@pytest.fixture
def ingestion_queue_pair() -> Iterator[Callable[..., IngestionQueuePair]]:
    """A factory: ``make(max_receive_count=2, visibility_seconds=2)`` creates a fresh pair."""
    _require_integration()
    ensure_services_ready()
    sqs = sqs_client()
    with contextlib.ExitStack() as stack:

        def make(*, max_receive_count: int = 2, visibility_seconds: int = 2) -> IngestionQueuePair:
            return stack.enter_context(
                queue_pair(
                    sqs, max_receive_count=max_receive_count, visibility_seconds=visibility_seconds
                )
            )

        yield make


@pytest.fixture
def ingestion_tables() -> Iterator[IngestionTables]:
    _require_integration()
    ensure_services_ready()
    with table_pair(dynamodb_client()) as tables:
        yield tables


# --- ToggleableTcpProxy -------------------------------------------------------------------------

_RESET_LINGER = struct.pack("ii", 1, 0)


def _reset(sock: socket.socket) -> None:
    """Close with an RST rather than a FIN."""
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _RESET_LINGER)
    with contextlib.suppress(OSError):
        sock.close()


class ToggleableTcpProxy:
    """A thread-based loopback TCP forwarder that can be taken down and brought back up.

    ``down()`` closes the listener, so new connections are refused at once (refuse mode, never
    a blackhole), and resets every established forwarded socket, so a pooled keep-alive
    connection cannot bypass the outage. ``up()`` listens again on the same port.
    """

    def __init__(self, target_host: str, target_port: int, *, listen_host: str = "127.0.0.1"):
        self._target = (target_host, target_port)
        self._listen_host = listen_host
        self._lock = threading.Lock()
        self._listener: socket.socket | None = None
        self._connections: set[socket.socket] = set()
        self._port = 0
        self.up()

    @property
    def port(self) -> int:
        return self._port

    @property
    def url(self) -> str:
        return f"http://{self._listen_host}:{self._port}"

    def __enter__(self) -> ToggleableTcpProxy:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.down()

    def up(self) -> None:
        with self._lock:
            if self._listener is not None:
                return
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind((self._listen_host, self._port))
                listener.listen(64)
            except OSError:
                listener.close()
                raise
            self._port = listener.getsockname()[1]
            self._listener = listener
        threading.Thread(target=self._accept_loop, args=(listener,), daemon=True).start()

    def down(self) -> None:
        with self._lock:
            listener, self._listener = self._listener, None
            connections, self._connections = list(self._connections), set()
        if listener is not None:
            # shutdown wakes the blocked accept(); close alone does not on Linux.
            with contextlib.suppress(OSError):
                listener.shutdown(socket.SHUT_RDWR)
            listener.close()
        # SHUT_RD first wakes every pump blocked in recv without sending anything to the peer;
        # the pumps then see that their sockets are no longer tracked and leave them alone.
        for sock in connections:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RD)
        for sock in connections:
            _reset(sock)

    def _accept_loop(self, listener: socket.socket) -> None:
        while True:
            try:
                client, _ = listener.accept()
            except OSError:
                return
            try:
                upstream = socket.create_connection(self._target, timeout=5)
                upstream.settimeout(None)
            except OSError:
                _reset(client)
                continue
            with self._lock:
                if self._listener is not listener:
                    _reset(client)
                    _reset(upstream)
                    continue
                self._connections.update((client, upstream))
            for source, target in ((client, upstream), (upstream, client)):
                threading.Thread(
                    target=self._pump, args=(source, target, (client, upstream)), daemon=True
                ).start()

    def _pump(
        self, source: socket.socket, target: socket.socket, pair: tuple[socket.socket, ...]
    ) -> None:
        try:
            while data := source.recv(65536):
                target.sendall(data)
        except OSError:
            pass
        with self._lock:
            # After down() the sockets are no longer tracked: it resets them, not the pump.
            if not all(sock in self._connections for sock in pair):
                return
            self._connections.difference_update(pair)
        # A normal end of either direction closes both; shutdown wakes the other pump's recv.
        for sock in pair:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()
