import argparse
import json
import socket
from pathlib import Path


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1883
DEFAULT_TOPIC = "limnopulse/v1/devices/local-device-001/readings"
DEFAULT_PAYLOAD = Path("examples/telemetry/reading.local.json")


def _remaining_length_bytes(length: int) -> bytes:
    encoded = bytearray()
    while True:
        digit = length % 128
        length //= 128
        if length > 0:
            digit |= 128
        encoded.append(digit)
        if length == 0:
            return bytes(encoded)


def _utf8_field(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return len(encoded).to_bytes(2, "big") + encoded


def _connect_packet(client_id: str) -> bytes:
    variable_header = _utf8_field("MQTT") + bytes([4, 2]) + (30).to_bytes(2, "big")
    payload = _utf8_field(client_id)
    remaining = variable_header + payload
    return bytes([0x10]) + _remaining_length_bytes(len(remaining)) + remaining


def _publish_packet(topic: str, payload: bytes) -> bytes:
    variable_header = _utf8_field(topic)
    remaining = variable_header + payload
    return bytes([0x30]) + _remaining_length_bytes(len(remaining)) + remaining


def publish(host: str, port: int, topic: str, payload: bytes) -> None:
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.sendall(_connect_packet("limnopulse-local-publisher"))
        connack = sock.recv(4)
        if connack != b"\x20\x02\x00\x00":
            raise RuntimeError(f"mqtt connect failed: {connack!r}")
        sock.sendall(_publish_packet(topic, payload))
        sock.sendall(b"\xe0\x00")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--payload", type=Path, default=DEFAULT_PAYLOAD)
    args = parser.parse_args()

    payload = json.dumps(json.loads(args.payload.read_text()), separators=(",", ":")).encode("utf-8")
    publish(args.host, args.port, args.topic, payload)
    print(f"published {len(payload)} bytes to {args.topic}")


if __name__ == "__main__":
    main()
