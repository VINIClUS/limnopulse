#!/usr/bin/env bash
# Run a local InfluxDB 2.7.11 for the integration harness, without Docker.
#
# Downloads the pinned release into the user cache, verifies its SHA-256,
# starts influxd on 127.0.0.1 in the background (or reuses the one it started
# before), waits for /health and onboards the harness defaults. Safe to re-run.
set -euo pipefail

TARBALL="influxdb2-2.7.11_linux_amd64.tar.gz"
TARBALL_SHA256="8d7872013cad3524fb728ca8483d0adc30125ad1af262ab826dcf5d1801159cf"
DOWNLOAD_URL="https://dl.influxdata.com/influxdb/releases/${TARBALL}"
PORT="${INFLUXD_PORT:-8086}"
CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/influxd"
INSTALL_DIR="${CACHE_DIR}/influxdb2-2.7.11"
BINARY="${INSTALL_DIR}/usr/bin/influxd"
PID_FILE="${CACHE_DIR}/influxd.pid"
PORT_FILE="${CACHE_DIR}/influxd.port"
LOG_FILE="${CACHE_DIR}/influxd.log"
BASE_URL="http://127.0.0.1:${PORT}"
ORG="${INFLUX_INTEGRATION_ORG:-limnopulse}"
TOKEN="${INFLUX_INTEGRATION_TOKEN:-local-dev-token}"
WORK_DIR=""
RESPONSE=""

cleanup() {
  if [[ -n "${WORK_DIR}" ]]; then rm -rf "${WORK_DIR}"; fi
  if [[ -n "${RESPONSE}" ]]; then rm -f "${RESPONSE}"; fi
}
trap cleanup EXIT

healthy() {
  curl -fsS --max-time 2 "${BASE_URL}/health" >/dev/null 2>&1
}

# Wait for /health; stop early if the given pid (when set) has exited.
wait_healthy() {
  local pid="${1:-}"
  for _ in $(seq 1 60); do
    if healthy; then
      return 0
    fi
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
      return 1
    fi
    sleep 1
  done
  return 1
}

# Print the version /health reports (empty when it reports none).
health_version() {
  curl -fsS --max-time 2 "${BASE_URL}/health" 2>/dev/null \
    | sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' || true
}

# Encode a value as a JSON string literal.
json_string() {
  local value="${1//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '"%s"' "${value}"
}

# True when the PID file names a live influxd process.
running() {
  local pid
  [[ -f "${PID_FILE}" ]] || return 1
  pid="$(cat "${PID_FILE}")"
  kill -0 "${pid}" 2>/dev/null || return 1
  [[ ! -r "/proc/${pid}/comm" || "$(cat "/proc/${pid}/comm")" == "influxd" ]]
}

install_influxd() {
  WORK_DIR="$(mktemp -d)"
  curl --proto "=https" --tlsv1.2 -fsSL -o "${WORK_DIR}/${TARBALL}" "${DOWNLOAD_URL}"
  if ! echo "${TARBALL_SHA256}  ${WORK_DIR}/${TARBALL}" | sha256sum -c --status; then
    echo "sha256 checksum mismatch for ${TARBALL}; refusing to install it" >&2
    exit 1
  fi
  # The tarball ships influxd only; there is no influx CLI. Extract aside and
  # move into place so an interrupted run never leaves a truncated binary.
  tar -xzf "${WORK_DIR}/${TARBALL}" -C "${WORK_DIR}"
  rm -rf "${INSTALL_DIR}"
  mv "${WORK_DIR}/influxdb2-2.7.11" "${INSTALL_DIR}"
  rm -rf "${WORK_DIR}"
  WORK_DIR=""
}

mkdir -p "${CACHE_DIR}"
if running; then
  if [[ -f "${PORT_FILE}" && "$(cat "${PORT_FILE}")" != "${PORT}" ]]; then
    echo "influxd (pid $(cat "${PID_FILE}")) already runs on port $(cat "${PORT_FILE}"), not ${PORT}; stop it first" >&2
    exit 1
  fi
  echo "Reusing influxd (pid $(cat "${PID_FILE}")) on ${BASE_URL}"
  if ! wait_healthy; then
    echo "influxd is not healthy on ${BASE_URL}; see ${LOG_FILE}" >&2
    exit 1
  fi
  version="$(health_version)"
  if [[ -n "${version}" && "${version}" != "v2.7.11" ]]; then
    echo "the reused server on ${BASE_URL} is InfluxDB ${version}, not v2.7.11; stop it first" >&2
    exit 1
  fi
else
  if healthy; then
    echo "another server already answers on ${BASE_URL}; stop it or set INFLUXD_PORT" >&2
    exit 1
  fi
  if [[ ! -x "${BINARY}" ]]; then
    install_influxd
  fi
  mkdir -p "${CACHE_DIR}/data"
  nohup "${BINARY}" \
    --http-bind-address "127.0.0.1:${PORT}" \
    --bolt-path "${CACHE_DIR}/data/influxd.bolt" \
    --engine-path "${CACHE_DIR}/data/engine" \
    --reporting-disabled >"${LOG_FILE}" 2>&1 &
  pid="$!"
  echo "${pid}" >"${PID_FILE}"
  echo "${PORT}" >"${PORT_FILE}"
  if ! wait_healthy "${pid}" || ! kill -0 "${pid}" 2>/dev/null; then
    echo "influxd did not start on ${BASE_URL}; see ${LOG_FILE}" >&2
    exit 1
  fi
  if [[ "$(health_version)" != "v2.7.11" ]]; then
    echo "the server on ${BASE_URL} is not InfluxDB v2.7.11" >&2
    exit 1
  fi
  echo "Started influxd (pid ${pid}) on ${BASE_URL}"
fi

# Onboard the harness defaults. A conflict means a previous run already did.
RESPONSE="$(mktemp)"
status="$(curl -sS -o "${RESPONSE}" -w '%{http_code}' -X POST "${BASE_URL}/api/v2/setup" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"limnopulse\",\"password\":\"local-dev-password\",\"org\":$(json_string "${ORG}"),\"bucket\":\"limnopulse_dev\",\"token\":$(json_string "${TOKEN}")}")"
if [[ "${status}" == "201" ]]; then
  echo "Onboarded org ${ORG} on ${BASE_URL}"
elif grep -q '"conflict"' "${RESPONSE}" && grep -q 'onboarding has already been completed' "${RESPONSE}"; then
  echo "Onboarding has already been completed on ${BASE_URL}"
else
  echo "setup failed with HTTP ${status}: $(cat "${RESPONSE}")" >&2
  exit 1
fi
