#!/usr/bin/env bash
# Run a local InfluxDB 2.7.11 for the integration harness, without Docker.
#
# Downloads the pinned release into the user cache, verifies its SHA-256,
# starts influxd on 127.0.0.1 in the background (or reuses a running one),
# waits for /health and onboards the harness defaults. Safe to re-run.
set -euo pipefail

TARBALL="influxdb2-2.7.11_linux_amd64.tar.gz"
TARBALL_SHA256="8d7872013cad3524fb728ca8483d0adc30125ad1af262ab826dcf5d1801159cf"
DOWNLOAD_URL="https://dl.influxdata.com/influxdb/releases/${TARBALL}"
PORT="${INFLUXD_PORT:-8086}"
CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/influxd"
PID_FILE="${CACHE_DIR}/influxd.pid"
LOG_FILE="${CACHE_DIR}/influxd.log"
BASE_URL="http://127.0.0.1:${PORT}"
ORG="${INFLUX_INTEGRATION_ORG:-limnopulse}"
TOKEN="${INFLUX_INTEGRATION_TOKEN:-local-dev-token}"

healthy() {
  curl -fsS --max-time 2 "${BASE_URL}/health" >/dev/null 2>&1
}

wait_healthy() {
  for _ in $(seq 1 60); do
    if healthy; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# True when the PID file names a live influxd process.
running() {
  local pid
  [[ -f "${PID_FILE}" ]] || return 1
  pid="$(cat "${PID_FILE}")"
  kill -0 "${pid}" 2>/dev/null || return 1
  [[ ! -r "/proc/${pid}/comm" || "$(cat "/proc/${pid}/comm")" == "influxd" ]]
}

find_binary() {
  find "${CACHE_DIR}" -type f -name influxd -perm -u+x 2>/dev/null | head -n 1 || true
}

install_influxd() {
  local work_dir
  work_dir="$(mktemp -d)"
  trap 'rm -rf "${work_dir}"' EXIT
  curl -fsSL -o "${work_dir}/${TARBALL}" "${DOWNLOAD_URL}"
  if ! echo "${TARBALL_SHA256}  ${work_dir}/${TARBALL}" | sha256sum -c --status; then
    echo "sha256 checksum mismatch for ${TARBALL}; refusing to install it" >&2
    exit 1
  fi
  # The tarball ships influxd only; there is no influx CLI.
  tar -xzf "${work_dir}/${TARBALL}" -C "${CACHE_DIR}"
  rm -rf "${work_dir}"
  trap - EXIT
}

mkdir -p "${CACHE_DIR}"
if running; then
  echo "Reusing influxd (pid $(cat "${PID_FILE}")) on ${BASE_URL}"
else
  binary="$(find_binary)"
  if [[ -z "${binary}" ]]; then
    install_influxd
    binary="$(find_binary)"
  fi
  mkdir -p "${CACHE_DIR}/data"
  nohup "${binary}" \
    --http-bind-address "127.0.0.1:${PORT}" \
    --bolt-path "${CACHE_DIR}/data/influxd.bolt" \
    --engine-path "${CACHE_DIR}/data/engine" \
    --reporting-disabled >"${LOG_FILE}" 2>&1 &
  echo "$!" >"${PID_FILE}"
  echo "Started influxd (pid $!) on ${BASE_URL}"
fi

if ! wait_healthy; then
  echo "influxd is not healthy on ${BASE_URL}; see ${LOG_FILE}" >&2
  exit 1
fi

# Onboard the harness defaults. A conflict means a previous run already did.
response="$(mktemp)"
trap 'rm -f "${response}"' EXIT
status="$(curl -sS -o "${response}" -w '%{http_code}' -X POST "${BASE_URL}/api/v2/setup" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"limnopulse\",\"password\":\"local-dev-password\",\"org\":\"${ORG}\",\"bucket\":\"limnopulse_dev\",\"token\":\"${TOKEN}\"}")"
if [[ "${status}" == "201" ]]; then
  echo "Onboarded org ${ORG} on ${BASE_URL}"
elif grep -q '"conflict"' "${response}" && grep -q 'onboarding has already been completed' "${response}"; then
  echo "Onboarding has already been completed on ${BASE_URL}"
else
  echo "setup failed with HTTP ${status}: $(cat "${response}")" >&2
  exit 1
fi
