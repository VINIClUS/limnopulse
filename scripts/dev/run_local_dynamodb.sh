#!/usr/bin/env bash
# Run a local DynamoDB Local 3.3.2 for the ingestion harness, without Docker.
#
# Downloads the release tarball into the user cache, verifies its SHA-256, starts an in-memory
# server in the background (or reuses the one it started before) and waits until it answers.
# Safe to re-run. The download URL always serves the latest release, so a checksum mismatch
# means a newer release was published: the script then stops and asks for a deliberate re-pin.
#
# DynamoDB Local has no bind-address flag: it listens on 0.0.0.0 (every interface), not only on
# 127.0.0.1 like the ElasticMQ and influxd runners. It holds throwaway test data, but run it only
# on a trusted network or behind a host firewall.
set -euo pipefail

TARBALL_PATH="v2.x/dynamodb_local_latest.tar.gz"
TARBALL="${TARBALL_PATH##*/}"
TARBALL_SHA256="7a94eb65f81aee270738751a4e5e3e6a7ad02a5d25194f55b83e08ec2ba6342b"
DOWNLOAD_URL="https://d1ni2b6xgvw0s0.cloudfront.net/${TARBALL_PATH}"
PORT="${DYNAMODB_LOCAL_PORT:-8001}"
CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/dynamodb-local"
INSTALL_DIR="${CACHE_DIR}/dynamodb-local-3.3.2"
JAR_PATH="${INSTALL_DIR}/DynamoDBLocal.jar"
# Records the tarball checksum the install was verified against.
VERIFIED_FILE="${INSTALL_DIR}/.verified-sha256"
PID_FILE="${CACHE_DIR}/dynamodb-local.pid"
PORT_FILE="${CACHE_DIR}/dynamodb-local.port"
LOG_FILE="${CACHE_DIR}/dynamodb-local.log"
BASE_URL="http://127.0.0.1:${PORT}"
WORK_DIR=""

cleanup() {
  if [[ -n "${WORK_DIR}" ]]; then rm -rf "${WORK_DIR}"; fi
}
trap cleanup EXIT

# Matches the other local runners: linux/amd64 with Java 17 or newer.
if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "run_local_dynamodb.sh supports linux/amd64 only (this host is $(uname -s)/$(uname -m))" >&2
  exit 1
fi
if ! command -v java >/dev/null 2>&1; then
  echo "java (17 or newer) is required on PATH" >&2
  exit 1
fi
java_major="$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java\.specification\.version = \([0-9]*\).*/\1/p')"
if [[ -z "${java_major}" || "${java_major}" -lt 17 ]]; then
  echo "java 17 or newer is required (found '${java_major:-unknown}')" >&2
  exit 1
fi

# True once the server answers HTTP at all (an unsigned request gets a 400).
ready() {
  local code
  code="$(curl -sS --max-time 2 -o /dev/null -w '%{http_code}' "${BASE_URL}/" 2>/dev/null || true)"
  [[ -n "${code}" && "${code}" != "000" ]]
}

wait_ready() {
  local pid="${1:-}"
  for _ in $(seq 1 60); do
    if ready; then
      return 0
    fi
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
      return 1
    fi
    sleep 1
  done
  return 1
}

# True when the PID file names a live java process running this JAR (not a reused pid).
running() {
  local pid
  [[ -f "${PID_FILE}" ]] || return 1
  pid="$(cat "${PID_FILE}")"
  kill -0 "${pid}" 2>/dev/null || return 1
  [[ ! -r "/proc/${pid}/cmdline" ]] || tr '\0' ' ' <"/proc/${pid}/cmdline" | grep -qF -- "${JAR_PATH}"
}

abort_start() {
  local pid="$1"
  shift
  echo "$*" >&2
  kill "${pid}" 2>/dev/null || true
  rm -f "${PID_FILE}" "${PORT_FILE}"
  exit 1
}

install_dynamodb_local() {
  # Inside the cache directory, so the final mv is an atomic rename.
  WORK_DIR="$(mktemp -d "${CACHE_DIR}/.tmp.XXXXXX")"
  curl --proto "=https" --tlsv1.2 -fsSL -o "${WORK_DIR}/${TARBALL}" "${DOWNLOAD_URL}"
  if ! echo "${TARBALL_SHA256}  ${WORK_DIR}/${TARBALL}" | sha256sum -c --status; then
    echo "sha256 checksum mismatch for ${DOWNLOAD_URL}: the URL is mutable and now serves" \
      "another release than DynamoDB Local 3.3.2. Re-pin deliberately: check the new release," \
      "then update TARBALL_SHA256 here, the CI image digest and docs/verification.md." >&2
    exit 1
  fi
  mkdir -p "${WORK_DIR}/extract"
  tar -xzf "${WORK_DIR}/${TARBALL}" -C "${WORK_DIR}/extract"
  rm -rf "${INSTALL_DIR}"
  echo "${TARBALL_SHA256}" >"${WORK_DIR}/extract/.verified-sha256"
  mv "${WORK_DIR}/extract" "${INSTALL_DIR}"
  rm -rf "${WORK_DIR}"
  WORK_DIR=""
}

mkdir -p "${CACHE_DIR}"
if running; then
  if [[ -f "${PORT_FILE}" && "$(cat "${PORT_FILE}")" != "${PORT}" ]]; then
    echo "DynamoDB Local (pid $(cat "${PID_FILE}")) already runs on port $(cat "${PORT_FILE}"), not ${PORT}; stop it first" >&2
    exit 1
  fi
  echo "Reusing DynamoDB Local (pid $(cat "${PID_FILE}")) on ${BASE_URL}"
  if ! wait_ready; then
    echo "DynamoDB Local does not answer on ${BASE_URL}; see ${LOG_FILE}" >&2
    exit 1
  fi
  exit 0
fi

if ready; then
  echo "another server already answers on ${BASE_URL}; stop it or set DYNAMODB_LOCAL_PORT" >&2
  exit 1
fi
# Reinstall unless the cached copy was verified against the current pin.
if [[ ! -f "${JAR_PATH}" || "$(cat "${VERIFIED_FILE}" 2>/dev/null || true)" != "${TARBALL_SHA256}" ]]; then
  install_dynamodb_local
fi

# In-memory: every run starts empty, and the harness creates and deletes its own tables.
# Started from the cache directory, where DynamoDB Local writes its metadata file.
cd "${CACHE_DIR}"
nohup java -Djava.library.path="${INSTALL_DIR}/DynamoDBLocal_lib" -jar "${JAR_PATH}" \
  -inMemory -port "${PORT}" >"${LOG_FILE}" 2>&1 &
pid="$!"
echo "${pid}" >"${PID_FILE}"
echo "${PORT}" >"${PORT_FILE}"
if ! wait_ready "${pid}" || ! kill -0 "${pid}" 2>/dev/null; then
  abort_start "${pid}" "DynamoDB Local did not start on ${BASE_URL}; see ${LOG_FILE}"
fi
echo "Started DynamoDB Local 3.3.2 (pid ${pid}) on ${BASE_URL}"
