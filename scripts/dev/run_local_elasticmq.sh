#!/usr/bin/env bash
# Run a local ElasticMQ 1.6.16 (SQS emulator) for the ingestion harness, without Docker.
#
# Downloads the pinned server JAR into the user cache, verifies its SHA-256, writes a config that
# binds 127.0.0.1 with strict SQS limits and no pre-declared queues, starts the server in the
# background (or reuses the one it started before) and waits for ListQueues. Safe to re-run.
set -euo pipefail

JAR="elasticmq-server-1.6.16.jar"
JAR_SHA256="30a81f56a40b670fafc3f32b0f09f3baf59ad2645edaf17bc2e79170c75479f2"
DOWNLOAD_URL="https://s3-eu-west-1.amazonaws.com/softwaremill-public/${JAR}"
PORT="${ELASTICMQ_PORT:-9324}"
CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/elasticmq"
JAR_PATH="${CACHE_DIR}/${JAR}"
CONFIG_FILE="${CACHE_DIR}/elasticmq.conf"
PID_FILE="${CACHE_DIR}/elasticmq.pid"
PORT_FILE="${CACHE_DIR}/elasticmq.port"
LOG_FILE="${CACHE_DIR}/elasticmq.log"
BASE_URL="http://127.0.0.1:${PORT}"
WORK_DIR=""

cleanup() {
  if [[ -n "${WORK_DIR}" ]]; then rm -rf "${WORK_DIR}"; fi
}
trap cleanup EXIT

# Matches the other local runners: linux/amd64 with Java 17 or newer.
if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "run_local_elasticmq.sh supports linux/amd64 only (this host is $(uname -s)/$(uname -m))" >&2
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

# True once the SQS API answers ListQueues.
ready() {
  curl -fsS --max-time 2 -o /dev/null "${BASE_URL}/?Action=ListQueues" 2>/dev/null
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

# True when the PID file names a live java process.
running() {
  local pid
  [[ -f "${PID_FILE}" ]] || return 1
  pid="$(cat "${PID_FILE}")"
  kill -0 "${pid}" 2>/dev/null || return 1
  [[ ! -r "/proc/${pid}/comm" || "$(cat "/proc/${pid}/comm")" == "java" ]]
}

abort_start() {
  local pid="$1"
  shift
  echo "$*" >&2
  kill "${pid}" 2>/dev/null || true
  rm -f "${PID_FILE}" "${PORT_FILE}"
  exit 1
}

install_jar() {
  WORK_DIR="$(mktemp -d)"
  curl --proto "=https" --tlsv1.2 -fsSL -o "${WORK_DIR}/${JAR}" "${DOWNLOAD_URL}"
  if ! echo "${JAR_SHA256}  ${WORK_DIR}/${JAR}" | sha256sum -c --status; then
    echo "sha256 checksum mismatch for ${JAR}; refusing to install it" >&2
    exit 1
  fi
  mv "${WORK_DIR}/${JAR}" "${JAR_PATH}"
  rm -rf "${WORK_DIR}"
  WORK_DIR=""
}

mkdir -p "${CACHE_DIR}"
if running; then
  if [[ -f "${PORT_FILE}" && "$(cat "${PORT_FILE}")" != "${PORT}" ]]; then
    echo "ElasticMQ (pid $(cat "${PID_FILE}")) already runs on port $(cat "${PORT_FILE}"), not ${PORT}; stop it first" >&2
    exit 1
  fi
  echo "Reusing ElasticMQ (pid $(cat "${PID_FILE}")) on ${BASE_URL}"
  if ! wait_ready; then
    echo "ElasticMQ does not answer ListQueues on ${BASE_URL}; see ${LOG_FILE}" >&2
    exit 1
  fi
  exit 0
fi

if ready; then
  echo "another server already answers on ${BASE_URL}; stop it or set ELASTICMQ_PORT" >&2
  exit 1
fi
if [[ ! -f "${JAR_PATH}" ]] || ! echo "${JAR_SHA256}  ${JAR_PATH}" | sha256sum -c --status; then
  install_jar
fi

# Strict SQS limits; queues are created and deleted by each test, so none is declared here.
cat >"${CONFIG_FILE}" <<EOF
include classpath("application.conf")

node-address {
  protocol = http
  host = "127.0.0.1"
  port = ${PORT}
  context-path = ""
}

rest-sqs {
  enabled = true
  bind-port = ${PORT}
  bind-hostname = "127.0.0.1"
  sqs-limits = strict
}

rest-stats {
  enabled = false
}
EOF

nohup java -Dconfig.file="${CONFIG_FILE}" -jar "${JAR_PATH}" >"${LOG_FILE}" 2>&1 &
pid="$!"
echo "${pid}" >"${PID_FILE}"
echo "${PORT}" >"${PORT_FILE}"
if ! wait_ready "${pid}" || ! kill -0 "${pid}" 2>/dev/null; then
  abort_start "${pid}" "ElasticMQ did not start on ${BASE_URL}; see ${LOG_FILE}"
fi
if ! grep -q "ElasticMQ server (1.6.16) started" "${LOG_FILE}"; then
  abort_start "${pid}" "the server on ${BASE_URL} is not ElasticMQ 1.6.16; see ${LOG_FILE}"
fi
echo "Started ElasticMQ 1.6.16 (pid ${pid}) on ${BASE_URL}"
