#!/usr/bin/env bash
# LimnoPulse M1 end-to-end check: replayed probe readings -> MQTT -> Node-RED
# verdict flow -> API (/v1 device, /v2 component/deployments/asset) -> verdicts.
#
# Assumes the local stack (mosquitto, telegraf, influx, DynamoDB) and the API
# are already running and seeded. Starts Node-RED headless and an mqttx
# subscriber in the background, replays readings via ops/e2e/wokwi/replay.py,
# then runs the API-driven scenarios (West->East relocation of a new /v1
# device, a native /v2 device) and summarizes verdicts. The relocation scenario
# needs V1_POND_DEVICE_PROJECTION_ENABLED=true and creates a pond and devices
# on every run, so point it at a disposable table. Re-runnable: stale processes
# from a previous run are stopped and the work directory is recreated;
# background processes are always cleaned up on exit.
#
# Environment (defaults):
#   LIMNOPULSE_API_URL=http://127.0.0.1:8000  LIMNOPULSE_TENANT=tnt_local_001
#   MQTT_HOST=127.0.0.1  MQTT_PORT=1883       E2E_DEVICE_ID=local-device-001
#   NODE_RED_PORT=18800  E2E_TIMEOUT=45
#   E2E_WORK_DIR=<repo>/.e2e-out/m1-e2e  (must stay inside the repository: the
#                          Python tools only read and write files under the
#                          working directory, which is the repository root)
#   WOKWI_SERIAL_LOG=<path> (else ops/e2e/wokwi/serial.log, else the fixture)
#   E2E_TS_MODE=now        (keep = publish the logged ts unchanged)
#   ALLOW_V2_UNAVAILABLE=0  (1 accepts ok=null / reason v2-unavailable)
#   E2E_SCENARIOS="relocation v2-native"  (ops/e2e/scenarios.py; "" = replay only)
#   E2E_WEST_POND_ID=pond_local_001  LIMNOPULSE_DEV_USER_SUB/EMAIL (dev auth headers)
#   PYTHON=python3  NODE_RED_BIN=node-red
#   MQTTX_BIN=mqttx (its directory is put first on PATH; the Python tools run
#                    the `mqttx` found there)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
E2E_DIR="$ROOT/ops/e2e"
# The Python tools confine their file arguments to the working directory.
cd "$ROOT"

export LIMNOPULSE_API_URL="${LIMNOPULSE_API_URL:-http://127.0.0.1:8000}"
export LIMNOPULSE_TENANT="${LIMNOPULSE_TENANT:-tnt_local_001}"
export MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export NODE_RED_PORT="${NODE_RED_PORT:-18800}"
E2E_DEVICE_ID="${E2E_DEVICE_ID:-local-device-001}"
E2E_TIMEOUT="${E2E_TIMEOUT:-45}"
ALLOW_V2_UNAVAILABLE="${ALLOW_V2_UNAVAILABLE:-0}"
E2E_SCENARIOS="${E2E_SCENARIOS-relocation v2-native}"
PYTHON="${PYTHON:-python3}"
NODE_RED_BIN="${NODE_RED_BIN:-node-red}"
MQTTX_BIN="${MQTTX_BIN:-mqttx}"
WORK="$(realpath -m -- "${E2E_WORK_DIR:-$ROOT/.e2e-out/m1-e2e}")"
case "$WORK" in
  "$ROOT"/?*) ;;
  *)
    printf '[m1-e2e] E2E_WORK_DIR must be inside the repository (%s): %s\n' "$ROOT" "$WORK" >&2
    exit 2
    ;;
esac
PIDFILE="$WORK.pids"
VERDICT_TOPIC="limnopulse/e2e/verdicts"

# Loopback API calls must never go through an outbound proxy.
export NO_PROXY="127.0.0.1,localhost${NO_PROXY:+,$NO_PROXY}"
export no_proxy="$NO_PROXY"

log() { printf '[m1-e2e] %s\n' "$*" >&2; }

stop_pids() {
  [[ -f "$PIDFILE" ]] || return 0
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.2
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
  done <"$PIDFILE"
  rm -f "$PIDFILE"
}

cleanup() {
  local status=$?
  stop_pids
  if [[ $status -ne 0 && -f "$WORK/node-red.log" ]]; then
    log "node-red log tail:"
    tail -n 40 "$WORK/node-red.log" >&2 || true
  fi
  exit "$status"
}

require() {
  command -v "$1" >/dev/null 2>&1 || { log "missing required command: $1"; exit 2; }
}

wait_for() {  # wait_for <seconds> <command...>
  local deadline=$((SECONDS + $1))
  shift
  until "$@"; do
    if ((SECONDS >= deadline)); then
      return 1
    fi
    sleep 0.5
  done
}

require "$PYTHON"
require "$NODE_RED_BIN"
require "$MQTTX_BIN"
require curl
# replay.py and scenarios.py run `mqttx` from PATH (a fixed command name).
mqttx_path="$(command -v "$MQTTX_BIN")"
if [[ "$(basename "$mqttx_path")" != "mqttx" ]]; then
  log "MQTTX_BIN must point at an executable named mqttx: $mqttx_path"
  exit 2
fi
export PATH="$(dirname "$mqttx_path"):$PATH"

# Idempotency: stop anything a previous (interrupted) run left behind.
stop_pids
rm -rf "$WORK"
mkdir -p "$WORK/node-red-user"
trap cleanup EXIT INT TERM

log "API ${LIMNOPULSE_API_URL} tenant ${LIMNOPULSE_TENANT} broker ${MQTT_HOST}:${MQTT_PORT}"
if ! curl -fsS --max-time 5 "${LIMNOPULSE_API_URL}/healthz" >/dev/null; then
  log "API not reachable at ${LIMNOPULSE_API_URL}/healthz"
  exit 2
fi

"$PYTHON" "$E2E_DIR/node-red/build_flows.py" --check

if (exec 3<>"/dev/tcp/127.0.0.1/${NODE_RED_PORT}") 2>/dev/null; then
  log "port ${NODE_RED_PORT} is already in use (stale Node-RED from a run with another E2E_WORK_DIR?)"
  log "stop it or set NODE_RED_PORT to a free port"
  exit 2
fi

NODE_RED_USER_DIR="$WORK/node-red-user" "$NODE_RED_BIN" \
  --settings "$E2E_DIR/node-red/settings.js" \
  --userDir "$WORK/node-red-user" \
  --port "$NODE_RED_PORT" \
  >"$WORK/node-red.log" 2>&1 &
node_red_pid=$!
echo "$node_red_pid" >>"$PIDFILE"

node_red_connected() {
  kill -0 "$node_red_pid" 2>/dev/null || return 2
  grep -q "Connected to broker" "$WORK/node-red.log"
}
node_red_ready() {
  node_red_connected
  local rc=$?
  if [[ $rc -eq 2 ]]; then
    log "Node-RED exited during startup"
    exit 1
  fi
  return "$rc"
}
if ! wait_for 60 node_red_ready; then
  log "Node-RED did not connect to the broker"
  exit 1
fi
if grep -Eq "\[error\]" "$WORK/node-red.log"; then
  log "Node-RED reported errors while starting"
  exit 1
fi
log "Node-RED flow running (admin http://127.0.0.1:${NODE_RED_PORT}/)"

"$MQTTX_BIN" sub -h "$MQTT_HOST" -p "$MQTT_PORT" -t "$VERDICT_TOPIC" -q 1 \
  --output-mode clean >"$WORK/verdicts.raw" 2>"$WORK/mqttx-sub.log" &
echo $! >>"$PIDFILE"
# The mqtt-in subscription is made right after "Connected to broker"; give
# both subscriptions a moment before publishing (QoS1, non-retained).
sleep "${E2E_SUBSCRIBE_SETTLE:-2}"

replay_args=(--device-id "$E2E_DEVICE_ID" --host "$MQTT_HOST" --port "$MQTT_PORT" --ts-mode "${E2E_TS_MODE:-now}")
if [[ -n "${WOKWI_SERIAL_LOG:-}" ]]; then
  replay_args+=(--serial-log "$WOKWI_SERIAL_LOG")
fi
replay_out="$("$PYTHON" "$E2E_DIR/wokwi/replay.py" "${replay_args[@]}" 2>"$WORK/replay.log")" || {
  cat "$WORK/replay.log" >&2
  exit 1
}
cat "$WORK/replay.log" >&2
expected="$(printf '%s\n' "$replay_out" | awk '/^REPLAYED /{print $2}')"
expected="${expected:-0}"
if [[ ! "$expected" =~ ^[0-9]+$ || "$expected" -eq 0 ]]; then
  log "replay.py reported no REPLAYED line (or 0 readings); output was:"
  printf '%s\n' "$replay_out" >&2
  exit 1
fi
log "replayed ${expected} reading(s) for ${E2E_DEVICE_ID}"

# Every published reading is identified by (device_id, seq, ts); the summary
# needs exactly one (agreeing) verdict per reading and none for anything else.
published_args() {  # published_args <tool output>
  local _tag device seq ts
  while read -r _tag device seq ts; do
    printf '%s\n' "--expect-reading" "${device},${seq},${ts}"
  done < <(printf '%s\n' "$1" | grep '^PUBLISHED ' || true)
}
mapfile -t replay_expect < <(published_args "$replay_out")
if [[ ${#replay_expect[@]} -ne $((expected * 2)) ]]; then
  log "replay.py printed ${#replay_expect[@]} PUBLISHED argument(s) for ${expected} reading(s)"
  exit 1
fi

count_verdicts() {
  "$PYTHON" "$E2E_DIR/summarize_verdicts.py" --raw "$WORK/verdicts.raw" \
    --device-id "$E2E_DEVICE_ID" "${replay_expect[@]}" --count-only
}
enough_verdicts() { [[ "$(count_verdicts)" -ge "$expected" ]]; }
wait_for "$E2E_TIMEOUT" enough_verdicts || log "timed out waiting for ${expected} verdict(s)"

summary_args=(--raw "$WORK/verdicts.raw" --device-id "$E2E_DEVICE_ID" "${replay_expect[@]}" --jsonl-out "$WORK/verdicts.replay.jsonl")
scenario_args=()
if [[ "$ALLOW_V2_UNAVAILABLE" == "1" ]]; then
  summary_args+=(--allow-v2-unavailable)
  scenario_args+=(--allow-v2-unavailable)
fi
set +e
log "replay verdicts (${E2E_DEVICE_ID}):"
"$PYTHON" "$E2E_DIR/summarize_verdicts.py" "${summary_args[@]}"
result=$?
set -e

# API-driven scenarios (ops/e2e/scenarios.py): West->East relocation of a NEW
# /v1 device and a native /v2 device. local-device-001 is never moved.
devices=(--device-id "$E2E_DEVICE_ID")
all_expect=("${replay_expect[@]}")
scenarios_failed=0
for scenario in $E2E_SCENARIOS; do
  log "scenario ${scenario}"
  set +e
  scenario_out="$("$PYTHON" "$E2E_DIR/scenarios.py" "$scenario" --raw "$WORK/verdicts.raw" \
    --timeout "$E2E_TIMEOUT" "${scenario_args[@]}")"
  scenario_rc=$?
  set -e
  printf '%s\n' "$scenario_out" | grep -Ev '^(SCENARIO_DEVICE|PUBLISHED) ' || true
  if [[ $scenario_rc -ne 0 ]]; then
    log "scenario ${scenario} failed (exit ${scenario_rc})"
    scenarios_failed=$((scenarios_failed + 1))
    result=1
  fi
  while read -r _ device _count; do
    devices+=(--device-id "$device")
  done < <(printf '%s\n' "$scenario_out" | grep '^SCENARIO_DEVICE ' || true)
  mapfile -t scenario_expect < <(published_args "$scenario_out")
  all_expect+=("${scenario_expect[@]}")
done

# Failed scenarios still report their devices (SCENARIO_DEVICE), and each one
# counts as a failure, so the final SUMMARY shows failures>0 whenever a
# scenario made this script fail.
final_args=(--raw "$WORK/verdicts.raw" "${devices[@]}" "${all_expect[@]}" --jsonl-out "$WORK/verdicts.jsonl" --scenarios-failed "$scenarios_failed")
if [[ "$ALLOW_V2_UNAVAILABLE" == "1" ]]; then
  final_args+=(--allow-v2-unavailable)
fi
log "all verdicts:"
set +e
"$PYTHON" "$E2E_DIR/summarize_verdicts.py" "${final_args[@]}"
final=$?
set -e
if [[ $result -eq 0 ]]; then
  result=$final
fi
log "verdicts: $WORK/verdicts.jsonl (exit ${result})"
exit "$result"
