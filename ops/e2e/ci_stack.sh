#!/usr/bin/env bash
# Bring the local M1 stack up (or down) for .github/workflows/e2e-m1.yml.
#
#   bash ops/e2e/ci_stack.sh up    # compose deps, DynamoDB init/seed, M1 migration
#                                  # (dry-run, apply, apply), API with both M1 flags
#   bash ops/e2e/ci_stack.sh down  # stop API, remove compose deps and volumes
#
# `down` deletes the compose volumes (local DynamoDB, Influx, broker data), so
# it refuses to run unless CI=true or GITHUB_ACTIONS=true, or `down --force`
# is given explicitly.
#
# Local development normally uses the stack you already run; this script is
# for disposable CI runners. The API starts with API_V2_ENABLED=true and
# V1_POND_DEVICE_PROJECTION_ENABLED=true on 127.0.0.1:${API_PORT:-8000}.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STATE="${E2E_CI_STATE_DIR:-$ROOT/.e2e-out/ci}"
API_PORT="${API_PORT:-8000}"
TENANT="${LIMNOPULSE_TENANT:-tnt_local_001}"
SERVICES=(redis dynamodb-local influxdb mqtt-broker telegraf elasticmq)
MIGRATION_MODULE="scripts.migrations.m1_backfill_default_site_assets"
UV="${UV:-uv}"

log() { printf '[m1-ci] %s\n' "$*" >&2; }

wait_http() {  # wait_http <url> <seconds>
  local deadline=$((SECONDS + $2))
  until curl -fsS --max-time 3 "$1" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      log "timed out waiting for $1"
      return 1
    fi
    sleep 1
  done
}

wait_tcp() {  # wait_tcp <port> <seconds>
  local deadline=$((SECONDS + $2))
  until (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; do
    if ((SECONDS >= deadline)); then
      log "timed out waiting for 127.0.0.1:$1"
      return 1
    fi
    sleep 1
  done
}

up() {
  cd "$ROOT"
  mkdir -p "$STATE"
  [[ -f .env ]] || install -m 600 .env.example .env

  log "starting compose dependencies: ${SERVICES[*]}"
  # No --wait: telegraf has no healthcheck and may restart until influxdb
  # finishes its first-run setup. Poll the ports the harness needs instead.
  docker compose up -d "${SERVICES[@]}"
  wait_tcp 8001 60   # dynamodb-local
  wait_tcp 1883 60   # mosquitto
  wait_tcp 6379 60   # redis
  wait_http "http://127.0.0.1:8086/health" 90

  "$UV" run --locked --no-sync python scripts/dev/init_dynamodb.py
  "$UV" run --locked --no-sync python scripts/dev/seed_local.py

  # M1-06 backfill: dry-run, apply, then apply again (must be a no-op re-run).
  local mode
  for mode in dry-run apply apply-again; do
    local args=(--tenant "$TENANT" --page-size 25 --deadline-seconds 300)
    [[ "$mode" == "dry-run" ]] || args+=(--apply)
    log "M1 migration (${mode}) for ${TENANT}"
    "$UV" run --locked --no-sync python -m "$MIGRATION_MODULE" "${args[@]}" \
      | tee "$STATE/migration-${mode}.json"
  done
  # A second apply that writes anything is an idempotency regression.
  "$UV" run --locked --no-sync python "$ROOT/ops/e2e/assert_migration_noop.py" \
    <"$STATE/migration-apply-again.json"

  log "starting API on 127.0.0.1:${API_PORT}"
  # Run the venv's uvicorn directly (nohup execs it), so the recorded PID is
  # the server itself and not a `uv run` wrapper.
  local uvicorn_bin
  uvicorn_bin="$("$UV" run --locked --no-sync python -c 'import shutil; print(shutil.which("uvicorn") or "")')"
  if [[ -z "$uvicorn_bin" ]]; then
    log "uvicorn not found in the project environment"
    return 1
  fi
  API_V2_ENABLED=true V1_POND_DEVICE_PROJECTION_ENABLED=true APP_ENV=local AUTH_MODE=dev \
    nohup "$uvicorn_bin" limnopulse_api.main:app \
    --host 127.0.0.1 --port "$API_PORT" >"$STATE/uvicorn.log" 2>&1 &
  # "<pid> <start time>": stop_api only signals the PID while it still has
  # this /proc start time, never a process that reused it.
  printf '%s %s\n' "$!" "$(pid_start "$!" || true)" >"$STATE/uvicorn.pid"
  if ! wait_http "http://127.0.0.1:${API_PORT}/readyz" 90; then
    tail -n 80 "$STATE/uvicorn.log" >&2 || true
    return 1
  fi
  log "stack ready (uvicorn pid $(cut -d' ' -f1 "$STATE/uvicorn.pid"))"
}

pid_start() {
  local stat
  stat="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1
  stat="${stat##*) }"
  # After "comm) " the state is field 3, so the start time is word 20.
  set -- $stat
  printf '%s\n' "${20}"
}

stop_api() {
  local pids=()
  if [[ -f "$STATE/uvicorn.pid" ]]; then
    local saved_pid saved_start
    read -r saved_pid saved_start <"$STATE/uvicorn.pid" || true
    if [[ -n "${saved_pid:-}" && -n "${saved_start:-}" \
      && "$(pid_start "$saved_pid" || true)" == "$saved_start" ]]; then
      pids+=("$saved_pid")
    fi
  fi
  # Also stop by command line/port, in case the pidfile is stale or missing.
  local pid
  while read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done < <(pgrep -f "uvicorn limnopulse_api.main:app --host 127.0.0.1 --port ${API_PORT}" || true)
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.25
    done
    kill -9 "$pid" 2>/dev/null || true
  done
  rm -f "$STATE/uvicorn.pid"
  if curl -fsS --max-time 2 "http://127.0.0.1:${API_PORT}/healthz" >/dev/null 2>&1; then
    log "an API still answers on 127.0.0.1:${API_PORT}"
    return 1
  fi
}

down() {
  if [[ "${1:-}" != "--force" && "${CI:-}" != "true" && "${GITHUB_ACTIONS:-}" != "true" ]]; then
    log "refusing to run 'down' outside CI: it removes the compose services AND volumes"
    log "set CI=true / GITHUB_ACTIONS=true (disposable runner) or pass: $0 down --force"
    return 2
  fi
  cd "$ROOT"
  stop_api
  docker compose down -v --remove-orphans || true
}

case "${1:-}" in
  up) up ;;
  down) down "${2:-}" ;;
  *)
    echo "usage: $0 up|down [--force]" >&2
    exit 2
    ;;
esac
