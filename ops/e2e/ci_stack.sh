#!/usr/bin/env bash
# Bring the local M1 stack up (or down) for .github/workflows/e2e-m1.yml.
#
#   bash ops/e2e/ci_stack.sh up    # compose deps, DynamoDB init/seed, M1 migration
#                                  # (dry-run, apply, apply), API with both M1 flags
#   bash ops/e2e/ci_stack.sh down  # stop API, remove compose deps and volumes
#
# Local development normally uses the stack you already run; this script is
# for disposable CI runners. The API starts with API_V2_ENABLED=true and
# V1_POND_DEVICE_PROJECTION_ENABLED=true on 127.0.0.1:${API_PORT:-8000}.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STATE="${E2E_CI_STATE_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/limnopulse-m1-ci}"
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

  log "starting API on 127.0.0.1:${API_PORT}"
  API_V2_ENABLED=true V1_POND_DEVICE_PROJECTION_ENABLED=true APP_ENV=local AUTH_MODE=dev \
    nohup "$UV" run --locked --no-sync uvicorn limnopulse_api.main:app \
    --host 127.0.0.1 --port "$API_PORT" >"$STATE/uvicorn.log" 2>&1 &
  echo $! >"$STATE/uvicorn.pid"
  if ! wait_http "http://127.0.0.1:${API_PORT}/readyz" 90; then
    tail -n 80 "$STATE/uvicorn.log" >&2 || true
    return 1
  fi
  log "stack ready"
}

down() {
  cd "$ROOT"
  if [[ -f "$STATE/uvicorn.pid" ]]; then
    kill "$(cat "$STATE/uvicorn.pid")" 2>/dev/null || true
    rm -f "$STATE/uvicorn.pid"
  fi
  docker compose down -v --remove-orphans || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *)
    echo "usage: $0 up|down" >&2
    exit 2
    ;;
esac
