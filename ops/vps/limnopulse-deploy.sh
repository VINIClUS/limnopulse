#!/usr/bin/env bash
# Installed by hand on the Hostinger VPS as /usr/local/bin/limnopulse-deploy
# (root:root, 0755) — Fase 1 bootstrap of the CI/CD plan. Never installed or
# modified by CI; this file is tracked here only so it is reviewable and has
# a single source of truth instead of being hand-typed on the server.
#
# Invoked exclusively via the limnopulse-deploy SSH forced-command
# (~limnopulse-deploy/.ssh/authorized_keys), mirroring the existing
# cnesdata-prod-deploy pattern already running on this VPS
# (/usr/local/bin/cnesdata-prod-deploy). The image tag is the SSH command
# itself (SSH_ORIGINAL_COMMAND), never a free-form shell.
set -euo pipefail

STACK_DIR="${LIMNOPULSE_STACK_DIR:-/opt/limnopulse}"
COMPOSE=(docker compose --env-file .env --env-file .env.image -f compose.production.yaml)
LOG="${LIMNOPULSE_DEPLOY_LOG:-/var/log/limnopulse-deploy.log}"
HEALTH_TIMEOUT_SECONDS="${LIMNOPULSE_HEALTH_TIMEOUT_SECONDS:-120}"

log() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$LOG"
}

TAG="${SSH_ORIGINAL_COMMAND:-}"
if ! [[ "$TAG" =~ ^main-[0-9a-f]{7,40}$ ]]; then
  log "rejected tag='$TAG' (must match main-<git-sha>)"
  exit 1
fi
if ! [[ "$HEALTH_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
  log "invalid health timeout (must be a nonnegative integer)"
  exit 1
fi

cd "$STACK_DIR"
log "deploy start tag=$TAG"

PREV_TAG=""
if [ -f .env.image ]; then
  PREV_TAG="$(grep -oP '(?<=^IMAGE_TAG=).*' .env.image || true)"
fi

rollback() {
  # Recovery failures must not recursively trigger another rollback.
  trap - ERR
  local rollback_failed=0
  log "deploy failed tag=$TAG, rolling back to prev=$PREV_TAG" || true
  if [ -n "$PREV_TAG" ]; then
    printf 'IMAGE_TAG=%s\n' "$PREV_TAG" > .env.image || rollback_failed=1
    "${COMPOSE[@]}" pull >>"$LOG" 2>&1 || rollback_failed=1
    "${COMPOSE[@]}" up -d --remove-orphans >>"$LOG" 2>&1 || rollback_failed=1
  else
    # Compose still needs the candidate env file while stopping its services.
    # Stop preserves the volumes and network; this is not a stack reset.
    "${COMPOSE[@]}" stop >>"$LOG" 2>&1 || rollback_failed=1
    rm -f -- .env.image || rollback_failed=1
  fi
  if [ "$rollback_failed" -ne 0 ]; then
    log "rollback failed; inspect the stack before retrying deployment" || true
  fi
  exit 1
}
trap rollback ERR

echo "IMAGE_TAG=$TAG" > .env.image

"${COMPOSE[@]}" pull >>"$LOG" 2>&1
"${COMPOSE[@]}" up -d --remove-orphans >>"$LOG" 2>&1

log "waiting for api health"
deadline=$((SECONDS + 10#$HEALTH_TIMEOUT_SECONDS))
until [ "$("${COMPOSE[@]}" ps -q api | xargs -r docker inspect -f '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    log "api did not become healthy within ${HEALTH_TIMEOUT_SECONDS}s"
    rollback
  fi
  sleep 3
done

trap - ERR
log "deploy ok tag=$TAG"

docker image prune -f --filter "until=168h" >>"$LOG" 2>&1 || true
