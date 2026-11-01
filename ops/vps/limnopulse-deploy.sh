#!/usr/bin/env bash
# Installed by hand on the Hostinger VPS as /usr/local/bin/limnopulse-deploy
# (root:root, 0755) — Fase 1 bootstrap of the CI/CD plan. Never installed or
# modified by CI; this file is tracked here only so it is reviewable and has
# a single source of truth instead of being hand-typed on the server.
#
# Invoked exclusively via the limnopulse-deploy SSH forced-command
# (~limnopulse-deploy/.ssh/authorized_keys), mirroring the existing
# cnesdata-prod-deploy pattern already running on this VPS
# (/usr/local/bin/cnesdata-prod-deploy). The request is the SSH command itself
# (SSH_ORIGINAL_COMMAND): exactly `main-<40 hex> <64 hex>`, the release tag and
# the workers bundle SHA-256 (ADR-020), never a free-form shell.
#
# One release tag moves the API image, the profiled Compose workers and the
# workers bundle together (ops/workers/README.md, "Deploy"):
#   1. stage the bundle through the root-owned activation helper;
#   2. pull and start Compose with every profile in deploy.compose_profiles;
#   3. wait for api and each profiled worker to be healthy;
#   4. activate the bundle;
#   5. record IMAGE_TAG and WORKERS_BUNDLE_SHA256 in .env.image.
# A failure after step 1 rolls all three back to the release .env.image records.
# -E: the ERR trap (rollback) must also fire inside functions such as activation().
set -Eeuo pipefail
export LC_ALL=C

STACK_DIR="${LIMNOPULSE_STACK_DIR:-/opt/limnopulse}"
LOG="${LIMNOPULSE_DEPLOY_LOG:-/var/log/limnopulse-deploy.log}"
HEALTH_TIMEOUT_SECONDS="${LIMNOPULSE_HEALTH_TIMEOUT_SECONDS:-120}"
# activation.helper, deploy.forced_command and deploy.compose_profiles in
# ops/workers/roles.yaml, and the Compose service of each role in those profiles.
ACTIVATE=/usr/local/sbin/limnopulse-workers-activate
FORCED_COMMAND='^main-[0-9a-f]{40} [0-9a-f]{64}$'
PROFILES=(ingestion)
PROFILED_WORKERS=(telemetry-ingest-worker)

log() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$LOG"
}

# The only privileged call: the root-owned helper, through the sudoers rule. Its output is
# appended to the log as limnopulse-deploy, which owns the log.
activation() {
  sudo -n "$ACTIVATE" "$@"
}

profile_args=()
for profile in "${PROFILES[@]}"; do
  profile_args+=(--profile "$profile")
done
# .env.image records the last release that finished all five steps. The candidate tag
# reaches Compose through its own env file until step 5 records it.
COMPOSE=(docker compose --env-file .env --env-file .env.image -f compose.production.yaml "${profile_args[@]}")
CANDIDATE_COMPOSE=(docker compose --env-file .env --env-file .env.image.candidate -f compose.production.yaml "${profile_args[@]}")

# --- request validation; nothing above this line does deploy I/O ---
REQUEST="${SSH_ORIGINAL_COMMAND:-}"
if ! [[ "$REQUEST" =~ $FORCED_COMMAND ]]; then
  log "rejected request $(printf '%q' "${REQUEST:0:200}") (must be main-<40 hex> <64 hex>)"
  exit 1
fi
TAG="${REQUEST%% *}"
BUNDLE_SHA256="${REQUEST##* }"
if ! [[ "$HEALTH_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
  log "invalid health timeout (must be a nonnegative integer)"
  exit 1
fi

cd "$STACK_DIR"
exec 9>>.deploy.lock
if ! flock -n 9; then
  log "rejected tag=$TAG: another deploy is running"
  exit 1
fi
log "deploy start tag=$TAG bundle_sha256=$BUNDLE_SHA256"

PREV_TAG=""
PREV_BUNDLE_SHA256=""
if [ -f .env.image ]; then
  PREV_TAG="$(sed -n 's/^IMAGE_TAG=//p' .env.image | tail -n 1)"
  PREV_BUNDLE_SHA256="$(sed -n 's/^WORKERS_BUNDLE_SHA256=//p' .env.image | tail -n 1)"
fi

# 1. Stage. Nothing has changed yet, so a refusal (a missing or tampered bundle, a bad
# SHA256SUMS or a rollback floor) stops the deploy with the helper's exit code.
stage_status=0
activation stage "$TAG" "$BUNDLE_SHA256" >>"$LOG" 2>&1 || stage_status=$?
if [ "$stage_status" -ne 0 ]; then
  log "stage refused tag=$TAG status=$stage_status; nothing changed"
  exit "$stage_status"
fi

rollback() {
  # Recovery failures must not recursively trigger another rollback.
  trap - ERR
  local rollback_failed=0 bundle_status=0
  log "deploy failed tag=$TAG, rolling back to prev=${PREV_TAG:-none}" || true
  if [ -n "$PREV_TAG" ]; then
    "${COMPOSE[@]}" pull >>"$LOG" 2>&1 || rollback_failed=1
    "${COMPOSE[@]}" up -d --remove-orphans >>"$LOG" 2>&1 || rollback_failed=1
  else
    # Compose still needs the candidate env file while stopping its services.
    # Stop preserves the volumes and network; this is not a stack reset.
    "${CANDIDATE_COMPOSE[@]}" stop >>"$LOG" 2>&1 || rollback_failed=1
  fi
  rm -f -- .env.image.candidate .env.image.next || rollback_failed=1
  # Re-activate the previous release from its verified directory, or return to the
  # "no bundle" state when .env.image recorded none. Never with --override-floor.
  if [ -n "$PREV_BUNDLE_SHA256" ]; then
    activation activate "$PREV_TAG" >>"$LOG" 2>&1 || bundle_status=$?
  else
    activation deactivate >>"$LOG" 2>&1 || bundle_status=$?
  fi
  if [ "$bundle_status" -eq 3 ]; then
    log "the rollback floor refused the previous workers bundle; it needs a manual decision" || true
    rollback_failed=1
  elif [ "$bundle_status" -ne 0 ]; then
    log "workers bundle rollback failed status=$bundle_status" || true
    rollback_failed=1
  fi
  if [ "$rollback_failed" -ne 0 ]; then
    log "rollback failed; inspect the stack before retrying deployment" || true
  fi
  exit 1
}
trap rollback ERR

# 2. Compose, with every deploy profile, on the candidate tag.
printf 'IMAGE_TAG=%s\n' "$TAG" > .env.image.candidate
"${CANDIDATE_COMPOSE[@]}" pull >>"$LOG" 2>&1
"${CANDIDATE_COMPOSE[@]}" up -d --remove-orphans >>"$LOG" 2>&1

# 3. api and each profiled worker must report healthy.
unhealthy=""
all_healthy() {
  local service status
  for service in api "${PROFILED_WORKERS[@]}"; do
    status="$("${CANDIDATE_COMPOSE[@]}" ps -q "$service" | xargs -r docker inspect -f '{{.State.Health.Status}}' 2>/dev/null)" || status=""
    if [ "$status" != "healthy" ]; then
      unhealthy="$service"
      return 1
    fi
  done
}
log "waiting for api and ${PROFILED_WORKERS[*]} health"
deadline=$((SECONDS + 10#$HEALTH_TIMEOUT_SECONDS))
until all_healthy; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    log "$unhealthy did not become healthy within ${HEALTH_TIMEOUT_SECONDS}s"
    rollback
  fi
  sleep 3
done

# 4. Activate the bundle.
activation activate "$TAG" >>"$LOG" 2>&1

# 5. Record the release. Nothing after the rename may roll it back.
printf 'IMAGE_TAG=%s\nWORKERS_BUNDLE_SHA256=%s\n' "$TAG" "$BUNDLE_SHA256" > .env.image.next
mv -f -- .env.image.next .env.image
trap - ERR
rm -f -- .env.image.candidate || true
log "deploy ok tag=$TAG"

docker image prune -f --filter "until=168h" >>"$LOG" 2>&1 || true
