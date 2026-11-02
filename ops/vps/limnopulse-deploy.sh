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
#   2. pull and start Compose with each profile in deploy.compose_profiles whose worker
#      is enabled on the host, after stopping and removing each disabled worker;
#   3. wait for api and each enabled profiled worker to be healthy;
#   4. activate the bundle;
#   5. record IMAGE_TAG and WORKERS_BUNDLE_SHA256 in .env.image.
# A failure after step 1 rolls all three back to the release .env.image records.
# -E: the ERR trap (rollback) must also fire inside functions such as activation().
set -Eeuo pipefail
export LC_ALL=C
# Compose also reads COMPOSE_PROFILES from .env, which would start a disabled worker; only the
# --profile flags this script passes may choose profiles. An empty value overrides the file.
export COMPOSE_PROFILES=

STACK_DIR="${LIMNOPULSE_STACK_DIR:-/opt/limnopulse}"
LOG="${LIMNOPULSE_DEPLOY_LOG:-/var/log/limnopulse-deploy.log}"
HEALTH_TIMEOUT_SECONDS="${LIMNOPULSE_HEALTH_TIMEOUT_SECONDS:-120}"
# activation.helper, deploy.forced_command and deploy.compose_profiles in
# ops/workers/roles.yaml. PROFILED_WORKERS has one "compose_service compose_profile
# host_enable_env" entry per Compose role in those profiles.
ACTIVATE=/usr/local/sbin/limnopulse-workers-activate
FORCED_COMMAND='^main-[0-9a-f]{40} [0-9a-f]{64}$'
PROFILES=(ingestion)
PROFILED_WORKERS=("telemetry-ingest-worker ingestion TELEMETRY_V2_WRITE_ENABLED")

# Reads `docker compose config --format json` and prints "<service> enabled|disabled" for
# each PROFILED_WORKERS entry. A host switch is on or off exactly as the worker's settings
# parse a boolean (pydantic); any other value, or a missing one, is refused.
SWITCHES_PY='
import json, sys
ON = {"1", "on", "t", "true", "y", "yes"}
OFF = {"0", "off", "f", "false", "n", "no"}
services = json.load(sys.stdin)["services"]
for entry in sys.argv[1:]:
    service, _profile, variable = entry.split()
    value = (services.get(service, {}).get("environment") or {}).get(variable)
    word = value.lower() if isinstance(value, str) else None
    if word not in ON | OFF:
        sys.exit(f"{service}: {variable} must be a boolean, got {value!r}")
    print(service, "enabled" if word in ON else "disabled")
'

log() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$LOG"
}

# The only privileged call: the root-owned helper, through the sudoers rule. Its output is
# appended to the log as limnopulse-deploy, which owns the log.
activation() {
  sudo -n "$ACTIVATE" "$@"
}

all_profile_args=()
for profile in "${PROFILES[@]}"; do
  all_profile_args+=(--profile "$profile")
done
# .env.image records the last release that finished all five steps. The candidate tag
# reaches Compose through its own env file until step 5 records it.
COMPOSE=(docker compose --env-file .env --env-file .env.image -f compose.production.yaml)
CANDIDATE_COMPOSE=(docker compose --env-file .env --env-file .env.image.candidate -f compose.production.yaml)

contains() {
  local needle="$1" item
  shift
  for item in "$@"; do
    [ "$item" = "$needle" ] && return 0
  done
  return 1
}

# resolve_workers <compose command...>: set ENABLED_WORKERS and DISABLED_WORKERS from the
# host switches as Compose resolves them. Every deploy profile is active for the read, so
# each worker appears in the config. On failure both lists stay as they were.
ENABLED_WORKERS=()
DISABLED_WORKERS=()
resolve_workers() {
  local resolved service state
  resolved="$("$@" "${all_profile_args[@]}" config --format json 2>>"$LOG" |
    python3 -I -c "$SWITCHES_PY" "${PROFILED_WORKERS[@]}" 2>>"$LOG")" || return 1
  ENABLED_WORKERS=()
  DISABLED_WORKERS=()
  while read -r service state; do
    [ -n "$service" ] || continue
    if [ "$state" = enabled ]; then
      ENABLED_WORKERS+=("$service")
    else
      DISABLED_WORKERS+=("$service")
    fi
  done <<<"$resolved"
}

# start_release [--keep-going] <compose command...>: pull, stop and remove each disabled
# worker, then start the stack with the profiles of the enabled ones. Plain `up` without a
# profile would leave a disabled worker's old container running. A promotion stops at the first
# failure. A rollback (--keep-going) logs a failed pull, starts the previous tag from the images
# still cached, and then returns the failure.
start_release() {
  local keep_going=0 pull_failed=0 entry service profile enabled_profiles=()
  if [ "$1" = --keep-going ]; then
    keep_going=1
    shift
  fi
  for entry in "${PROFILED_WORKERS[@]}"; do
    read -r service profile _ <<<"$entry"
    if contains "$service" "${ENABLED_WORKERS[@]}"; then
      enabled_profiles+=(--profile "$profile")
    fi
  done
  if ! "$@" "${enabled_profiles[@]}" pull >>"$LOG" 2>&1; then
    [ "$keep_going" -eq 1 ] || return 1
    log "pull failed; starting from the cached images" || true
    pull_failed=1
  fi
  for entry in "${PROFILED_WORKERS[@]}"; do
    read -r service profile _ <<<"$entry"
    if contains "$service" "${DISABLED_WORKERS[@]}"; then
      "$@" --profile "$profile" rm --stop --force "$service" >>"$LOG" 2>&1 || return 1
    fi
  done
  "$@" "${enabled_profiles[@]}" up -d --remove-orphans >>"$LOG" 2>&1 || return 1
  # A disabled worker must be gone, however Compose chose its profiles.
  local remaining
  for entry in "${PROFILED_WORKERS[@]}"; do
    read -r service profile _ <<<"$entry"
    if contains "$service" "${DISABLED_WORKERS[@]}"; then
      remaining="$("$@" --profile "$profile" ps -a -q "$service" 2>>"$LOG")" || return 1
      if [ -n "$remaining" ]; then
        log "disabled worker $service still has a container after up" || true
        return 1
      fi
    fi
  done
  return "$pull_failed"
}

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

# The recorded release must be well formed and be the helper's current bundle (or none when it
# recorded no bundle). Rollback can then restore it, and staging never prunes it.
# Releases from before ADR-020 may have short tags.
host_state_problem() {
  local status current
  if [ -f .env.image ] && ! [[ "$PREV_TAG" =~ ^main-[0-9a-f]{7,40}$ ]]; then
    echo ".env.image has no valid IMAGE_TAG"
  elif [ -n "$PREV_BUNDLE_SHA256" ] && ! [[ "$PREV_TAG $PREV_BUNDLE_SHA256" =~ $FORCED_COMMAND ]]; then
    echo ".env.image has an invalid bundle tag or WORKERS_BUNDLE_SHA256"
  elif ! status="$(activation status 2>>"$LOG")"; then
    echo "the activation helper cannot report its status"
  else
    current="$(sed -n 's/^current //p' <<<"$status")"
    if [ -z "$PREV_BUNDLE_SHA256" ] && [ "$current" != none ]; then
      echo ".env.image records no bundle but the helper's current is $current"
    elif [ -n "$PREV_BUNDLE_SHA256" ] &&
      ! grep -qxF "release $PREV_TAG sha256=$PREV_BUNDLE_SHA256 current" <<<"$status"; then
      echo "the helper's current is $current, not the recorded $PREV_TAG bundle"
    fi
  fi
}
problem="$(host_state_problem)"
if [ -n "$problem" ]; then
  log "rejected tag=$TAG: host state inconsistent ($problem); fix it by hand; nothing changed"
  exit 1
fi

# The host switches decide which profiled workers this release starts and waits for.
printf 'IMAGE_TAG=%s\n' "$TAG" > .env.image.candidate
if ! resolve_workers "${CANDIDATE_COMPOSE[@]}"; then
  rm -f -- .env.image.candidate
  log "rejected tag=$TAG: cannot resolve the profiled workers' host switches; nothing changed"
  exit 1
fi
log "profiled workers enabled: ${ENABLED_WORKERS[*]:-none}; disabled: ${DISABLED_WORKERS[*]:-none}"

# 1. Stage. Nothing has changed yet, so a refusal (a missing or tampered bundle, a bad
# SHA256SUMS or a rollback floor) stops the deploy with the helper's exit code.
stage_status=0
activation stage "$TAG" "$BUNDLE_SHA256" >>"$LOG" 2>&1 || stage_status=$?
if [ "$stage_status" -ne 0 ]; then
  rm -f -- .env.image.candidate
  log "stage refused tag=$TAG status=$stage_status; nothing changed"
  exit "$stage_status"
fi

ACTIVATION_STARTED=0
rollback() {
  # Recovery failures must not recursively trigger another rollback.
  trap - ERR
  local rollback_failed=0 bundle_status=0
  log "deploy failed tag=$TAG, rolling back to prev=${PREV_TAG:-none}" || true
  if [ -n "$PREV_TAG" ]; then
    # The same rule as the promotion, with the host switches as they are now.
    if ! resolve_workers "${COMPOSE[@]}"; then
      log "cannot re-read the host switches; rolling back with the values read at promotion" || true
    fi
    start_release --keep-going "${COMPOSE[@]}" || rollback_failed=1
  else
    # Compose still needs the candidate env file while stopping its services.
    # Stop preserves the volumes and network; this is not a stack reset.
    "${CANDIDATE_COMPOSE[@]}" "${all_profile_args[@]}" stop >>"$LOG" 2>&1 || rollback_failed=1
  fi
  rm -f -- .env.image.candidate .env.image.next || rollback_failed=1
  # Re-activate the previous release from its verified directory, or return to the
  # "no bundle" state when .env.image recorded none. Never with --override-floor. Before
  # step 4 the bundle has not moved: the pre-stage check proved it is the recorded one.
  if [ "$ACTIVATION_STARTED" -eq 0 ]; then
    log "the workers bundle never moved from ${PREV_TAG:-none}" || true
  elif [ -n "$PREV_BUNDLE_SHA256" ]; then
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

# 2. Compose on the candidate tag, with the profiles of the enabled workers.
start_release "${CANDIDATE_COMPOSE[@]}"

# 3. api and each enabled profiled worker must report healthy. A parked worker never does.
unhealthy=""
all_healthy() {
  local service status
  for service in api "${ENABLED_WORKERS[@]}"; do
    status="$("${CANDIDATE_COMPOSE[@]}" "${all_profile_args[@]}" ps -q "$service" | xargs -r docker inspect -f '{{.State.Health.Status}}' 2>/dev/null)" || status=""
    if [ "$status" != "healthy" ]; then
      unhealthy="$service"
      return 1
    fi
  done
}
log "waiting for the health of api ${ENABLED_WORKERS[*]}"
deadline=$((SECONDS + 10#$HEALTH_TIMEOUT_SECONDS))
until all_healthy; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    log "$unhealthy did not become healthy within ${HEALTH_TIMEOUT_SECONDS}s"
    rollback
  fi
  sleep 3
done

# 4. Activate the bundle. From here on, a rollback must move the bundle back too. The rollback
# runs here, not from the ERR trap inside the redirected call, so its log lines reach the
# workflow once.
ACTIVATION_STARTED=1
activation activate "$TAG" >>"$LOG" 2>&1 || rollback

# 5. Record the release. Nothing after the rename may roll it back.
printf 'IMAGE_TAG=%s\nWORKERS_BUNDLE_SHA256=%s\n' "$TAG" "$BUNDLE_SHA256" > .env.image.next
mv -f -- .env.image.next .env.image
trap - ERR
rm -f -- .env.image.candidate || true
log "deploy ok tag=$TAG"

docker image prune -f --filter "until=168h" >>"$LOG" 2>&1 || true
