#!/usr/bin/env bash
# Build one ADR-020 worker executable and run it against the local emulators.
#
#   scripts/dev/run_worker.sh <role> [arguments...]
#
# <role> names a Go role in ops/workers/roles.yaml that has local settings in
# scripts/dev/workers/<role>.env. The script builds ./cmd/<role> into
# ${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/workers/bin with `${GO:-go} build`, exports
# each local setting the caller has not set already, and runs the executable with the
# remaining arguments. Production runs the same executables under systemd instead
# (ops/workers/systemd); Docker never runs a Go worker.
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
GO="${GO:-go}"
BIN_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/workers/bin"

fail() {
  printf 'run_worker: %s\n' "$1" >&2
  exit 2
}

[[ $# -ge 1 ]] || fail "usage: scripts/dev/run_worker.sh <role> [arguments...]"
role="$1"
shift
env_file="$ROOT/scripts/dev/workers/$role.env"
if [[ ! "$role" =~ ^[a-z][a-z0-9-]*$ || ! -f "$env_file" ]]; then
  fail "no local settings for role '$role' (scripts/dev/workers/<role>.env)"
fi

mkdir -p "$BIN_DIR"
(cd -- "$ROOT" && "$GO" build -trimpath -o "$BIN_DIR/$role" "./cmd/$role")

# A shell started by systemd or a CI runner must not make a local run look supervised:
# with these set, the worker would wait for LoadCredential= files or sd_notify.
unset NOTIFY_SOCKET WATCHDOG_USEC WATCHDOG_PID CREDENTIALS_DIRECTORY LIMNOPULSE_SUPERVISOR

while IFS= read -r line || [[ -n "$line" ]]; do
  [[ -z "$line" || "$line" == \#* ]] && continue
  key="${line%%=*}"
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ && "$line" == *=* ]] || fail "malformed line in $env_file: $line"
  # The caller's environment wins, so one run can point at another queue or table.
  [[ -v "$key" ]] || export "${line?}"
done <"$env_file"

exec "$BIN_DIR/$role" "$@"
