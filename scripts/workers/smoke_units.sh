#!/usr/bin/env bash
# ADR-020 S3, CI only: install a built workers bundle on the runner's real systemd and run
# each unit once, so its sandbox (DynamicUser, seccomp, MemoryDenyWriteExecute,
# LoadCredential=, the D-Bus socket) runs the real executable.
#
#   sudo scripts/workers/smoke_units.sh <tarball> <tag>
#
# - Every role but worker-monitor gets an empty env file and empty credentials. It must
#   exit 78 (a configuration error, not a crash or a seccomp kill), and a continuous unit
#   must not be restarted.
# - worker-monitor gets dummy keys and AWS_REGION. Nothing is enabled, so it reads every
#   unit over D-Bus, publishes nothing and must exit 0. It runs as the static user that
#   ops/workers/sysusers.d declares, which the script creates first, as the host does.
#
# It writes /opt/limnopulse/workers, /etc/limnopulse/workers and /etc/systemd/system, so it
# refuses to run outside GitHub Actions.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <tarball> <tag>" >&2
  exit 2
fi
if [[ "${GITHUB_ACTIONS:-}" != "true" || "$EUID" -ne 0 ]]; then
  echo "smoke_units: runs only as root on a disposable CI runner" >&2
  exit 2
fi

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
tarball="$1"
tag="$2"
workers=/opt/limnopulse/workers
config=/etc/limnopulse/workers
monitor=worker-monitor
failed=0

install -d -m 0755 "$workers/releases/$tag"
tar -xzf "$tarball" -C "$workers/releases/$tag" --no-same-owner
ln -sfn "releases/$tag" "$workers/current"
install -m 0644 "$workers/current/systemd/"* /etc/systemd/system/
install -d -m 0700 "$config" "$config/credentials"

# name, lifecycle, service unit, credentials (space separated), one role per line.
mapfile -t roles < <(python3 -I - "$workers/current/roles.json" <<'EOF'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    table = json.load(handle)
for role in table["roles"]:
    if role["lifecycle"] in ("continuous", "oneshot"):
        print(role["name"], role["lifecycle"], role["units"]["service"],
              " ".join(role["credentials"]), sep="\t")
EOF
)

for row in "${roles[@]}"; do
  IFS=$'\t' read -r name _ _ credentials <<<"$row"
  install -d -m 0700 "$config/credentials/$name"
  : >"$config/$name.env"
  for credential in $credentials; do
    if [[ "$name" == "$monitor" ]]; then
      printf 'smoke-not-a-key\n' >"$config/credentials/$name/$credential"
    else
      : >"$config/credentials/$name/$credential"
    fi
  done
done
printf 'AWS_REGION=us-east-1\n' >"$config/$monitor.env"
chmod 0600 "$config/"*.env
systemd-sysusers "$ROOT/ops/workers/sysusers.d/limnopulse-workers.conf"
systemctl daemon-reload

show() {
  systemctl show --property="$2" --value "$1"
}

expect() {
  local unit="$1" property="$2" want="$3" got
  got="$(show "$unit" "$property")"
  if [[ "$got" != "$want" ]]; then
    echo "::error::$unit $property=$got, want $want"
    failed=1
  fi
}

for row in "${roles[@]}"; do
  IFS=$'\t' read -r name lifecycle service _ <<<"$row"
  [[ "$name" == "$monitor" ]] && continue
  if systemctl start "$service"; then
    echo "::error::$service started with empty credentials"
    failed=1
  fi
  if [[ "$lifecycle" == "continuous" ]]; then
    # Longer than RestartSec, so a restart would have happened by now.
    sleep 8
    expect "$service" NRestarts 0
  fi
  expect "$service" ActiveState failed
  expect "$service" Result exit-code
  expect "$service" ExecMainCode 1
  expect "$service" ExecMainStatus 78
done

if ! systemctl start "limnopulse-$monitor.service"; then
  echo "::error::limnopulse-$monitor.service failed"
  failed=1
fi
expect "limnopulse-$monitor.service" Result success
expect "limnopulse-$monitor.service" ExecMainStatus 0
# A dynamic user would have been disconnected by the bus; the static one must be in use.
if ! getent passwd "limnopulse-$monitor" >/dev/null; then
  echo "::error::sysusers.d did not create limnopulse-$monitor"
  failed=1
fi

for row in "${roles[@]}"; do
  IFS=$'\t' read -r _ _ service _ <<<"$row"
  echo "::group::$service"
  journalctl --no-pager -o cat -u "$service" || true
  systemd-analyze security --no-pager "$service" | tail -n 1 || true
  echo "::endgroup::"
done
exit "$failed"
