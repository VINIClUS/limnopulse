#!/usr/bin/env bash

set -euo pipefail

LAB_MARKER_PATH="${LAB_MARKER_PATH:-/etc/vinisantana-lab}"
LIMNOPULSE_LAB_COMPOSE_PROJECT="${LIMNOPULSE_LAB_COMPOSE_PROJECT:-limnopulse_lab}"
export COMPOSE_PROJECT_NAME="$LIMNOPULSE_LAB_COMPOSE_PROJECT"
UV_VERSION="0.11.28"
UV_ARCHIVE_SHA256="e490a6464492183c5d4534a5527fb4440f7f2bb2f228162ad7e4afe076dc0224"
# ADR-020: the lab builds the Go workers it runs (scripts/dev/run_worker.sh) with this
# toolchain, the latest patch of go.mod's Go minor; the checksum is go.dev/dl's.
GO_VERSION="1.26.10"
GO_ARCHIVE_SHA256="ccfe7ae822d2c2f41f4fe7c956199967a4dac22d0ee0d2b2bfdb440aab3b20c5"

fail() {
  local message="$1"
  local exit_code="${2:-1}"
  printf 'limnopulse lab: %s\n' "$message" >&2
  exit "$exit_code"
}

require_lab_marker() {
  if [[ ! -f "$LAB_MARKER_PATH" ]] || \
    ! grep -Fqx "managed_by: debian-vps-lab" "$LAB_MARKER_PATH" || \
    ! grep -Fqx "environment: lab" "$LAB_MARKER_PATH" || \
    ! grep -Fqx "target: limnopulse" "$LAB_MARKER_PATH"; then
    fail "lab marker is required at $LAB_MARKER_PATH" 9
  fi
}

require_command() {
  local command="$1"
  command -v "$command" >/dev/null 2>&1 || fail "required command is unavailable: $command" 3
}

lab_root() {
  if [[ -n "${LAB_ROOT:-}" ]]; then
    [[ -d "$LAB_ROOT" ]] || fail "LAB_ROOT is not a directory: $LAB_ROOT" 3
    cd -- "$LAB_ROOT"
    pwd -P
    return
  fi
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
  pwd -P
}

lab_uv() {
  local root
  root="$(lab_root)"
  printf '%s\n' "${LIMNOPULSE_LAB_UV:-$root/.lab-tools/uv-$UV_VERSION/uv}"
}

ensure_lab_uv() {
  local root uv_bin archive extract_dir
  root="$(lab_root)"
  uv_bin="$(lab_uv)"
  [[ -x "$uv_bin" ]] && return
  require_command curl
  require_command sha256sum
  require_command tar
  archive="$root/.lab-tools/uv-$UV_VERSION.tar.gz"
  extract_dir="$root/.lab-tools/uv-$UV_VERSION"
  install -d -m 755 "$root/.lab-tools"
  curl --fail --location --silent --show-error \
    "https://github.com/astral-sh/uv/releases/download/$UV_VERSION/uv-x86_64-unknown-linux-gnu.tar.gz" \
    --output "$archive"
  printf '%s  %s\n' "$UV_ARCHIVE_SHA256" "$archive" | sha256sum --check --status || \
    fail "uv archive checksum mismatch" 3
  rm -rf "$extract_dir"
  tar -xzf "$archive" -C "$root/.lab-tools"
  mv "$root/.lab-tools/uv-x86_64-unknown-linux-gnu" "$extract_dir"
  [[ -x "$uv_bin" ]] || fail "uv bootstrap did not provide $uv_bin" 3
}

lab_go() {
  local root
  root="$(lab_root)"
  printf '%s\n' "${LIMNOPULSE_LAB_GO:-$root/.lab-tools/go-$GO_VERSION/bin/go}"
}

ensure_lab_go() {
  local root go_bin archive extract_dir
  root="$(lab_root)"
  go_bin="$(lab_go)"
  [[ -x "$go_bin" ]] && return
  require_command curl
  require_command sha256sum
  require_command tar
  archive="$root/.lab-tools/go-$GO_VERSION.tar.gz"
  extract_dir="$root/.lab-tools/go-$GO_VERSION"
  install -d -m 755 "$root/.lab-tools"
  curl --fail --location --silent --show-error \
    "https://go.dev/dl/go$GO_VERSION.linux-amd64.tar.gz" \
    --output "$archive"
  printf '%s  %s\n' "$GO_ARCHIVE_SHA256" "$archive" | sha256sum --check --status || \
    fail "Go archive checksum mismatch" 3
  rm -rf "$extract_dir" "$root/.lab-tools/go"
  tar -xzf "$archive" -C "$root/.lab-tools"
  mv "$root/.lab-tools/go" "$extract_dir"
  [[ -x "$go_bin" ]] || fail "Go bootstrap did not provide $go_bin" 3
}

wait_for_compose_services() {
  local service
  for _ in {1..30}; do
    local all_running=true
    for service in "$@"; do
      docker compose ps --status running --services | grep -Fx "$service" >/dev/null || all_running=false
    done
    "$all_running" && return
    sleep 2
  done
  fail "Compose services did not become ready" 6
}

wait_for_http() {
  local url="$1"
  for _ in {1..30}; do
    curl --fail --silent --show-error --max-time 10 "$url" >/dev/null && return
    sleep 2
  done
  fail "HTTP endpoint did not become ready: $url" 7
}
