# Verification

LimnoPulse exposes the same credential-free verification boundaries to local
contributors and GitHub Actions. The gates reproduce repository dependency and
configuration inputs; they do not promise byte-identical local toolchains.

## Prerequisites

Install GNU Make, Python 3.12 or newer, uv, the Go version declared by
`go.mod`, OpenTofu 1.8.0 or newer, and Docker with the Compose plugin. The uv
and OpenTofu binaries can be overridden with the `UV` and `TOFU` Make
variables when a controlled local binary is required.

## Local gates

- `make verify-python` checks that `uv.lock` matches `pyproject.toml`, syncs
  the `dev` extra from the lock, and runs the normal repository pytest suite.
- `make verify-go` runs all repository Go tests with the race detector using
  `go.mod` and `go.sum`.
- `make verify-tofu` initializes `infra/opentofu` with the configured backend
  disabled, checks recursive formatting, and validates the configuration.
- `make verify-compose` parses and validates `compose.yaml` quietly. It does
  not start, build, pull, or create services.
- `make verify` runs all four boundaries.

The Python suite includes `tests/integration/test_notifications_local.py`, but
that multiprocess integration remains opt-in. It is skipped unless
`RUN_NOTIFICATION_INTEGRATION=1` is set after its required local services have
been started explicitly.

## Safety and readiness boundary

These gates use no AWS credentials. OpenTofu initialization has its backend
disabled, and verification never runs `tofu plan` or `tofu apply`. Compose is
parsed only and does not start services. Public package and provider registries
may still be contacted to install locked dependencies or provider plugins, so
credential-free does not mean offline.

Passing these checks does not establish AWS sandbox or production readiness.
Cloud deployment, remote-state configuration, environment smoke tests, and
operational readiness remain a later gate.
