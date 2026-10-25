# Local tooling and deployment stabilization

Approved scope: synchronize main, configure the existing Wokwi/Arduino/MQTTX/
Node-RED toolchain, and fix deployment input handling and rollback. Preserve
API contracts, firmware, rollout defaults, existing local files and other
worktrees. No production deployment, paid subscription or roadmap expansion.

## Deliverables

1. Preserve the existing VPS README change in a patch and SHA-addressed stash;
   fast-forward main and work on a `codex/` branch. The old backend-lock test
   is already present in main and must not be replayed.
2. Synchronize locked Python/frontend dependencies and install the versions
   pinned in `e2e-m1.yml`, checking the binary hashes. Use Node 22 without
   changing the user's global default. Keep Wokwi credentials outside Git.
3. Write regression tests before implementation and obtain an independent
   test-quality review. Exercise the real Bash script with a fake Docker
   boundary: success, invalid tags, pull/up failure, health timeout, rollback
   failure, and first-deployment cleanup without deleting volumes.
4. Pass workflow input through the environment; explicitly roll back on
   health timeout, disable recursive error traps during rollback, and retain
   the production path/log/120-second defaults while allowing isolated tests.
   Add a parse-only production Compose Make target and reuse it in CI.
5. Validate the existing ESP32 sketch, serial scenario and replay. Run M1 and
   InfluxDB 2.7.11 integration checks in a dedicated Compose project with its
   own volumes. Run the default suite and frontend checks. Configure the
   Wokwi CI secret and verify an actual simulation, not merely a skipped job.
6. Obtain a fresh independent audit of the full base-to-head diff, fix
   material findings, open/attach/merge the PR after checks, and return to
   updated main. Report any external blocker precisely.

## Verification

- Deployment tests must fail for the original bugs before the fixes and pass
  afterward; malformed workflow inputs must never execute shell payloads.
- Failed promotion restores the previous image tag and exits nonzero. With no
  previous release, stop the candidate before removing its image-tag file;
  retain volumes. Rollback failure must remain nonzero and be visible in logs.
- Run `make verify`, `make verify-compose-production`, frontend test/typecheck/
  build, `make verify-influx`, M1 E2E, Arduino compile, Wokwi lint/simulation.
- The auditor checks secret handling, rollback, local data preservation,
  unchanged API/flags, and tests that would catch realistic regressions.
