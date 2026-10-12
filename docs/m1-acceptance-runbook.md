# M1 Acceptance and Rollout Runbook

This runbook covers Epic #16 (M1, API v2 domain foundation and compatibility). It gives the
operator the exact order for running the default Site/Asset/Device migration and turning on the
two M1 feature flags. It also says how to roll back. M1-08 adds no runtime capability. The
release gate is `tests/contracts/test_m1_acceptance_gate.py`. If the gate fails, the release is
blocked and the owning task (M1-05, M1-06 or M1-07) fixes the problem. Never weaken the gate,
and never regenerate `tests/contracts/openapi/v1.json` to hide drift.

The two flags are independent and both default to `false`:

| Flag | Effect when `true` |
|---|---|
| `API_V2_ENABLED` | Mounts the M1 `/v2` groups: 13 paths and 25 operations (Sites, Assets, Devices, Components, Capabilities, Integration accounts, Device integrations, Deployments) for every tenant of the deployment; there is no tenant allowlist. |
| `V1_POND_DEVICE_PROJECTION_ENABLED` | `/v1` Pond/Device routes read and write through the v2 projection. When any of the four M1 repositories is missing from app state, they return `503` and never fall back to the legacy repository. |

## Release gate

Every command must exit `0`. None of them uses production credentials or changes cloud state.
Run the gate without a repo-root `.env` (move it aside, or export settings in the shell),
because `Settings` reads it and two `tests/unit/test_settings.py` tests then fail.

```bash
uv run --locked --extra dev pytest -q \
  tests/contracts/test_v1_openapi_contract.py \
  tests/contracts/test_m1_acceptance_gate.py \
  tests/unit/test_tenant_mapping_conformance.py \
  tests/unit/test_no_scan_guard.py
uv run --locked --extra dev python scripts/dev/export_v1_openapi.py --check
make verify
git diff --check
```

The acceptance gate proves these invariants:

1. A Gateway Device exposes two distinct Probe Components. This runs in memory and on the real
   Dynamo adapters.
2. When a Probe moves from West to East, the old event-time assignment is kept. The intervals
   are half-open `[started_at, ended_at)`. A second current interval or a backdated overlap
   returns `409`. This is proved through the real `DeploymentService` and the real Dynamo
   adapters, and also through a flag-on `/v1` Pond move over migrated rows.
3. `DeviceV2` has no `pond_id`. The `/v1` projection still exposes it.
4. `QuotaDimension` covers sites, assets, devices, components and notification destinations.
5. The domain modules, `repositories/v2_contracts.py`, the M1 adapters, services and migration,
   and `/v2` never mention `redis`, `aws_iot`, `thing_name`, `certificate_arn` or `shadow_name`.
6. A dry-run writes nothing. A second `--apply` creates nothing; the only row it changes is the
   migration receipt.
7. With both flags on, removing any one of the four M1 repositories makes `/v1` ponds and
   devices return `503 {"detail": "service unavailable"}`.
8. With `API_V2_ENABLED=false` there is no `/v2` path. With `true` there are exactly the M1
   groups.
9. `/v1` matches the P0 golden whatever the flags. Every tenant route requires membership: 28
   routes by default, 53 with `/v2`. The no-Scan guard walks `src/limnopulse_api/migrations` and
   `scripts/migrations`.

## Rollout sequence

Use dynamodb-local or real DynamoDB as the reference store. Floci can double-commit concurrent
conditional transactions and ignores the 1 MB Query page, so its results do not count.

1. **Confirm a backup and PITR.** Read the domain table name from the environment's
   `DYNAMODB_DOMAIN_TABLE` (setting `dynamodb_domain_table`; `LimnopulseDomain` is only the
   default, e.g. locally). Confirm point-in-time recovery is enabled on **that** table, or take
   an on-demand backup of it. Record the environment, both table names (`DYNAMODB_DOMAIN_TABLE`,
   `DYNAMODB_AUDIT_TABLE`) and the pilot tenant IDs. The migration CLI uses the same setting, so
   run it with the environment's value exported.
2. **Dry-run an explicit tenant.** Run this from the repository root. The migration never
   discovers tenants and has no `--all`.

   ```bash
   python -m scripts.migrations.m1_backfill_default_site_assets \
     --tenant <pilot-tenant> --page-size 25 --deadline-seconds 300
   ```

   Expect `"apply": false`, `"result": "success"`, `"schema_conflicts": 0` and `"conflicts": []`.
   Check that the `would_*` counts match the tenant's Ponds and Devices. A dry-run writes
   nothing.
3. **Apply.** Run the same command with `--apply`. Expect `"result": "success"` and `*_created`
   counts equal to the dry-run's `would_*` counts.
4. **Run a second apply to prove idempotency.** Run `--apply` again. Every `*_created` count
   must be `0`, `legacy_links_updated` must be `0` and `already_migrated` must be nonzero.
5. **Deploy with both flags false.** Set `API_V2_ENABLED=false` and
   `V1_POND_DEVICE_PROJECTION_ENABLED=false`. `/v1` behaves exactly as before.

   Both flags are read once, when `create_app()` builds the app: `Settings` is captured there
   and the `/v2` routers are mounted (or not) at startup. **Every** flag change in this runbook
   (enable, rollback, re-enable) therefore takes effect only after a restart or redeploy of
   **every** API instance; changing the environment alone does nothing. After each restart,
   [verify the flags took effect](#verify-the-flags-took-effect).
6. **Pilot `API_V2_ENABLED=true`.** `API_V2_ENABLED` mounts `/v2` for the whole application;
   there is no tenant allowlist. Enabling it is **environment-wide**: every tenant served by
   that deployment gets the M1 endpoints (still gated by tenant membership). A tenant-limited
   pilot therefore needs an isolated environment or deployment serving only the pilot tenants;
   otherwise accept and record that the pilot is environment-wide. Smoke each M1 group: list and
   create Sites, Assets, Devices, Components, Capabilities, Integration accounts, Device
   integrations, and start and end Deployments. Every `/v2` create needs an `Idempotency-Key`
   of 8–128 characters. Restart every API instance and verify the flags took effect.
7. **Check the `/v1` golden and tenant conformance.** Run
   `tests/contracts/test_v1_openapi_contract.py`, `tests/unit/test_tenant_mapping_conformance.py`
   and `tests/unit/test_no_scan_guard.py` against the deployed revision, or just run the release
   gate above.
8. **Enable the projection with a closed cutover window.**
   1. Re-run the dry-run and `--apply` immediately before the cutover. The flag was off, so
      `/v1` moves made since step 3 may have left drift; this pass repairs it (see
      [Drift repair](#drift-repair)).
   2. Set `V1_POND_DEVICE_PROJECTION_ENABLED=true` and cut over **atomically** where the
      platform allows it: blue/green, or scale the flag-off instances to zero before any
      flag-on instance serves traffic. If only a rolling restart is possible, **quiesce `/v1`
      Pond/Device writes** (POST/PATCH ponds and devices) from the final dry-run until every
      instance runs flag-on. During a rolling restart, a flag-off instance still accepts
      legacy-only writes: a Device moved there leaves `DEPLOYMENT#CURRENT` on the old Asset, and
      a Device created there has no Component or Deployment until it is migrated.
   3. Verify the flags took effect on **every** instance.
   4. **Mandatory, in either case:** once every instance is confirmed flag-on, run the dry-run
      and `--apply` **again**. It repairs window moves through drift repair and backfills
      window-created Devices. Running it while flag-on `/v1` writes continue is safe; the M1-05
      writes and the M1-06 migration converge. The gate proves this sequence
      (`test_apply_after_a_mixed_flag_window_converges`): after this apply, CURRENT is on the new
      Asset, the late Device has its Component and Deployment, and the next apply is a no-op.
9. **Run the full gate.** Run the release gate. Then watch the `/v1` and `/v2` `409` and `503`
   rates. Diagnose a sustained `503` on `/v1` Pond/Device routes with
   [Diagnosing 503 on /v1 Ponds and Devices](#diagnosing-503-on-v1-ponds-and-devices) before
   touching any flag.

## Rollback

Roll back by changing flags only: set `V1_POND_DEVICE_PROJECTION_ENABLED=false` and
`API_V2_ENABLED=false`, then restart or redeploy **every** API instance (the flags are read
only at startup) and [verify the flags took effect](#verify-the-flags-took-effect). A rolling
rollback mixes flag-on writes (which keep the v2 rows in sync) with flag-off legacy-only writes
(which leave drift). That is safe: the legacy rows remain the `/v1` source of truth (D1), and the
re-enable sequence below repairs the drift; the gate covers this interleaving too. With the
projection off, `/v1` reads come from the legacy `POND#` and
`DEVICE#` rows (under D1 they do with either flag value). Native v2 Devices never appear in `/v1`.

- **Never delete rows.** Additive v2 rows, the `v2_*` link attributes on legacy rows,
  Deployment history, Deployment markers and migration receipts all stay in place.
- **Re-enabling after a rollback** is the full step 8: dry-run and `--apply` so the v2 rows
  catch up with `/v1` edits made while the flag was off, an atomic cutover or quiesced `/v1`
  Pond/Device writes, a restart of every instance with verification, and then the mandatory
  post-cutover dry-run and `--apply`.

### Verify the flags took effect

Run these against every restarted instance (or through the load balancer once all are
replaced):

- `API_V2_ENABLED=false`: `GET /v2/tenants/<pilot>/sites` with a valid member token returns
  `404`, and the app's OpenAPI document has no `/v2` path.
- `API_V2_ENABLED=true`: the same request returns `200`, and OpenAPI lists exactly the 13 M1
  `/v2` paths.
- Either value: the `/v1` contract still matches the golden
  (`tests/contracts/test_v1_openapi_contract.py`, or compare `build_v1_openapi_contract` of the
  deployed revision with `tests/contracts/openapi/v1.json`).
- `V1_POND_DEVICE_PROJECTION_ENABLED`: `/v1` reads look the same with either value (D1), so
  confirm the value in the running process's environment for every instance, and check that
  `GET /v1/tenants/<pilot>/ponds` and `/devices` return `200` (for a `503`, see
  [Diagnosing 503 on /v1 Ponds and Devices](#diagnosing-503-on-v1-ponds-and-devices)). Do not probe with writes: with the flag off a patch to a
  nonexistent `pond_id` succeeds and creates exactly the `device_missing_pond` state.

### Diagnosing 503 on /v1 Ponds and Devices

A `503 {"detail": "service unavailable"}` on `/v1` Ponds or Devices has more than one cause, and
the body is identical for all of them:

- **Infrastructure:** `main.py` maps every DynamoDB `ClientError` and `BotoCoreError` to that
  same `503` on every route, and membership resolution (which also reads DynamoDB and Redis)
  runs before the Pond/Device handler.
- **Incomplete M1 app state (flag on only):** `get_v1_pond_device_projection` in
  `api/dependencies.py` raises `HTTPException(status_code=503, detail="service unavailable")`
  when any of `site_asset_repository`, `device_component_repository`,
  `integration_repository` or `deployment_repository` is missing from `app.state`, or when the
  Site/Asset repository has no `table_name`/`client`. It **logs nothing** distinctive, so
  identify it by elimination.

Steps:

1. For the same tenant and token, call `/v1` routes that do not use the projection, e.g.
   `GET /v1/tenants/<t>/alert-rules` and a telemetry read. If they also return `503`, it is an
   infrastructure or membership failure.
2. Check the API logs around the failing requests for a logged DynamoDB/botocore, Redis or
   membership exception (the infrastructure handler itself does not log; look for the
   framework/server error logs and the dependency's own logs and metrics).
3. **Infrastructure:** fix the dependency (DynamoDB, network, credentials, Redis). Do **not**
   change any flag.
4. **Only Ponds/Devices fail, other `/v1` routes are healthy, and no infrastructure error is
   logged:** this is the projection's missing-repository `503`. Roll back the flags (see
   [Rollback](#rollback)), then fix the deployment so all four M1 repositories are created.

## Migration exit codes

For every runtime outcome the CLI prints one JSON summary on stdout. Its `result` is one of
`success`, `schema_conflict`, `deadline_exceeded`, `dynamodb_error`, `configuration_error` or
`failed`. Usage errors (a `--tenant-file` outside the working directory, neither `--tenant` nor
`--tenant-file`, bad or out-of-range arguments) exit `1` with a message on stderr and print **no**
JSON summary.

| Exit | Meaning | Action |
|---|---|---|
| `0` | Every scoped tenant completed (`success`). | Continue. |
| `2` | Schema or ownership conflict (`schema_conflict`). `conflicts` lists `{tenant_id, kind, key}`. The conflicting tenant gets no receipt; other tenants continue. | Fix each conflict using the remedies below, then run the dry-run and `--apply` again. |
| `1` | Configuration, DynamoDB, deadline or `failed`. Exit `1` takes precedence over `2`. | Fix the configuration or the connectivity, or raise `--deadline-seconds`, then re-run. The migration is idempotent and converges. |

`failed` means that tenant had transient churn: `TransactionConflict` retries were exhausted,
or rows were still changing after 3 verification passes. That tenant gets no receipt. Re-run it,
ideally while `/v1` writes for it are quiet. `failed` also covers an unclassified error that
aborts the whole run; check stderr and the infrastructure, then re-run.

`--tenant-file` must be a regular file of at most 1 MiB inside the working directory, with one
tenant ID per line. Symlinks are resolved before the check, so run from the repository root. A
path outside the repository, such as the plan's `/secure/path/...` example, is rejected as a
usage error (exit `1`, no JSON summary). Copy the file under the repository root first, and do not
commit it.

### Conflict remedies (exit 2)

| `kind` | Cause | Remedy |
|---|---|---|
| `device_missing_pond` | A legacy Device's `pond_id` names no Pond (D5). The whole tenant is blocked. | Patch the Device through `/v1` to an **existing** `pond_id`, then re-run the dry-run and `--apply`. This works with either flag value: with the flag off the next `--apply` repairs the Deployment ([Drift repair](#drift-repair)); with the flag on the `/v1` write reconciles it. With the flag on, a `/v1` create or patch to a **nonexistent** `pond_id` returns `409 "pond not found"`. Delete the Device only under the strict conditions in [Removing a legacy Device](#removing-a-legacy-device). |
| `tenant_missing` | `TENANT#<id>/META` does not exist. | Check the tenant ID (it is case- and whitespace-sensitive) and remove it from the scope. |
| `tenant_invalid` | Tenant META does not parse as a legacy Tenant, or its ID is not v2-safe. | Repair the Tenant row from backup, or exclude the tenant until it is fixed. |
| `site` | A row at the deterministic default Site key has different ownership, or the default Site is not ACTIVE when a new Asset or initial Deployment needs it. | Inspect `TENANT#<id>/SITE#<default_site_id>`. The default Site cannot be archived (D6); restore its ownership and `active` status from backup. |
| `asset` | A row at `ASSET#<asset_id_for_legacy_pond>` belongs to a different Site or Pond, or cannot be rebuilt. | Inspect the Asset and its PondProfile. Restore the deterministic Asset from backup; never hand-edit IDs. |
| `pond_profile` | The PondProfile's `legacy_pond_id` or owner disagrees with the Pond or Asset. | Restore `ASSET#<id>/PROFILE#POND` from backup so it points at the legacy Pond. |
| `component` | The canonical row and the tenant mirror of the default Component disagree, or belong to another Device. | Restore both Component rows from backup; they must match field for field. |
| `deployment`, `deployment_current`, `deployment_history`, `deployment_marker` | A Deployment group (mirror, history row, `DEPLOYMENT#CURRENT`, marker) is inconsistent, malformed, or shows a stray history row or marker where no deterministic Deployment exists. | Do not delete history. Compare the group with backup or PITR, restore the missing or divergent row, and re-run. Escalate to the M1-04/M1-06 owner if the history itself is wrong. |
| `device_lookup` | `DEVICE#<id>/META` disagrees with the tenant `DEVICE#` row the same way on every re-read. | Restore the lookup from the tenant Device row; every field except `PK`, `SK` and `entity_type` must match. |
| `device_invalid` | A legacy Device row does not parse, or has an identifier that is not v2-compatible. | Fix the row from backup, then re-run. If the Device is junk, see [Removing a legacy Device](#removing-a-legacy-device). Native v2 rows (`schema_version == 2`) are skipped, never reported. |
| `pond_invalid` | A legacy Pond row does not parse, or its ID is not v2-safe. | Fix the Pond row from backup. With the flag on, `/v1` returns `409` for such ids. |
| `pond_missing`, `device_missing` | A source row vanished while the run was planning it, and stayed gone. | Re-run; if it persists, check for concurrent deletes. |
| `migration_receipt` | `MIGRATION#M1#DEFAULT_SITE_ASSET_DEVICE` exists but is not this tenant's M1 receipt (wrong owner or malformed `run_count`). | Inspect the row. Restore it from backup, or remove it only after confirming with the M1-06 owner. The receipt is an audit trail and is never used to skip validation. |

### Removing a legacy Device

`/v1` has no DELETE route for Devices, so prefer the patch remedy above. Deleting legacy rows
is allowed **only for a Device that was never projected**, which means all of:

- its `TENANT#<tenant>/DEVICE#<device>` row and `DEVICE#<device>/META` lookup carry neither
  `default_component_id` nor `v2_projection_version` (the link attributes the migration and the
  flag-on `/v1` writes add), and
- no `COMPONENT#<cmp>/META` row and no `TENANT#<tenant>/COMPONENT#<cmp>` mirror exist for
  `cmp = default_component_id_for_legacy_device(tenant, device)`, and no
  `COMPONENT#<cmp>` partition rows (`DEPLOYMENT#…`) exist.

The migration discovers Devices only through the legacy `DEVICE#` rows, so deleting a projected
Device would orphan its default Component, `DEPLOYMENT#CURRENT`, marker and active history. For
a projected Device, do **not** delete anything: fix its `pond_id` as above, or escalate to the
M1-05/M1-06 owner.

For a never-projected Device: take a backup first, delete **both** the
`TENANT#<tenant>/DEVICE#<device>` row and the `DEVICE#<device>/META` lookup, then re-run the
dry-run and `--apply`. This is a deliberate exception to "never delete rows", which governs
rollback.

## Drift repair

- **Flag off:** a Device moved through `/v1` (its legacy `pond_id` changed) has its
  default-Component Deployment repaired on the next `--apply`. The current interval ends and a
  new one starts on the new Pond's Asset at
  `transition_at = max(device.updated_at, current.started_at + 1µs)`. The real move time is
  unknown, so the legacy row approximates it, and earlier event-time assignments are kept. A
  repair counts under `deployments_created` and `devices_projected`.
- **Flag on:** `/v1` writes reconcile drift themselves, using a clock-based transition. Reads
  never write (D4).
- There is no repair into an ARCHIVED Asset or Site, or for a DECOMMISSIONED Component; those
  count as already migrated.
- Archived legacy Ponds: their Devices keep an ACTIVE Deployment on an ARCHIVED Asset, which
  mirrors the legacy state.
- **Archived targets depend on the flag.** With `V1_POND_DEVICE_PROJECTION_ENABLED=true`, `/v1`
  creates and moves into an archived Pond return `409`, and `/v2` Deployment starts into an
  archived Asset or Site return `409`. With the projection flag **off** (rollout steps 1–8 and
  after a rollback), the legacy repository accepts both. The next `--apply` then does this:
  - **Device moved into an archived Pond while the flag was off:** no repair. The run reports
    the Device as already migrated (`deployments_created` 0), its `DEPLOYMENT#CURRENT` stays
    ACTIVE on the previous Pond's Asset, while `/v1` (legacy rows, D1) shows the archived
    `pond_id`. Flag-on `/v1` writes skip this drift too. Move the Device to an active Pond to
    converge.
  - **Device created in an archived Pond while the flag was off:** it is projected like any
    legacy Device: a new default Component and an ACTIVE Deployment on the ARCHIVED Asset,
    mirroring the legacy state (counted in `components_created`/`deployments_created`).

## Behavior changes with the projection flag on (PR #61)

- **D5:** a `/v1` Device create or patch with a nonexistent `pond_id` returns
  `409 "pond not found"`.
- Version conflicts read `"pond version conflict"` or `"device version conflict"`. The v1 and v2
  version counters are independent (D2).
- `409` for archived or inactive targets (flag on only; see [Drift repair](#drift-repair) for
  the flag-off outcome).
- Legacy identifiers that are not v2-compatible (outside `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`, or
  a `dev_` id that is not `dev_<32 lower-hex>`): flag-on `/v1` **create and update** writes
  return `409`. **Reads do not check this:** `list_ponds`, `get_pond`, `list_devices` and
  `get_device` return the legacy row unchanged (`200`) with either flag value, and a row that does
  not parse as a legacy Pond or Device at all fails the request with `500`. A successful read
  therefore does not mean a row is v2-compatible. The migration dry-run is the detector: it
  reports such rows as `pond_invalid` or `device_invalid` (exit `2`).
- `/v2` cannot start or end Deployments of the legacy default Components; it returns `409`
  "legacy default component deployments are managed through /v1" (D3).

## Local rehearsal

Dynamodb-local is the reference store. The migration needs only `dynamodb-local`; the other
services are part of the standard local stack. From the repository root, export the local
settings in the shell instead of creating a repo-root `.env`:

```bash
export APP_ENV=local DYNAMODB_ENDPOINT_URL=http://localhost:8001
```

A repo-root `.env` is read by `Settings` and breaks two `tests/unit/test_settings.py` tests during
`make verify`. If you do create one for the rehearsal, move it aside before running the release
gate.

```bash
docker compose up -d redis dynamodb-local influxdb mqtt-broker telegraf elasticmq
python scripts/dev/init_dynamodb.py
python scripts/dev/seed_local.py
python -m scripts.migrations.m1_backfill_default_site_assets \
  --tenant tnt_local_001 --page-size 25 --deadline-seconds 300
python -m scripts.migrations.m1_backfill_default_site_assets \
  --tenant tnt_local_001 --apply --page-size 25 --deadline-seconds 300
python -m scripts.migrations.m1_backfill_default_site_assets \
  --tenant tnt_local_001 --apply --page-size 25 --deadline-seconds 300
```

Expected results:

- The dry-run reports `apply=false` and makes no writes.
- The first apply reports `result="success"` and creates the deterministic projection.
- The second apply reports zero new Site/Asset/Component/Deployment rows and a nonzero
  `already_migrated`.

Recorded on dynamodb-local, all three exiting `0`:

```json
{"apply": false, "tenants_requested": 1, "tenants_completed": 1, "default_sites_created": 0, "assets_created": 0, "devices_projected": 0, "components_created": 0, "deployments_created": 0, "legacy_links_updated": 0, "would_create_default_sites": 1, "would_create_assets": 1, "would_project_devices": 1, "would_create_components": 1, "would_create_deployments": 1, "would_update_legacy_links": 2, "already_migrated": 0, "schema_conflicts": 0, "conflicts": [], "scope_completed": true, "result": "success"}
{"apply": true, "tenants_requested": 1, "tenants_completed": 1, "default_sites_created": 1, "assets_created": 1, "devices_projected": 1, "components_created": 1, "deployments_created": 1, "legacy_links_updated": 2, "would_create_default_sites": 0, "would_create_assets": 0, "would_project_devices": 0, "would_create_components": 0, "would_create_deployments": 0, "would_update_legacy_links": 0, "already_migrated": 0, "schema_conflicts": 0, "conflicts": [], "scope_completed": true, "result": "success"}
{"apply": true, "tenants_requested": 1, "tenants_completed": 1, "default_sites_created": 0, "assets_created": 0, "devices_projected": 0, "components_created": 0, "deployments_created": 0, "legacy_links_updated": 0, "would_create_default_sites": 0, "would_create_assets": 0, "would_project_devices": 0, "would_create_components": 0, "would_create_deployments": 0, "would_update_legacy_links": 0, "already_migrated": 2, "schema_conflicts": 0, "conflicts": [], "scope_completed": true, "result": "success"}
```

Do not move `local-device-001` during rehearsals. The Telegraf device registry is static, so
its tags would no longer match.

## Known out-of-scope items

- Telegraf enrichment uses the static Starlark registry (`infra/telegraf/device_registry.star`),
  so moving a Device does not change its Influx tags. That is P2/P3 (#22, #15).
- A `/v1` POST with a `NaN` body returns `500`. This predates M1 and is tracked for P5.
- AWS IoT, Stripe, canonical telemetry v2 writes, advanced health, Push/SMS, notification
  policy and commands are not part of M1.
