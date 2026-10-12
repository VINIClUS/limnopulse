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
| `API_V2_ENABLED` | Mounts the M1 `/v2` groups: 13 paths and 25 operations (Sites, Assets, Devices, Components, Capabilities, Integration accounts, Device integrations, Deployments). |
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

1. **Confirm a backup and PITR.** Confirm point-in-time recovery is enabled on
   `LimnopulseDomain`, or take an on-demand backup. Record the environment, the table names
   (`DYNAMODB_DOMAIN_TABLE`, `DYNAMODB_AUDIT_TABLE`) and the pilot tenant IDs.
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
6. **Pilot `API_V2_ENABLED=true`.** Enable it for the pilot. Smoke each M1 group: list and
   create Sites, Assets, Devices, Components, Capabilities, Integration accounts, Device
   integrations, and start and end Deployments. Every `/v2` create needs an `Idempotency-Key`
   of 8–128 characters.
7. **Check the `/v1` golden and tenant conformance.** Run
   `tests/contracts/test_v1_openapi_contract.py`, `tests/unit/test_tenant_mapping_conformance.py`
   and `tests/unit/test_no_scan_guard.py` against the deployed revision, or just run the release
   gate above.
8. **Re-run the dry-run and `--apply` immediately before enabling the projection.** The flag
   was off, so `/v1` moves made since step 3 may have left drift; this pass repairs it (see
   [Drift repair](#drift-repair)). Then set `V1_POND_DEVICE_PROJECTION_ENABLED=true` for the
   rollout environment.
9. **Run the full gate.** Run the release gate. Then watch the `/v1` and `/v2` `409` and `503`
   rates. A sustained `503` on `/v1` Pond/Device routes means M1 app state is incomplete; roll
   back.

## Rollback

Roll back by changing flags only: set `V1_POND_DEVICE_PROJECTION_ENABLED=false` and
`API_V2_ENABLED=false`. With the projection off, `/v1` reads come from the legacy `POND#` and
`DEVICE#` rows (under D1 they do with either flag value). Native v2 Devices never appear in `/v1`.

- **Never delete rows.** Additive v2 rows, the `v2_*` link attributes on legacy rows,
  Deployment history, Deployment markers and migration receipts all stay in place.
- **Before re-enabling after a rollback,** run the dry-run and then `--apply` again so the v2
  rows catch up with `/v1` edits made while the flag was off. After that, follow the rollout
  from step 8.

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
| `device_missing_pond` | A legacy Device's `pond_id` names no Pond (D5). The whole tenant is blocked. | Preferred: patch the orphan Device through `/v1` to an **existing** `pond_id`; this works with either flag value. Then re-run. With the flag on, a `/v1` create or patch to a **nonexistent** `pond_id` returns `409 "pond not found"`. If the Device must go instead, see [Removing a legacy Device](#removing-a-legacy-device). |
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

`/v1` has no DELETE route for Devices, so prefer the patch remedy above. When removal is truly
needed, it is a manual operation: take a backup first, then delete **both** the
`TENANT#<tenant>/DEVICE#<device>` row and the `DEVICE#<device>/META` lookup, and re-run the
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
  mirrors the legacy state. New `/v2` starts, `/v1` moves, or creates into an archived Asset or
  Site return `409`.

## Behavior changes with the projection flag on (PR #61)

- **D5:** a `/v1` Device create or patch with a nonexistent `pond_id` returns
  `409 "pond not found"`.
- Version conflicts read `"pond version conflict"` or `"device version conflict"`. The v1 and v2
  version counters are independent (D2).
- `409` for archived or inactive targets, and for legacy identifiers that are not
  v2-compatible.
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
