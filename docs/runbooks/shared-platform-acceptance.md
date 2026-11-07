# Shared-platform acceptance evidence index

Status: evidence index only. This page does not grant production acceptance.

## Scope and status

This index records the evidence needed before a shared platform change is used by
LimnoPulse. It links the existing runbooks; it does not copy their procedures and
does not absorb the LimnoPulse functional roadmap.

| State | Status | Meaning |
| --- | --- | --- |
| Código integrado | Pendente | This index is on the implementation branch until its PR is reviewed and merged. |
| Pronto produção | Não | The candidate has no recoverable artifacts and the owner gate is incomplete. |
| Produção aceita | Não | The LimnoPulse owner has not recorded an explicit acceptance. |

The implementation base is `7c7952e90e7a4ad8bfc278f08ce1a8703ef66361`.
The controller must replace this branch-local record with the merged commit when
publishing the index. A merge or a successful deploy is not an acceptance.

The only allowed scope is this Markdown index. It does not change application code,
IAM, OpenTofu, workflows, data, feature flags, DNS, or a production host.

## Candidate deployment record

The controller reported the following read-only candidate. Values below are public
references, not credentials or host configuration:

| Field | Candidate value | Evidence interpretation |
| --- | --- | --- |
| Workflow run | `41521975392` (`deploy`, manual) | Completed `success` on 2026-11-02. |
| Source | `a019ec5b5db1f8eda203079f1de4e7821500ec81` | Source ref only; it does not prove what reached the host. |
| Production record | `production7502943323` (`success`) | Controller-provided identifier; its evidence is not attached here. |
| Artifacts | `0` | No artifact, digest, log bundle, or readback is recoverable from this run. |

The candidate therefore does not prove the shared-platform contract, a smoke, a
rollback, or acceptance. Do not replace the missing evidence with a workflow badge,
unit tests, or a deploy `success` conclusion.

The governing cross-repository references are [CnesData#478](https://github.com/VINIClUS/CnesData/pull/478),
merged at `e7fa08d7a45193dd44a63ca01201668c8f3ff420`, [CnesData#465](https://github.com/VINIClUS/CnesData/issues/465),
and [LimnoPulse#144](https://github.com/VINIClUS/limnopulse/issues/144). Issue #144
has no owner acceptance record at the time of this audit.

## Canonical runbooks and sources

Use the source runbook for execution and this page only as an evidence index:

| Concern | Canonical source |
| --- | --- |
| M1 compatibility and flag rollback | [`m1-acceptance-runbook.md`](../m1-acceptance-runbook.md) |
| P2 telemetry cutover and rollback | [`p2-telemetry-rollout-runbook.md`](../p2-telemetry-rollout-runbook.md) |
| P3 ingress, worker, queue and rollback | [`p3-ingestion-rollout-runbook.md`](../p3-ingestion-rollout-runbook.md) |
| Worker units, leases and release state | [`worker-runtime-runbook.md`](../worker-runtime-runbook.md) |
| Credential-free local/CI verification | [`verification.md`](../verification.md) |
| VPS bootstrap, Caddy and deploy recovery | [`ops/vps/README.md`](../../ops/vps/README.md) |
| Worker roles and activation contract | [`ops/workers/README.md`](../../ops/workers/README.md) |
| Release architecture and process boundary | [`ADR-020`](../adr/ADR-020-each-worker-role-is-a-supervised-process.md) |
| Production promotion workflow | [`deploy.yml`](../../.github/workflows/deploy.yml) |
| Build and bundle provenance | [`build.yml`](../../.github/workflows/build.yml) |

The LimnoPulse edge documentation names Caddy, not Nginx, as its shared reverse
proxy. A platform that uses Nginx must supply its own exact public/redacted ref and
digest; this repository must not infer Nginx state from the Caddy runbook.

## Evidence ledger

Every row needs an immutable ref or digest, a UTC timestamp, an operator/owner
role, and a recoverable location. Publish only the ref, digest, and redacted
summary. Keep payloads, tokens, hostnames that are not public, account IDs, and
private logs in approved storage.

| Evidence item | Required public/redacted reference | Current source or gap |
| --- | --- | --- |
| Platform/release | Candidate source SHA, release tag, image digest, workers bundle SHA-256 | Source SHA is recorded above; image and bundle digests are missing. |
| Inventory | Versioned platform/host/service inventory ref and digest | No production inventory or readback is attached to run `41521975392`. |
| Dependencies | Exact dependency lock/ref and relevant image/tool digests | Source pins exist in `verify.yml` and the build/deploy workflows; no candidate readback is attached. |
| Preflight | Immutable preflight/plan result ref, including the checked source | No artifact or preflight result is recoverable from the candidate. |
| Dispatcher | `deploy.yml` ref and installed forced-command/deployer ref | Workflow source is linkable; installed host version is unproven. |
| Roles Anywhere / OIDC | External producer contract ref, public/redacted digest, and readback | Not produced by this repository; missing owner evidence. |
| Lease/fencing | Before/after lease and fencing readback ref, without secrets | Runbooks define the contract; no production readback is attached. |
| Edge proxy | Caddyfile/deployed proxy ref and validation/reload evidence | Caddy is documented; live ref, validation, and reload evidence are missing. |
| DNS/CA | Authoritative record and certificate-chain evidence ref | External owner evidence is missing; no DNS operation is authorized here. |
| Recovery | Prior release/bundle refs, recovery result, and redacted logs | Runbook describes recovery; candidate has no recoverable recovery evidence. |

Source-level dependency pins are not deployment evidence. The current source pins
include ORAS `1.3.6` with SHA-256
`66fb0ad7206f3f6fa77ed5b552049d364ccf4b50b1684554f731f76302b52ca0`, InfluxDB
`2.7.11@sha256:d92a10e9e75aff18eca38ff3a8f0b4a800706a5dd44d1b0ece264af04458525b`,
ElasticMQ `1.6.16@sha256:8383f29ad746c12981ca2128f689a4b4a93815235f1a7851d2f165316503d132`,
and DynamoDB Local
`3.3.2@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86`.
They identify checks in source; they do not prove that a production host used them.

## Minimum smoke contract

The owner runs this matrix only in an approved environment and with the already
contracted, least-privileged smoke identity. This task did not run it and did not
use credentials. Each shared change records a baseline immediately before the
change and repeats the same observation immediately after it.

| Change surface | Before change | After change | Pass evidence |
| --- | --- | --- | --- |
| Release/platform | Record source, image, bundle, dispatcher and inventory refs; verify the preflight ref is the expected source. | Repeat refs and read the running version; no unexpected component changed. | Redacted readback with immutable refs/digests. |
| OIDC / Roles Anywhere | Credential-free health and tenant-bound read-only request; record trust/profile/CA contract refs only. | Repeat the request and record the same authorization boundary and certificate/RA readback. | Owner-approved RA/OIDC readback; no token or private ARN. |
| KMS / evidence store | Write/read a non-sensitive canary or approved metadata object and record its digest. | Read the same object, verify digest/signature/retention, and remove only through the approved retention procedure. | Recoverable object/signature ref and digest. |
| Backend/state | Read-only backend and lock-health check; record state/lock contract ref. | Repeat lock/state check and confirm no unexpected state mutation. | Redacted backend and lock readback. |
| Dispatcher / host | Confirm installed dispatcher, active release, worker health, queue age, and lease/fencing health. | Repeat after the change; verify the expected release is active and old work is fenced. | Host readback ref, service/version refs, and lease/fence evidence. |
| Lease / worker | Observe one contracted idempotent read-only health or synthetic canary; capture lease owner/epoch in redacted form. | Repeat and verify no duplicate owner, stale epoch, or unbounded queue. | Before/after lease and queue metrics refs. |
| Caddy / Nginx edge | Check TLS, public health, routing, and the unrelated shared site; validate config before reload. | Repeat all checks and confirm the unrelated site stayed healthy. | Config-validation output, public status, and proxy ref/digest. |
| DNS / CA | Resolve through authoritative servers and check certificate chain/expiry. | Repeat from the same vantage points after the change. | Redacted DNS and certificate evidence. |

A failed before/after pair blocks the change or invokes the exact recovery below.
An HTTP success, a deploy conclusion, or a unit test cannot substitute for a pair.

## Recovery and rollback evidence

Use the existing [P3 rollback](../p3-ingestion-rollout-runbook.md#rollback),
[P2 rollback](../p2-telemetry-rollout-runbook.md#rollback),
[worker deployment recovery](../worker-runtime-runbook.md#look-at-the-workers), and
[VPS deployment recovery](../../ops/vps/README.md#deployment-verification-and-recovery).
Do not invent a second rollback procedure in this index.

Before a shared change, record the prior release tag, image digest, workers bundle
digest, dispatcher/proxy refs, and the applicable lease/queue checkpoint. After a
failed smoke, the owner records the recovery decision, exact prior refs, recovery
readback, and a new before/after pair before retrying. Preserve queued work and
history according to the source runbook; never purge data as a smoke shortcut.

Current recovery status: missing. No recovery was run for candidate
`41521975392`, and no production operation is authorized by this task.

## Explicit owner acceptance

The LimnoPulse owner must record acceptance in
[`LimnoPulse#144`](https://github.com/VINIClUS/limnopulse/issues/144) only after
the evidence ledger and every applicable smoke pair are complete. The acceptance
record must name, using public/redacted values only:

1. the contract and owner role;
2. source, release, dispatcher, platform, inventory, dependency, RA/OIDC, lease,
   proxy, DNS/CA, and preflight refs or digests;
3. the candidate run and recoverable evidence location;
4. the smoke result before and after each shared change;
5. the tested recovery/rollback refs and result; and
6. the UTC timestamp, explicit `accepted`, or an explicit rejection/block reason.

Until that record exists, EXT-6 is blocked and all CnesData production stages that
depend on it remain blocked. This page must never be edited to imply acceptance on
behalf of the owner.

## Audit handoff

The controller should re-audit the branch/head after any external ref changes,
replace candidate placeholders only with independently recoverable evidence, and
keep the following distinction in the final report:

- **Código integrado:** this index is reviewed and merged at a named commit;
- **Pronto produção:** all required producer contracts and evidence are complete;
- **Produção aceita:** the owner has explicitly accepted EXT-6 with smoke and
  recovery evidence.

The current record reaches none of the latter two states.
