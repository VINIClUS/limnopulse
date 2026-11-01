# Optional Evaluation Value Design (issue #6)

**Date:** 2026-10-31\
**Status:** Accepted design\
**Issue:** #6 — Model `Evaluation.Value` as an optional value\
**Depends on:** Phase 3B evaluator and durable state; PR #5 / commit `0a41659`

**Decision:** evaluation values are presence-aware; an indeterminate evaluation never becomes an implicit zero.

This is a design record rather than an ADR because the ADR set in `docs/adr/` is the pinned V4 inventory.

## Context

The Phase 3B evaluator modelled `Evaluation.Value` and `State.LastValue` as Go `float64`. A sufficient aggregate of `0` and "no value" (no data, incomplete coverage, stale data, timeout, query failure, invalid aggregation) were the same bits. Commit `0a41659` (July 2026) and PR #5 applied the narrow fix: `Decide` updates `LastValue` only for sufficient evaluations, and the DynamoDB writer removes `last_evaluation_value` from rule and active-event items after an indeterminate evaluation. The in-memory model and `state_json` still could not say "absent". Issue #6 asks for the full change.

The evaluation value is stored in four places today:

| Location | Writer | Readers |
|---|---|---|
| `state_json` key `LastValue` on the `ALERT_STATE#<rule>` item | Go `Store.Commit`; Python rewrites the document when it resolves an event or resets a pending state (`alert_event_state.py`) | Go `Store.LoadState` only. No code reads `LastValue` for a decision. Python preserves keys it does not update. |
| Rule item `last_evaluation_value` | Go | None |
| Alert Event `last_evaluation_value` | Go | Python `AlertEvent` (`float \| None`, exposed by `/v1` as nullable); the relay (`*float64`) |
| Outbox `evaluation_value` | Go | The relay (`*float64`, must be finite) |

The DynamoDB readers already treat a missing attribute as "no value". Only `state_json` cannot express absence.

## Decision

### 1. Go representation

`internal/alertevaluator` gets a value type:

```go
type Measurement struct { value float64; present bool }

func Measured(value float64) Measurement      // absent outside the persistable range (below); -0 becomes +0
func (m Measurement) Get() (float64, bool)
func (m Measurement) IsPresent() bool
func (m Measurement) MarshalJSON() ([]byte, error)   // number, or null when absent
func (m *Measurement) UnmarshalJSON([]byte) error    // null → absent; number → Measured; anything else → error
```

- The zero value `Measurement{}` is absent. A field nobody sets can never turn into a present zero.
- A present value is always persistable. `Measured` returns absent for NaN, ±Inf, and any non-zero magnitude outside **[1e-100, 1e25]**. Being finite is not enough:
  - a DynamoDB number must lie in about [1e-130, 9.99e125] in magnitude, or `TransactWriteItems` rejects the whole commit;
  - the Python adapters normalize DynamoDB numbers with `value % 1 == 0` in a 28-digit `Decimal` context, which raises for magnitudes of 1e28 and above.

  The bounds keep a margin inside both limits, and no water-quality measurement comes near them. A value outside them is an invalid aggregation, so it is absent and the evaluation records `insufficient_data`. Before this change the effect depended on the magnitude:
  - beyond DynamoDB's range, the commit failed every minute until the sample left the window, which wedged the rule;
  - from 1e28 up to that range, the commit succeeded but the Python readers raised on the stored value;
  - from 1e25 to 1e28, the value was stored and read normally. Reporting `insufficient_data` there is a deliberate change: such a reading is a sensor or aggregation fault, not water quality.

  `Measured` enforces the same range for every caller.
- `-0` is normalized to `+0`. Python reads the JSON `-0` as the integer `0`, so after this change Go never writes a `-0` token. `-0 == +0` under all four operators, so threshold comparisons are unchanged.
- `MarshalJSON` has a value receiver. `json.Marshal` of a non-addressable `State` would skip a pointer-receiver method and write `{}`, which an old evaluator cannot decode into `float64`. Present values encode through `json.Marshal(float64)`, so they are byte-identical to the legacy output.
- `*float64` is rejected. `attributevalue` encodes a nil pointer as a DynamoDB `NULL` attribute rather than omitting it, and pointers alias across `cloneState` copies.
- `Measurement` has no DynamoDB marshaller. A marshaller that returned an error would make the domain package import the AWS SDK. The store therefore writes the `float64` from `Get()` or omits the attribute explicitly (section 4), and the store tests assert the attribute type is `N`.

### 2. One determinacy predicate

An evaluation is **determinate** when `Quality == sufficient` and `Value` is present. Everything else is indeterminate. `Evaluation.IsDeterminate()` is the only predicate.

`Evaluation.Normalized()` enforces it in one place:
- a sufficient evaluation without a value becomes `insufficient_data`;
- any indeterminate evaluation drops its value and its `Breached` flag.

The runner, `Decide` and the store's `Commit` all normalize before they use an evaluation, so the store cannot record `last_evaluation_quality = sufficient` without a value. `Decide` writes `LastQuality` and, for a determinate evaluation, `LastValue` together, so its next state never claims a sufficient quality without a value. `statePut` trusts that invariant rather than refusing the commit: a check there would wedge the rule over a field that no decision reads. A test pins it across every quality, value and starting state. In a normal run, "sufficient but absent" can only come from a caller bug; the normalization is defense in depth. It also keeps the property that the legacy decode relies on: an old evaluator that sees `LastQuality == sufficient` really had a value.

An indeterminate evaluation:
- never opens, never resolves, and never adds confirmation progress;
- keeps `LastValue` at its previous value;
- leaves a pending confirmation chain broken. With no gap, `Decide` leaves the pending state as is, but the next determinate breach is no longer consecutive (`LastBreachSlot + cadence != Slot`), so confirmation restarts at 1. This is existing behavior.
- still allows the existing coalesced-gap reset. When `MissedSlots > 0` and the rule is pending, `Decide` resets pending to healthy before the determinacy gate, whatever the quality. This is unchanged, and an active incident is never touched by it.

The runner computes `Breached` only from a present value, and the store writes value attributes only for a determinate evaluation.

### 3. Fields

| Field | Change |
|---|---|
| `Evaluation.Value` | `float64` → `Measurement`. Present exactly when the evaluation is determinate. |
| `State.LastValue` | `float64` → `Measurement`. Holds the last determinate value and is kept across indeterminate evaluations (PR #5 semantics). Absent until the first determinate evaluation, or when a legacy record is ambiguous (section 5). |
| `WindowResult.Value` | **Stays `float64`, as an interim boundary.** It is a raw diagnostic aggregate, meaningful only when `Quality == sufficient`: `EvaluateSamples` also fills it for stale windows. Changing its type would edit the InfluxDB adapter tests (`influx/query*_test.go`, `client_v2_test.go`, `parity_integration_test.go`), which another change owns right now. Instead `WindowResult.Measurement()` is present only when `Quality == sufficient`, `ValidPointCount > 0` and `Measured(Value)` is present. A sufficient window always counted at least one point, so a bare `{Quality: sufficient}` from a future `WindowReader` reads as absent, not as zero. The runner is the only consumer, and it reads only that projection. **Known gap:** a reader that counts points but forgets to set `Value` would still read as a present zero. Every reader today goes through `EvaluateSamples`, which sets both, so this cannot happen now. Making the window value presence-aware is a follow-up once the InfluxDB changes land. |
| `EvaluateSamples` | An aggregate that `Measured` rejects (NaN, ±Inf, or outside the persistable range) is an invalid aggregation. It returns `insufficient_data`, like an aggregation that produced no value. Stale classification comes after this check, so an overflowing stale window also reports `insufficient_data`. **This is an intentional lifecycle change.** Before, a non-finite aggregate counted as sufficient and reached `Decide`. `NaN` (for example `+Inf` and `-Inf` buckets in one mean) compared as clean, because `compare(NaN)` is false, so `Decide` moved toward recovery. Either way the commit then failed, because `json.Marshal(state)` rejects NaN and ±Inf, and it failed again on every run until the sample left the window. The rule was wedged, not resolved. Nothing records an invalid aggregate apart from `insufficient_data`; a dedicated signal is a follow-up. |
| Runner | On a window error, including a per-rule timeout (the reader returns the `PerRuleTimeout` context error), the evaluation is `query_error` with an absent value, whatever the reader returned. Otherwise `Value = window.Measurement()` and `Breached` is computed from it. The evaluation is then normalized (section 2), so a reader that reports `sufficient` without a usable value yields `insufficient_data`. |

`Sample.Value` (raw telemetry) and rule thresholds stay `float64`.

### 4. Durable encoding

Absent always means **no attribute**: omitted on `Put`, `REMOVE` on `Update`. It never means a DynamoDB `NULL` and never `N "0"`. Present zero is `N "0"`. Negative and positive values are ordinary numbers. The DynamoDB item shapes and the event `schema_version` do not change.

| Attribute | Meaning | Present when |
|---|---|---|
| Rule `last_evaluation_value` | Value of the most recent evaluation, paired with `last_evaluation_quality` | That evaluation was determinate; otherwise `REMOVE` |
| Event `last_evaluation_value` on open (`Put`) | Value that confirmed the episode | Always: opening requires a determinate evaluation. The store returns an error rather than write an event without one. |
| Event `last_evaluation_value` on metadata `Update` | Value of the most recent evaluation | Determinate; otherwise `REMOVE` |
| Event `last_evaluation_value` on resolution `Update` | Value of the clean evaluation | Always: recovery requires a determinate evaluation. The store returns an error otherwise. |
| Outbox `evaluation_value` | Snapshot of the evaluation that created the outbox | Determinate (always true for evaluator outboxes) |
| `state_json` `LastValue` | Last determinate value, retained | See section 5 |

"Retain the last valid value" applies only to `State.LastValue`. The `last_evaluation_*` attributes describe the latest evaluation, so they are absent when it was indeterminate.

### 5. `state_json` schema marker and legacy decode

`state_json` is the only shape whose meaning changes, so it alone gets a version marker. The dynamo adapter encodes an envelope that embeds the domain `State` and adds a top-level PascalCase key:

```json
{"SchemaVersion":2,"Mode":"active", "...":"...", "LastValue":0, "...":"..."}
```

- Version 2: `LastValue` is a number (present) or `null` (absent). A missing key also means absent.
- The marker lives in the adapter, not in the domain `State`. Embedding flattens `State` only while `State` has no JSON methods or tags of its own. The vector tests pin the exact Go bytes and so guard the flat key set that Python rewrites.
- Every decoded number passes through `Measured`. A stored value outside the persistable range, in either version, therefore decodes as absent.
- Legacy: no marker, `null`, `0` or `1`. Go's integer decoding cannot tell a missing marker from `null` or `0`, and no writer emits them. Legacy records are written by a binary from before this design. Since `0a41659`, an old `Decide` assigns `LastValue` only on sufficient evaluations, but its zero value is `0`, so a legacy `0` may be a placeholder. Legacy decode:
  - a non-zero number → present (a real retained value);
  - `0` with `LastQuality == "sufficient"` → present zero;
  - `0` with any other `LastQuality` (including empty or missing), `null`, or a missing key → absent.

  The comparison is numeric, so a legacy `-0` counts as zero. Old `Decide` writes `LastQuality` on every call but `LastValue` only after the sufficient gate, so `LastQuality == sufficient` proves the value came from that evaluation. An old evaluator cannot manufacture a zero in a new → old → new round trip, because it always overwrites `LastQuality` too.

  The rule never invents a zero. It can only lose a real retained zero whose last evaluation was indeterminate. That is acceptable because `LastValue` is not used for any decision (nothing reads it), and the value comes back at the next determinate evaluation.

  Development records written before `0a41659` may hold stale aggregates as non-zero values. Production runs `main-5ac62cf`, which already contains `0a41659`, so its legacy records follow the rule above exactly.
- `LastValue` is not scoped to a rule revision. The administrative reset after a semantic rule change (Python `alert_event_state.py`) clears the lifecycle keys but keeps `LastValue`. This is unchanged and has no effect, since nothing reads it.
- Any other `SchemaVersion` (for example 3, or a negative number) fails `LoadState` with `ErrUnsupportedStateSchema` (fail closed). A tolerant reader would be worse: `statePut` would write the document back through `State` and silently drop the fields a newer writer added.
  - **Operational effect:** the runner treats it like any state read failure. The rule is counted as fatal with the distinct error category `state_schema`, its lease expires and is not released, `next_evaluation_at` does not advance, and the run exits 1.
  - Rolling a future v3 writer back to a v2 binary would make every affected rule blind (no opens, no recoveries) until the rollback is reverted. A future version must therefore ship reader support first, in a separate deploy, before any writer emits it.
- A marker that is not an integer literal that fits Go's `int` is malformed rather than an unsupported version: a string, a fraction such as `2.5`, an integral number written as `2.0` or `2e0`, or an integer that overflows. No writer emits those forms. Python keeps an integer an integer, so a rewrite cannot produce them. So is a malformed `LastValue` (a string, an object). Both fail `LoadState` under the `state_read` category, as a malformed `LastValue` already did for `float64`.

### 6. Migration and backfill

No backfill and no `Scan`. Each enabled rule's state is rewritten in version 2 at its next evaluation, within a minute. Disabled or replaced rules keep legacy records, which the legacy rule decodes. DynamoDB item attributes need no migration: since `0a41659`, Go already removes the value after indeterminate evaluations, and the readers accept a missing attribute. Events resolved by the evaluator always carry a value from a sufficient evaluation.

### 7. Mixed-version deployment and replay

Old binary means current `main`; new binary means this design.

| Writer → reader | Result |
|---|---|
| New Go → old Go | Old Go ignores `SchemaVersion`. It reads `LastValue: null` as `0` without error (`encoding/json` leaves a `float64` unchanged on `null`). It never uses `LastValue` for a decision. Its full `Put` writes the record back without the marker, so the record correctly reverts to legacy shape. |
| Old Go → new Go | Legacy decode (section 5). |
| New Go → Python → either Go | Python's `json.loads`/`state.update`/`json.dumps(sort_keys=True)` preserves the *values* of `SchemaVersion`, `LastValue` and `LastQuality`, so the record stays version 2. The integers, `null` and the marker keep their exact text. Float text may change: `1e-7` becomes `1e-07` and `0.000001` becomes `1e-06`. Key order changes too. Go decodes the rewritten text to the same `float64`, so the vectors compare decoded values, not bytes. |
| Old Go → Python → new Go | Python preserves the legacy record; new Go applies the legacy rule. |
| Any Go → DynamoDB → Python / relay | Unchanged attribute semantics. `N "0"` deserializes to `0`, never `None`; a missing attribute to `None`. The Python reader is pinned against the shared vectors. The relay readers (`*float64`) are unchanged and behave the same, but binding them to the vectors is a follow-up in the notifications package. |

Rollback needs no contract step: old binaries read version 2 records.

Replay with `--evaluation-time` stays idempotent. Event and outbox identities do not depend on the value, and neither does `commitToken`. Two binaries cannot send one token with different parameters, because the token includes:
- `lease_epoch`, which each `Claim` increments;
- the previous `state_revision`, which every commit increments;
- the slot.

So a mixed deployment cannot cause `IdempotentParameterMismatch`. The token is computed from the normalized evaluation.

### 8. Python

The Python adapters treat `state_json` as an opaque document and update only the lifecycle keys they own. No Python production change is needed. The contract is pinned by tests:

- Python preserves the values of `SchemaVersion`, `LastValue` and `LastQuality` through `resolved_evaluator_state` and `reset_pending_evaluator_state`. Its exact output is pinned in the vectors.
- Go decodes the documents Python writes, for both the resolution and the pending reset.
- `AlertEvent.last_evaluation_value` reads `N "0"` as `0.0`, not `None`, and reads a missing attribute as `None`.

The Python `value % 1 == 0` normalization raises for numbers of 1e28 and above. It appears in several adapters. This predates this design and is out of scope here. The persistable range in section 1 keeps every value the evaluator writes far below that limit. Fixing the Python normalization itself is a separate follow-up.

## Consequences

The type system now carries absence. A forgotten assignment yields an absent value, and present values are always persistable. `state_json` gains one key. Old evaluators stay compatible in both directions, and Python needs no deployment ordering. The legacy decode rule may drop a retained zero once per legacy record, with no effect on any decision.

Two lifecycle behaviors change on purpose:
- non-finite and out-of-range aggregates no longer count as sufficient, so they report `insufficient_data` instead of failing every commit;
- a sufficient window that counted no point is indeterminate rather than a present zero.

`WindowResult.Value` remains a raw `float64` until the InfluxDB adapter tests can move (known gap, section 3). A rule stuck on out-of-range data reports `insufficient_data`, like ordinary missing data.

## V4 traceability

V4 §16.1 keeps quality-gated transitions and optimistic revisions. §16.3 requires that no-data, stale and query-error evaluations never silently open or resolve a water-condition incident. §7.3 keeps the one-shot, fenced evaluator.

## Implementation gate

Shared vectors in `testdata/alert_evaluation_value_vectors.json` must be consumed by both Go and Python tests. They cover:
- present zero, positive and negative values;
- absent values, both `null` and a missing key;
- every legacy decode case, including `-0` and an empty or missing `LastQuality`;
- unknown and malformed versions, including `2.0`, `2e0` and an overflowing integer;
- the Python-rewritten documents.

Go tests must also prove:
- the exact `state_json` bytes, and the DynamoDB attribute presence and type for each item kind;
- the old-shape round trip: new JSON decoded into a `float64` struct, re-marshalled, then read by the new decoder, applies the legacy rule;
- the persistable range: `1e26`, `1e200`, `5e-324` and `1e-101` are absent, and the boundaries `1e25` and `1e-100` are present;
- that "sufficient but absent", a sufficient window without points, `NaN` and `±Inf` inputs never open, confirm or resolve an incident, while a present zero does;
- that normalization records `insufficient_data` for "sufficient but absent";
- that the coalesced-gap reset still applies to an indeterminate evaluation of a pending rule;
- that a per-rule timeout yields an absent `query_error`;
- that an unknown schema version is reported under the `state_schema` category.

Each acceptance mutant must fail at least one test:

- absent decoded as `0`;
- zero treated as absent;
- the legacy decode dropped;
- an absent value resolving an incident.

## Non-goals

This record does not change `Sample.Value`, rule thresholds, alert-event identities, `commitToken`, the event `schema_version`, or the `/v1` contract. It does not add recovery hysteresis, and it does not migrate `WindowResult.Value`.
