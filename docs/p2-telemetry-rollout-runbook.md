# P2 Canonical Telemetry Rollout Runbook

Epic #22 (P2) adds canonical metric observations in the InfluxDB `observation_numeric` measurement,
controlled dual writes to the legacy `water_quality` measurement, the `/v2` telemetry reads, the Go
alert evaluator v2 window reader and the `/v1` read projection. Plan:
[`2026-10-18-limnopulse-v4-p2-canonical-telemetry.md`](superpowers/plans/2026-10-18-limnopulse-v4-p2-canonical-telemetry.md)
(revision r7.2).

P2 ships **dark** (D16). Every switch defaults off, and with all of them off the services behave
exactly as before P2. No deployed service calls the telemetry pipeline in P2: the only producer is the
dev CLI `scripts/dev/ingest_reading_v2.py`, and Telegraf keeps writing only `water_quality`. Turning
on any switch in a shared environment is therefore a **rehearsal** until P3 delivers a producer.

## Flags

| Setting / env | Default | Owner | Effect |
|---|---|---|---|
| `TELEMETRY_V2_WRITE_ENABLED` | `false` | API, dev CLI | Write `observation_numeric` points. |
| `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED` | `false` | API, dev CLI | Also write eligible `water_quality` points from the normalizer path (D7: legacy default Components on a legacy pond Asset only). |
| `API_V2_TELEMETRY_ENABLED` | `false` | API | Mount exactly `GET /v2/tenants/{tenant_id}/metric-definitions`, `.../observations` and `.../observations/latest` (tenant routes 53 → 56). Requires `API_V2_ENABLED=true`: the settings validator refuses to start otherwise. |
| `V1_TELEMETRY_READ_SOURCE` | `legacy` | API | `legacy` reads `/v1` telemetry from `water_quality`; `v2` swaps in the projection over `observation_numeric` (D16 precondition). |
| `ALERT_EVALUATOR_TELEMETRY_SOURCE` | `legacy` | Go evaluator | Window reader, strictly `legacy` or `v2` (also `--telemetry-source`; D16 precondition). |

- The two write switches are independent. The write path writes v2 first, then legacy. Any failure
  raises `TelemetryWriteError`, nothing is reported as written, and the caller must not acknowledge
  (D9). With both write switches off, the dev CLI is a dry run.
- `API_V2_ENABLED` alone still mounts exactly the 13 M1 paths. The `/v1` OpenAPI golden
  (`tests/contracts/openapi/v1.json`) is identical in every valid switch combination.
- Settings errors never echo input values (`hide_input_in_errors`).

## Release gate

Run from the repository root. `make verify-influx` needs a reachable InfluxDB 2.7.11 (CI uses the
`influx` job's service container; locally, `scripts/dev/run_local_influxd.sh`). For a non-default
InfluxDB, set `INFLUX_INTEGRATION_URL`, `INFLUX_INTEGRATION_TOKEN` and `INFLUX_INTEGRATION_ORG`
(`tests/support/influx_harness.py`). It runs every Python
`tests/influx` test and the Go `TestInfluxIntegration*` tests, including the evaluator v2 parity
suite, with `-count=1`.

```bash
uv run --locked --extra dev pytest -q tests/contracts/test_p2_acceptance_gate.py tests/unit/test_architecture_inventory.py
scripts/dev/run_local_influxd.sh   # local InfluxDB 2.7.11, no Docker
RUN_INFLUX_INTEGRATION=1 uv run --locked --extra dev pytest -q tests/influx/test_p2_acceptance_influx.py
make verify-influx   # Python tests/influx + Go TestInfluxIntegration* (evaluator parity)
make verify-python
make verify-go
uv run --locked --extra dev python scripts/dev/export_v1_openapi.py --check
git diff --check
```

CI runs `make verify-python`, `make verify-go` and `make verify-influx` as the `python`, `go` and
`influx` jobs of `.github/workflows/verify.yml`.

### ADR gate map

Every ADR-005/006/007 gate (plan "Final Acceptance Matrix") maps to its proving tests or to a
deferral. Gate numbers refer to the P2-09 acceptance gate.

| Requirement | Proof |
|---|---|
| ADR-005 unit conversion | Gate 6: `tests/contracts/test_p2_acceptance_gate.py::test_gate6_unit_conversion_is_applied_and_versions_are_persisted`, `tests/influx/test_p2_acceptance_influx.py::test_gate6_and_gate8_stored_points_keep_conversion_versions_and_quality`; P2-02 `tests/unit/test_telemetry_units.py::test_non_identity_conversions_follow_the_locked_formula` |
| ADR-005 two probes for one metric | Gate 2: `tests/influx/test_p2_acceptance_influx.py::test_gate2_two_probes_are_distinct_v2_series_and_absent_from_v1`; P2-04 `tests/influx/test_influx_v2_reader_integration.py::test_two_probes_on_one_device_are_distinguishable_series` |
| ADR-005 trusted tenant mapping | Gate 3: `tests/contracts/test_p2_acceptance_gate.py::test_gate3_payload_ownership_claims_never_reach_stored_points`, `tests/contracts/test_p2_acceptance_gate.py::test_gate3_source_observation_rejects_every_ownership_field`, `tests/contracts/test_p2_acceptance_gate.py::test_gate3_dev_cli_takes_the_tenant_only_from_its_arguments` |
| ADR-005 `/v1` pivot equivalence | Gate 1: `tests/influx/test_p2_acceptance_influx.py::test_gate1_v1_responses_are_identical_in_both_read_modes`; P2-07 `tests/influx/test_v1_projection_equivalence.py::test_v1_http_responses_match_legacy` |
| ADR-005 bounded tag cardinality | Gate 7: `tests/influx/test_p2_acceptance_influx.py::test_gate7_tag_keys_are_the_allowlist_and_no_identity_or_quality_is_a_tag`; P2-00 `tests/unit/test_telemetry_v2_schema.py::test_guard_rejects_unknown_tags` |
| ADR-005 out-of-range never valid, with provenance | Gate 8: `tests/contracts/test_p2_acceptance_gate.py::test_gate8_out_of_range_is_stored_with_its_state_and_provenance`, `tests/influx/test_p2_acceptance_influx.py::test_gate6_and_gate8_stored_points_keep_conversion_versions_and_quality`; P2-02 `tests/unit/test_telemetry_quality.py::test_far_out_of_range_values_are_never_valid` |
| ADR-005 mapping versions persisted; replay after revision keeps version and identity | Versions on stored points (gate 6): `tests/influx/test_p2_acceptance_influx.py::test_gate6_and_gate8_stored_points_keep_conversion_versions_and_quality`; `tests/unit/test_telemetry_normalizer.py::test_replay_pinned_to_v1_reproduces_the_original_mapping`, `tests/unit/test_telemetry_pipeline.py::test_pinned_replay_through_the_pipeline_restores_the_v1_point_in_the_same_series`, `tests/unit/test_telemetry_v2_contracts.py::test_observation_event_id_ignores_versions_quality_and_delivery_times` |
| ADR-006 delayed/duplicated/replayed/out-of-order | Gate 4: `tests/influx/test_p2_acceptance_influx.py::test_gate4_duplicate_replay_reclassified_and_shuffled_writes_are_idempotent`; P2-02 `tests/unit/test_telemetry_quality.py::test_replay_marker_adds_replay_flag_and_provenance`; P2-03 `tests/influx/test_influx_v2_writer_integration.py::test_reclassified_redelivery_overwrites_every_field_of_the_same_point` |
| ADR-006 clock-skew flag | P2-02 `tests/unit/test_telemetry_quality.py::test_future_skew_of_300_seconds_plus_one_microsecond_is_flagged`, `tests/unit/test_telemetry_quality.py::test_delay_of_30_days_plus_one_microsecond_is_past_extreme_and_delayed` |
| ADR-006 deployment at event time; no fallback | Gate 5: `tests/contracts/test_p2_acceptance_gate.py::test_gate5_each_reading_resolves_the_deployment_of_its_event_time`, `tests/contracts/test_p2_acceptance_gate.py::test_gate5_replay_after_the_move_keeps_the_original_deployment`, `tests/contracts/test_p2_acceptance_gate.py::test_gate5_unresolved_deployment_is_quarantined_without_fallback` |
| ADR-006 quarantine persistence + connector health | **DEFERRED → #15** (D13): P2 delivers only the bounded `QuarantinedObservation` record and the no-fallback rule; quarantine/DLQ persistence and connector-health marking are P3 entry gates and block production canonical writes (D16). |
| ADR-006 current health uses receive/ingest time | `observations/latest` is a data view, not a health view (D11): `tests/influx/test_influx_v2_reader_integration.py::test_a_replay_with_newer_receipt_never_becomes_latest`. Health and online status are **DEFERRED → #17** (P6), built on `received_at`/`ingested_at`. |
| ADR-006 event-time alert windows, completeness delay, order independence | P2-05 `internal/alertevaluator/influx/parity_integration_test.go::TestInfluxIntegrationParityIsIndependentOfWriteOrder`; existing `AllowedLateness`/ordering tests `internal/alertevaluator/schedule_test.go::TestLatestCompleteSlotIsStableWithinCadence`, `internal/alertevaluator/quality_test.go::TestMeanSumsBucketsAndDevicesInAscendingOrder` |
| ADR-007 duplicate/ordering/conversion/cardinality/evaluator parity/visibility before enabling v2 reads | Gates 4, 6, 7 and 1: `tests/influx/test_p2_acceptance_influx.py::test_gate4_duplicate_replay_reclassified_and_shuffled_writes_are_idempotent`, `tests/influx/test_p2_acceptance_influx.py::test_gate6_and_gate8_stored_points_keep_conversion_versions_and_quality`, `tests/influx/test_p2_acceptance_influx.py::test_gate7_tag_keys_are_the_allowlist_and_no_identity_or_quality_is_a_tag`, `tests/influx/test_p2_acceptance_influx.py::test_gate1_v1_responses_are_identical_in_both_read_modes`; evaluator parity `internal/alertevaluator/influx/parity_integration_test.go::TestInfluxIntegrationParityScenarios` |
| ADR-007 outage after durable acceptance: no ack, recovery without loss | Gate 9: `tests/influx/test_p2_acceptance_influx.py::test_gate9_outage_never_acknowledges_and_redelivery_recovers_the_exact_points`, `tests/influx/test_p2_acceptance_influx.py::test_gate9_permanent_write_error_is_never_acknowledged`; P2-06 `tests/unit/test_telemetry_pipeline.py::test_failed_ingest_is_never_acknowledged_and_redelivery_matches_the_baseline`. Durable SQS redrive is **DEFERRED → #15** (D13) and blocks production dual write (D16). |
| ADR-007 each write path disabled independently | P2-03 `tests/unit/test_telemetry_v2_write_service.py::test_v2_only_writes_every_observation_and_no_legacy_point`, `tests/unit/test_telemetry_v2_write_service.py::test_legacy_only_writes_the_legacy_point_and_no_v2_observation`; P2-08 `tests/api/test_v2_telemetry_composition.py::test_lifespan_creates_one_synchronous_writer_only_when_a_write_switch_is_on`, `tests/api/test_v2_telemetry_composition.py::test_lifespan_without_a_write_switch_creates_no_write_api` |

## D16 precondition

P2 is a rehearsal only: production enablement of any read switch is a P3 exit step, after P3 delivers
a producer that writes every legacy device to `observation_numeric`.

`ALERT_EVALUATOR_TELEMETRY_SOURCE=v2` and `V1_TELEMETRY_READ_SOURCE=v2` may be enabled only in an
environment where both of these hold:

- a producer writes every legacy device to `observation_numeric`;
- the [keyed divergence checks](#keyed-divergence-checks) report zero missing or different
  `(pond, device, metric, _time, value)` tuples over max(longest rule window, legacy read window),
  for every pond of every tenant in that environment.

## Rollout sequence

Each step is a separate change with its own soak time. Never skip a step; roll back with
[Rollback](#rollback).

1. Set `TELEMETRY_V2_WRITE_ENABLED=true` in the producer (the P3 worker; the dev CLI in a rehearsal).
   Only `observation_numeric` is written; legacy reads and alerts are unchanged.
2. Run the keyed divergence checks for every pond until they pass (see the pass criterion) over the
   D16 window.
   Telegraf still writes `water_quality`, so the checks compare the two producers.
3. Set `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=true` only after the P3 durable redrive test passes
   (#15): a legacy write that fails after acceptance must be redelivered, never dropped.
4. Set `API_V2_ENABLED=true` and `API_V2_TELEMETRY_ENABLED=true` to expose the three `/v2` telemetry
   reads.
5. Once the D16 precondition holds, set `ALERT_EVALUATOR_TELEMETRY_SOURCE=v2` in one environment first, and compare alert outcomes with
   a `legacy` evaluator there before any wider rollout. Expect the D15 divergence.
6. Set `V1_TELEMETRY_READ_SOURCE=v2` only after the `observation_numeric` retention covers the legacy
   read window (see [Retention](#retention)) and the D16 precondition holds.

## Keyed divergence checks

The checks compare each pond's `water_quality` points with its `observation_numeric` points key by
key. Counts alone are not sufficient: equal counts can hide a missing point balanced by an extra one,
or a changed value. Each check returns **only divergent rows**; an empty result means no divergence.
Run all three for every pond over the D16 window. An empty diff proves nothing if a placeholder is
wrong on both sides (for example `<pond_id>` or `<asset_id>`), so first confirm that both
measurements have points for the pond in the window, for example with a `count()` of the `temp_c`
field in `water_quality` and of the `value` field in `observation_numeric` under the same filters.

**Pass criterion.** A pond passes when `latest` and `summary` return no rows and `points` returns no
row whose `field` is a metric field. `points` rows with `field = "seq"` are diagnostics: review each
one, and accept it only when it is the D10 present-to-absent case (`v2` = `missing` for a reading
whose sequence was removed). Any other `seq` row, or any metric-field row, blocks the D16
precondition.

| Check | Key | Compares | Divergent row columns |
|---|---|---|---|
| `points` | `(pond, device, field, _time)` | every legacy field value and `seq`, in both directions | `_time`, `device_id`, `field`, `legacy`, `v2` |
| `latest` | `(pond, device, field)` | the newest `_time` and value per device and field (hence latest per pond) | `device_id`, `field`, `legacy`, `v2` |
| `summary` | `(pond, field)` for `temp_c`, `ph`, `do_mg_l` | count, min and max exactly; mean within relative `1e-12` (r7.2) | `field` plus each side's count, sum, min and max |

A missing tuple shows as `missing` on its side. `points` compares the canonical value as stored, so a
legacy integer field (`rssi`) and its float v2 value compare equal when the numbers are equal. `seq`
is compared against the metric rows of the reading whose sequence is present (`-1` means absent),
which is how the `/v1` projection picks `seq`. A reading whose every metric was rewritten without a
sequence (the D10 case) is reported; a partial rewrite of some metrics without a sequence is not,
because the projection still serves the remaining rows' `seq`, as legacy does. `latest` and
`summary` cover the metric fields only.

`points` and `latest` compare keys, so a duplicate v2 series with an equal value (for example the
backdated-Deployment case below) is visible only to `summary`, and only for its three fields. A `seq`
divergence is reported once per v2 metric row of that reading (up to seven identical rows).

### Inputs

Replace the placeholders before running a check (for example in the InfluxDB UI or with
`influx query`):

| Placeholder | Value |
|---|---|
| `<bucket>` | `INFLUXDB_BUCKET_RAW` |
| `<tenant_id>` | the tenant |
| `<pond_id>` | the legacy pond |
| `<asset_id>` | `asset_id_for_legacy_pond(tenant_id, pond_id)` |
| `<component_ids>` | comma-separated quoted `default_component_id_for_legacy_device(tenant_id, device_id)` of every legacy device of the tenant (at least every device that reported to this pond in the window; the `<asset_id>` filter already limits the pond) |
| `<start>`, `<stop>` | RFC 3339 bounds covering the D16 window, for example `2026-10-01T00:00:00Z` |

The IDs come from `limnopulse_api.domain.ids`:

```bash
uv run --locked python -c 'from limnopulse_api.domain.ids import asset_id_for_legacy_pond as a, default_component_id_for_legacy_device as c; print(a("tnt_local_001", "pond_local_001"), c("tnt_local_001", "local-device-001"))'
```

The component filter keeps native v2 Components (including a native Component on a legacy device)
out of the comparison, exactly as the `/v1` projection and the evaluator v2 reader do. The `flux`
blocks below are executed against InfluxDB 2.7.11 by `tests/influx/test_p2_acceptance_influx.py`
(`test_runbook_*`), which seeds dual-written data, injects divergences and checks each report.

### Points

```flux
// divergence-check: points
import "array"
import "join"

bucket = "<bucket>"
tenant = "<tenant_id>"
pond = "<pond_id>"
asset = "<asset_id>"
// The legacy default Components of the tenant's legacy devices (see "Inputs").
components = [<component_ids>]
start = <start>
stop = <stop>

// join.full rejects an empty input, so each side carries one placeholder row (device_id "");
// the final `device_id != ""` filter drops it.
placeholder = (value) => array.from(rows: [{_time: 1970-01-01T00:00:00Z, device_id: "", _value: value}])

legacy = (field, empty) =>
    union(
        tables: [
            placeholder(value: empty),
            from(bucket: bucket)
                |> range(start: start, stop: stop)
                |> filter(fn: (r) => r._measurement == "water_quality")
                |> filter(fn: (r) => r.tenant_id == tenant and r.pond_id == pond)
                |> filter(fn: (r) => r._field == field)
                |> keep(columns: ["_time", "device_id", "_value"]),
        ],
    )
        |> group()

v2 = (metric, column, empty) =>
    union(
        tables: [
            placeholder(value: empty),
            from(bucket: bucket)
                |> range(start: start, stop: stop)
                |> filter(fn: (r) => r._measurement == "observation_numeric")
                |> filter(fn: (r) => r.tenant_id == tenant and r.asset_id == asset)
                |> filter(fn: (r) => r._field == column and (metric == "*" or r.metric_key == metric))
                |> filter(fn: (r) => contains(value: r.component_id, set: components))
                |> keep(columns: ["_time", "device_id", "_value"]),
        ],
    )
        |> group()

// Both directions: a tuple missing from either side is reported as "missing".
compare = (field, left, right) =>
    join.full(
        left: left,
        right: right,
        on: (l, r) => l._time == r._time and l.device_id == r.device_id,
        as: (l, r) => ({
            _time: if exists l._time then l._time else r._time,
            device_id: if exists l.device_id then l.device_id else r.device_id,
            field: field,
            legacy: if exists l._value then string(v: l._value) else "missing",
            v2: if exists r._value then string(v: r._value) else "missing",
        }),
    )

value = (field, metric, empty) =>
    compare(
        field: field,
        left: legacy(field: field, empty: empty) |> toFloat(),
        right: v2(metric: metric, column: "value", empty: 0.0),
    )

union(
    tables: [
        value(field: "temp_c", metric: "water.temperature", empty: 0.0),
        value(field: "ph", metric: "water.ph", empty: 0.0),
        value(field: "do_mg_l", metric: "water.dissolved_oxygen", empty: 0.0),
        value(field: "turbidity_ntu", metric: "water.turbidity", empty: 0.0),
        value(field: "salinity_ppt", metric: "water.salinity", empty: 0.0),
        value(field: "battery_v", metric: "device.battery_voltage", empty: 0.0),
        value(field: "rssi", metric: "device.rssi", empty: 0),
        compare(
            field: "seq",
            left: legacy(field: "seq", empty: 0),
            // Every metric of a reading carries its `sequence`; -1 means absent (no legacy seq).
            right: v2(metric: "*", column: "sequence", empty: 0) |> filter(fn: (r) => r._value != -1),
        ),
    ],
)
    |> filter(fn: (r) => r.device_id != "" and r.legacy != r.v2)
    |> sort(columns: ["_time", "device_id", "field"])
```

### Latest

```flux
// divergence-check: latest
import "array"
import "join"

bucket = "<bucket>"
tenant = "<tenant_id>"
pond = "<pond_id>"
asset = "<asset_id>"
// The legacy default Components of the tenant's legacy devices (see "Inputs").
components = [<component_ids>]
start = <start>
stop = <stop>

// join.full rejects an empty input, so each side carries one placeholder row (device_id "");
// the final `device_id != ""` filter drops it.
placeholder = array.from(rows: [{device_id: "", latest: ""}])

newest = (tables=<-) =>
    tables
        |> group(columns: ["device_id"])
        // A device can have several series (e.g. after moving away and back): order by time.
        |> sort(columns: ["_time"])
        |> last()
        |> map(
            fn: (r) => ({device_id: r.device_id, latest: string(v: r._time) + " " + string(v: float(v: r._value))}),
        )

legacy = (field) =>
    union(
        tables: [
            placeholder,
            from(bucket: bucket)
                |> range(start: start, stop: stop)
                |> filter(fn: (r) => r._measurement == "water_quality")
                |> filter(fn: (r) => r.tenant_id == tenant and r.pond_id == pond)
                |> filter(fn: (r) => r._field == field)
                |> newest(),
        ],
    )
        |> group()

v2 = (metric) =>
    union(
        tables: [
            placeholder,
            from(bucket: bucket)
                |> range(start: start, stop: stop)
                |> filter(fn: (r) => r._measurement == "observation_numeric")
                |> filter(fn: (r) => r.tenant_id == tenant and r.asset_id == asset)
                |> filter(fn: (r) => r.metric_key == metric and r._field == "value")
                |> filter(fn: (r) => contains(value: r.component_id, set: components))
                |> newest(),
        ],
    )
        |> group()

latest = (field, metric) =>
    join.full(
        left: legacy(field: field),
        right: v2(metric: metric),
        on: (l, r) => l.device_id == r.device_id,
        as: (l, r) => ({
            device_id: if exists l.device_id then l.device_id else r.device_id,
            field: field,
            legacy: if exists l.latest then l.latest else "missing",
            v2: if exists r.latest then r.latest else "missing",
        }),
    )

union(
    tables: [
        latest(field: "temp_c", metric: "water.temperature"),
        latest(field: "ph", metric: "water.ph"),
        latest(field: "do_mg_l", metric: "water.dissolved_oxygen"),
        latest(field: "turbidity_ntu", metric: "water.turbidity"),
        latest(field: "salinity_ppt", metric: "water.salinity"),
        latest(field: "battery_v", metric: "device.battery_voltage"),
        latest(field: "rssi", metric: "device.rssi"),
    ],
)
    |> filter(fn: (r) => r.device_id != "" and r.legacy != r.v2)
    |> sort(columns: ["device_id", "field"])
```

### Summary

```flux
// divergence-check: summary
import "math"

bucket = "<bucket>"
tenant = "<tenant_id>"
pond = "<pond_id>"
asset = "<asset_id>"
// The legacy default Components of the tenant's legacy devices (see "Inputs").
components = [<component_ids>]
start = <start>
stop = <stop>

legacy = (field) =>
    from(bucket: bucket)
        |> range(start: start, stop: stop)
        |> filter(fn: (r) => r._measurement == "water_quality")
        |> filter(fn: (r) => r.tenant_id == tenant and r.pond_id == pond)
        |> filter(fn: (r) => r._field == field)
        |> keep(columns: ["_value"])
        |> set(key: "field", value: field)
        |> set(key: "side", value: "legacy")

v2 = (field, metric) =>
    from(bucket: bucket)
        |> range(start: start, stop: stop)
        |> filter(fn: (r) => r._measurement == "observation_numeric")
        |> filter(fn: (r) => r.tenant_id == tenant and r.asset_id == asset)
        |> filter(fn: (r) => r.metric_key == metric and r._field == "value")
        |> filter(fn: (r) => contains(value: r.component_id, set: components))
        |> keep(columns: ["_value"])
        |> set(key: "field", value: field)
        |> set(key: "side", value: "v2")

big = math.maxfloat

union(
    tables: [
        legacy(field: "temp_c"),
        legacy(field: "ph"),
        legacy(field: "do_mg_l"),
        v2(field: "temp_c", metric: "water.temperature"),
        v2(field: "ph", metric: "water.ph"),
        v2(field: "do_mg_l", metric: "water.dissolved_oxygen"),
    ],
)
    |> group(columns: ["field"])
    |> reduce(
        identity: {
            legacy_count: 0,
            legacy_sum: 0.0,
            legacy_min: big,
            legacy_max: -big,
            v2_count: 0,
            v2_sum: 0.0,
            v2_min: big,
            v2_max: -big,
        },
        fn: (r, accumulator) => {
            isLegacy = r.side == "legacy"

            return {
                legacy_count: accumulator.legacy_count + (if isLegacy then 1 else 0),
                legacy_sum: accumulator.legacy_sum + (if isLegacy then r._value else 0.0),
                legacy_min: if isLegacy and r._value < accumulator.legacy_min then r._value else accumulator.legacy_min,
                legacy_max: if isLegacy and r._value > accumulator.legacy_max then r._value else accumulator.legacy_max,
                v2_count: accumulator.v2_count + (if isLegacy then 0 else 1),
                v2_sum: accumulator.v2_sum + (if isLegacy then 0.0 else r._value),
                v2_min: if not isLegacy and r._value < accumulator.v2_min then r._value else accumulator.v2_min,
                v2_max: if not isLegacy and r._value > accumulator.v2_max then r._value else accumulator.v2_max,
            }
        },
    )
    |> group()
    |> filter(
        fn: (r) => {
            legacyMean = r.legacy_sum / float(v: if r.legacy_count > 0 then r.legacy_count else 1)
            v2Mean = r.v2_sum / float(v: if r.v2_count > 0 then r.v2_count else 1)

            return r.legacy_count != r.v2_count or r.legacy_min != r.v2_min or
                r.legacy_max != r.v2_max or
                math.abs(x: legacyMean - v2Mean) > 0.000000000001 * math.mMax(x: math.abs(x: legacyMean), y: math.abs(x: v2Mean))
        },
    )
    |> sort(columns: ["field"])
```

## Stop-dual-write criteria

Spec §10.6: stop legacy dual write only after `/v1` compatibility reads are proven against v2, or once
the legacy retention window expires. In practice, all of these hold in the environment:

- `V1_TELEMETRY_READ_SOURCE=v2` and `ALERT_EVALUATOR_TELEMETRY_SOURCE=v2` have served for at least
  max(longest rule window, legacy read window) with clean keyed divergence checks and no reported
  regression;
- no `/v1` consumer depends on `water_quality` beyond the legacy retention window, or that window has
  expired for all data written before the switch;
- Telegraf (or any other `water_quality` producer) is retired or its data is no longer read.

Stopping dual write means setting `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=false`. It deletes nothing.

## Rollback

Before cutover (while Telegraf still writes `water_quality`), rollback is by switches only. Set
them back to their defaults and restart (or redeploy) the producer (the P3 worker; the dev CLI reads
them per run), the API and the evaluator:

- `TELEMETRY_V2_WRITE_ENABLED=false`
- `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=false`
- `API_V2_TELEMETRY_ENABLED=false`
- `V1_TELEMETRY_READ_SOURCE=legacy`
- `ALERT_EVALUATOR_TELEMETRY_SOURCE=legacy`

Roll back in the reverse order of the [rollout sequence](#rollout-sequence); each switch is
independent. Set `API_V2_TELEMETRY_ENABLED=false` before or with any change to `API_V2_ENABLED`
(the API refuses to start with the telemetry routes on and `API_V2_ENABLED=false`); reset
`API_V2_ENABLED` only if it was `false` before step 4.

**After cutover** (the stop-dual-write criteria have retired every `water_quality` producer), the
defaults above are not safe on their own: with Telegraf retired and dual write off, nothing writes
`water_quality`, so `/v1` serves stale data and legacy alert windows miss new samples. Instead:

1. Restore a `water_quality` producer first: re-enable Telegraf, or set
   `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=true` (with `TELEMETRY_V2_WRITE_ENABLED` as needed) on a
   running producer.
2. Switch `V1_TELEMETRY_READ_SOURCE` and `ALERT_EVALUATOR_TELEMETRY_SOURCE` back to `legacy` only
   once `water_quality` holds the legacy read window and the longest rule window again. Nothing
   rewrites `water_quality` from `observation_numeric`: dual write runs only on the normalizer ingest
   path. Filling a gap means replaying the retained source observations through the pipeline with the
   replay marker and their first-receipt context (D5), which depends on P3 retention and durable
   redrive (#15); otherwise wait until new data covers both windows.
3. Return the write switches to their defaults only if Telegraf is the restored producer. If legacy
   dual write is the restored producer, keep it and its producer on.

Never delete points from `observation_numeric` or `water_quality`: canonical points are
idempotent by identity, so writing again after a rollback overwrites the same points, and the legacy
reads never depended on them.

## Retention

D6: `observation_numeric` lives in the existing `INFLUXDB_BUCKET_RAW` bucket, next to `water_quality`;
there is no per-tenant or per-plan bucket. Before `V1_TELEMETRY_READ_SOURCE=v2`, the bucket's
retention must be at least the legacy read window (the longest `/v1` summary period, `30d`, and any
longer `/v1` readings range in use), and `observation_numeric` must already hold data for the whole
window.

## Known divergences and limitations

- **D15 out-of-range values.** An out-of-range value is written with `quality_state=out_of_range` and
  provenance `plausible_range`, to v2 and (when eligible) to legacy. Legacy reads and legacy alerts
  still show it, as today; the v2 quality-gated alert reads (`quality_state == "valid"`) exclude it,
  so an alert evaluator on `v2` can stay silent where `legacy` fires.
- **D10 present-to-absent `seq`.** An identity rewritten from a present to an absent `sequence`
  keeps the old legacy `seq` (the legacy line omits `seq`, as Telegraf does), while v2 stores `-1`.
  This case is outside `/v1` parity and shows up in the `points` check as a `seq` divergence
  (`v2` = `missing`). It does not block the D16 precondition, which keys on value tuples (see the pass criterion under
  [Keyed divergence checks](#keyed-divergence-checks)).
- **Backdated Deployment end (P2-07).** A redelivery or correction after a Deployment was ended
  with a backdated `ended_at` can resolve to a different `deployment_id`, so one legacy point becomes
  two v2 series. The projection's readings and latest pick one series by series-key order; its
  summary counts both.
- **`seq` winner order (P2-07).** The projection ranks the `seq` winner by `ingested_at` (then
  `event_id`), while legacy keeps the last write when batches land out of `ingested_at` order.
- **Domain-listed devices and ponds (D10, P2-07).** The projection's component set comes from the
  current `list_devices`, and ponds from the domain repository. Data of a device or pond that was
  deleted or is no longer listed disappears from the projection, while legacy reads (by tenant and
  pond only) still show it. A tenant with no devices or ponds in the domain gets an empty projection.
  The tenant-latest `seq` window widens when one pond is stale (a cost issue only).
- **Summary relaxation (r7.2).** The `/v1` projection uses map-free Flux plus Python shaping on
  InfluxDB 2.7.11. Readings, latest and tenant-latest are byte-identical; in summaries, min, max,
  count and the window set are exact and the float means match within relative `1e-12`, because
  storage sums partials in series-key order and the v2 series keys differ by design. Summary
  equivalence covers completed windows only: the last window is stamped with the query's `now()`.
- **Go v2 reader IDs (#71).** The evaluator v2 reader escapes IDs in Flux literals rather than
  validating them, the same model as the legacy reader; this is an explicit exemption from the
  plan's "every ID is validated" constraint.
- **Flux string literals (#84).** Python `flux_string_literal` and Go `fluxV2String`
  (`internal/alertevaluator/influx/query_v2.go`) apply JSON escaping, then `${` → `\${`. They are
  not lossless for `$${`, `$"` or non-ASCII characters, and Go `json.Marshal` also escapes `<`, `>`,
  `&` and U+2028. They fail safe (no interpolation) and are unreachable with today's IDs; lossless
  literals are backlog #84.
- **InfluxDB version.** CI pins the `influx` job to `influxdb:2.7.11@sha256:d92a10e9e75aff18eca38ff3a8f0b4a800706a5dd44d1b0ece264af04458525b`
  (a true 2.7.11; the `influxdb:2.7.11-alpine` tag drifted to 2.7.12, plan erratum r7.1).
  Production compose uses the floating `influxdb:2-alpine`. The projection's Flux shapes and the
  divergence checks follow 2.7.11 behavior; re-run the release gate before any upgrade.
- **Writer error classes (P2-03).** A TLS misconfiguration (status 0), a transport failure
  (urllib3 `HTTPError` or `OSError`), 429, 5xx and an Asset lookup failure are retryable; 422 (a
  partial write) and other 4xx, including an auth or bucket misconfiguration (401/403/404), are
  permanent; over-limit batches and unclassified failures are not retryable. Every write failure is
  a `TelemetryWriteError`, and the pipeline never acknowledges on `TelemetryWriteError`: the caller
  redelivers, or dead-letters a non-retryable failure (P3). Through the pipeline, an over-limit batch
  or an unknown mapping version raises `ValueError` in the normalizer before any write (caller
  misuse); it is never acknowledged either.

## Out of scope

- **D13 → #15 (P3 entry gates; block production canonical writes, D16):** quarantine/DLQ
  persistence, connector-health marking, device and component last-seen, operator reconciliation
  (§10.2), external-identity-to-DeviceIntegration resolution, sequence gap/regression state, durable
  queue redrive, and emission of the §23.2 metrics.
- **D17 AlertRuleV2 (#77):** `/v2/alert-rules`, §16.2 selectors, §16.3 declared quality policies and
  §7.3 `metric_key` rules, after P3/P6.
- **P6 calibration states:** `suspect`, `uncalibrated`, `calibration_unknown` and `impossible_jump`
  are reserved; P6 decides their thresholds before writing them.
- **#6:** presence-aware `Evaluation.Value` stays canonical in #6; the P2 Go reader reports values
  only through the unchanged `WindowResult`.
- **#84:** lossless Flux string literals.
- **#17:** health and online-status views built on `received_at`/`ingested_at` (P6).
