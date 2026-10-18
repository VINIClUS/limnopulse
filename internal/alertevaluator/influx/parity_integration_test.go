package influx

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/domain"
)

var parityScenarios = []string{
	"legacy_sample", "sub_second", "delayed_replay", "relocation", "unit_converted", "multi_point_series",
}

var parityAggregations = []alertevaluator.Aggregation{
	alertevaluator.AggregationMin, alertevaluator.AggregationMax,
	alertevaluator.AggregationMean, alertevaluator.AggregationLast,
}

type parityReaders struct {
	legacy WindowReader
	v2     V2WindowReader
}

func newParityReaders(client influxdb2.Client, bucket *domain.Bucket) parityReaders {
	queryAPI := client.QueryAPI(*bucket.OrgID)
	return parityReaders{
		legacy: WindowReader{Bucket: bucket.Name, Querier: ClientQuerier{API: queryAPI}},
		v2:     V2WindowReader{Bucket: bucket.Name, Querier: ClientV2Querier{API: queryAPI}},
	}
}

// parityGroup is every eligible observation of one legacy pond and metric.
type parityGroup struct {
	pond    string
	metric  string
	devices []string
	times   []time.Time
	points  map[string]int // device -> observation count
}

type parityCase struct {
	name string
	rule alertevaluator.EvaluationRule
	spec alertevaluator.WindowSpec
	// points is the number of canonical samples the legacy reader must see.
	points int
}

func legacyFieldByMetricKey(t *testing.T, vectors contractVectors) map[string]string {
	t.Helper()
	fields := map[string]string{}
	for _, row := range vectors.MetricCatalog {
		if row.LegacyField != nil {
			fields[row.MetricKey] = *row.LegacyField
		}
	}
	return fields
}

func observedAt(t *testing.T, observation vectorObservation) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, observation.Observation.ObservedAt)
	if err != nil {
		t.Fatalf("observed_at %q: %v", observation.Observation.ObservedAt, err)
	}
	return parsed.UTC()
}

func parityGroups(t *testing.T, vectors contractVectors, scenario vectorScenario) []parityGroup {
	t.Helper()
	fields := legacyFieldByMetricKey(t, vectors)
	groups := map[string]*parityGroup{}
	for _, observation := range scenario.Observations {
		if observation.Quality.State != "valid" {
			t.Fatalf("parity scenario holds a %q observation", observation.Quality.State)
		}
		pond, ok := scenario.LegacyPondByAsset[observation.Resolved.AssetID]
		if !ok {
			t.Fatalf("parity observation on non-legacy asset %q", observation.Resolved.AssetID)
		}
		field, ok := fields[observation.Observation.MetricKey]
		if !ok {
			t.Fatalf("metric %q has no legacy field", observation.Observation.MetricKey)
		}
		key := pond + "/" + field
		group, ok := groups[key]
		if !ok {
			group = &parityGroup{pond: pond, metric: field, points: map[string]int{}}
			groups[key] = group
		}
		device := observation.Resolved.DeviceID
		if group.points[device] == 0 {
			group.devices = append(group.devices, device)
		}
		group.points[device]++
		group.times = append(group.times, observedAt(t, observation))
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([]parityGroup, 0, len(keys))
	for _, key := range keys {
		group := groups[key]
		sort.Strings(group.devices)
		sort.Slice(group.times, func(a, b int) bool { return group.times[a].Before(group.times[b]) })
		ordered = append(ordered, *group)
	}
	if len(ordered) == 0 {
		t.Fatal("scenario has no parity groups")
	}
	return ordered
}

func scenarioSpan(t *testing.T, scenario vectorScenario) (time.Time, time.Time) {
	t.Helper()
	var first, last time.Time
	for _, observation := range scenario.Observations {
		at := observedAt(t, observation)
		if first.IsZero() || at.Before(first) {
			first = at
		}
		if at.After(last) {
			last = at
		}
	}
	return first, last
}

// paritySpecs gives a tight window in which every group must be sufficient,
// and a wide window over the whole scenario with a minimal coverage bar.
func paritySpecs(t *testing.T, scenario vectorScenario, group parityGroup) (tight, wide alertevaluator.WindowSpec) {
	t.Helper()
	first, last := group.times[0], group.times[len(group.times)-1]
	if first.Equal(last) {
		start := first.Truncate(10 * time.Second)
		tight = alertevaluator.WindowSpec{
			Start: start, End: start.Add(10 * time.Second), ExpectedSampleInterval: 10 * time.Second,
			EvaluationCadence: 10 * time.Second, MinimumCoverageRatio: 1, MinimumPoints: 1,
			MaximumSampleAge: 10 * time.Second,
		}
	} else {
		start := first.Truncate(time.Minute)
		tight = alertevaluator.WindowSpec{
			Start: start, End: last.Truncate(time.Minute).Add(time.Minute), ExpectedSampleInterval: time.Minute,
			EvaluationCadence: time.Minute, MinimumCoverageRatio: .8, MinimumPoints: 3,
			MaximumSampleAge: 2 * time.Minute,
		}
	}
	spanStart, spanEnd := scenarioSpan(t, scenario)
	wideStart := spanStart.Truncate(time.Hour)
	wideEnd := spanEnd.Truncate(time.Hour).Add(time.Hour)
	wide = alertevaluator.WindowSpec{
		Start: wideStart, End: wideEnd, ExpectedSampleInterval: time.Minute,
		EvaluationCadence: wideEnd.Sub(wideStart), MinimumCoverageRatio: 1e-9, MinimumPoints: 1,
		MaximumSampleAge: wideEnd.Sub(wideStart),
	}
	return tight, wide
}

func parityCases(t *testing.T, vectors contractVectors, scenario vectorScenario) []parityCase {
	t.Helper()
	var cases []parityCase
	for _, group := range parityGroups(t, vectors, scenario) {
		tight, wide := paritySpecs(t, scenario, group)
		scopes := []string{""}
		scopes = append(scopes, group.devices...)
		for _, device := range scopes {
			points := 0
			for candidate, count := range group.points {
				if device == "" || candidate == device {
					points += count
				}
			}
			for _, aggregation := range parityAggregations {
				rule := alertevaluator.EvaluationRule{
					Rule:   alertevaluator.Rule{TenantID: scenario.TenantID},
					PondID: group.pond, DeviceID: device, Metric: group.metric, Aggregation: aggregation,
				}
				scope := "pond"
				if device != "" {
					scope = "device=" + device
				}
				for _, window := range []struct {
					name string
					spec alertevaluator.WindowSpec
				}{{"tight", tight}, {"wide", wide}} {
					spec := window.spec
					spec.DeviceID = device
					spec.Aggregation = aggregation
					cases = append(cases, parityCase{
						name: fmt.Sprintf("%s/%s/%s/%s/%s", group.pond, group.metric, scope, aggregation, window.name),
						rule: rule, spec: spec, points: points,
					})
				}
			}
		}
	}
	return cases
}

func seedScenario(t *testing.T, client influxdb2.Client, bucket *domain.Bucket, scenario vectorScenario) {
	t.Helper()
	lines := append(append([]string(nil), scenario.LegacyLines...), scenario.V2Lines...)
	if len(lines) == 0 {
		t.Fatal("scenario has no lines")
	}
	writeLines(t, client, bucket, lines...)
}

func assertSameWindowResult(t *testing.T, label string, legacy, v2 alertevaluator.WindowResult) {
	t.Helper()
	if legacy.Quality != v2.Quality || math.Float64bits(legacy.Value) != math.Float64bits(v2.Value) ||
		legacy.CoverageRatio != v2.CoverageRatio || legacy.ExpectedBucketCount != v2.ExpectedBucketCount ||
		legacy.OccupiedBucketCount != v2.OccupiedBucketCount || legacy.ValidPointCount != v2.ValidPointCount ||
		legacy.ContributingDeviceCount != v2.ContributingDeviceCount || !legacy.LatestSampleAt.Equal(v2.LatestSampleAt) {
		t.Errorf("%s: v2 result differs from legacy\nlegacy: %#v\n    v2: %#v", label, legacy, v2)
	}
}

func evaluateBoth(t *testing.T, readers parityReaders, rule alertevaluator.EvaluationRule, spec alertevaluator.WindowSpec) (legacy, v2 alertevaluator.WindowResult) {
	t.Helper()
	ctx := context.Background()
	legacy, err := readers.legacy.Evaluate(ctx, rule, spec)
	if err != nil {
		t.Fatalf("legacy reader: %v", err)
	}
	v2, err = readers.v2.Evaluate(ctx, rule, spec)
	if err != nil {
		t.Fatalf("v2 reader: %v", err)
	}
	return legacy, v2
}

func assertParity(t *testing.T, readers parityReaders, cases []parityCase) {
	t.Helper()
	for _, test := range cases {
		legacy, v2 := evaluateBoth(t, readers, test.rule, test.spec)
		if legacy.ValidPointCount != test.points || legacy.ValidPointCount == 0 {
			t.Errorf("%s: legacy saw %d points, want %d (seeded data missing)", test.name, legacy.ValidPointCount, test.points)
			continue
		}
		if legacy.Quality != alertevaluator.QualitySufficient {
			t.Errorf("%s: legacy quality %s, want sufficient so Value is compared", test.name, legacy.Quality)
			continue
		}
		assertSameWindowResult(t, test.name, legacy, v2)
	}
}

func TestInfluxIntegrationParityScenarios(t *testing.T) {
	vectors := loadContractVectors(t)
	for _, name := range parityScenarios {
		t.Run(name, func(t *testing.T) {
			scenario, ok := vectors.Scenarios[name]
			if !ok {
				t.Fatalf("scenario %q missing from contract vectors", name)
			}
			client, bucket := newIntegrationBucket(t)
			seedScenario(t, client, bucket, scenario)
			cases := parityCases(t, vectors, scenario)
			assertParity(t, newParityReaders(client, bucket), cases)
		})
	}
}

func TestInfluxIntegrationParityIsIndependentOfWriteOrder(t *testing.T) {
	vectors := loadContractVectors(t)
	scenario := vectors.Scenarios["multi_point_series"]
	cases := parityCases(t, vectors, scenario)

	client, ordered := newIntegrationBucket(t)
	seedScenario(t, client, ordered, scenario)
	reference := make([]alertevaluator.WindowResult, len(cases))
	orderedReaders := newParityReaders(client, ordered)
	for index, test := range cases {
		legacy, _ := evaluateBoth(t, orderedReaders, test.rule, test.spec)
		if legacy.ValidPointCount == 0 {
			t.Fatalf("%s: reference read is empty", test.name)
		}
		reference[index] = legacy
	}

	lines := append(append([]string(nil), scenario.V2Lines...), scenario.LegacyLines...)
	random := rand.New(rand.NewSource(71))
	for round := 0; round < 3; round++ {
		shuffled := append([]string(nil), lines...)
		if round == 0 {
			for left, right := 0, len(shuffled)-1; left < right; left, right = left+1, right-1 {
				shuffled[left], shuffled[right] = shuffled[right], shuffled[left]
			}
		} else {
			random.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		}
		t.Run(fmt.Sprintf("order-%d", round), func(t *testing.T) {
			client, bucket := newIntegrationBucket(t)
			// Several batches, so arrival order also differs across writes.
			for start := 0; start < len(shuffled); start += 37 {
				end := start + 37
				if end > len(shuffled) {
					end = len(shuffled)
				}
				writeLines(t, client, bucket, shuffled[start:end]...)
			}
			readers := newParityReaders(client, bucket)
			for index, test := range cases {
				legacy, v2 := evaluateBoth(t, readers, test.rule, test.spec)
				assertSameWindowResult(t, test.name+" legacy vs ordered", reference[index], legacy)
				assertSameWindowResult(t, test.name+" v2 vs ordered", reference[index], v2)
			}
		})
	}
}

func dissolvedOxygenSpec(at time.Time, device string, aggregation alertevaluator.Aggregation) alertevaluator.WindowSpec {
	start := at.Truncate(10 * time.Second)
	return alertevaluator.WindowSpec{
		Start: start, End: start.Add(10 * time.Second), DeviceID: device, ExpectedSampleInterval: 10 * time.Second,
		EvaluationCadence: 10 * time.Second, MinimumCoverageRatio: 1, MinimumPoints: 1,
		MaximumSampleAge: 10 * time.Second, Aggregation: aggregation,
	}
}

func onlyObservationTime(t *testing.T, scenario vectorScenario) time.Time {
	t.Helper()
	first, last := scenarioSpan(t, scenario)
	if !first.Equal(last) {
		t.Fatalf("scenario spans %s..%s, want one instant", first, last)
	}
	return first
}

func TestInfluxIntegrationOutOfRangeDivergesFromLegacy(t *testing.T) {
	vectors := loadContractVectors(t)
	scenario := vectors.Scenarios["out_of_range"]
	if len(scenario.Observations) != 1 || scenario.Observations[0].Quality.State != "out_of_range" {
		t.Fatalf("out_of_range scenario = %#v", scenario.Observations)
	}
	observation := scenario.Observations[0]
	at := observedAt(t, observation)
	client, bucket := newIntegrationBucket(t)
	seedScenario(t, client, bucket, scenario)

	// The point is stored in observation_numeric, flagged out_of_range.
	flux := fmt.Sprintf(`from(bucket: %q)
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r["_measurement"] == "observation_numeric" and r["_field"] == "quality_state")`,
		bucket.Name, at.Format(time.RFC3339Nano), at.Add(time.Second).Format(time.RFC3339Nano))
	result, err := client.QueryAPI(*bucket.OrgID).Query(context.Background(), flux)
	if err != nil {
		t.Fatal(err)
	}
	var states []any
	for result.Next() {
		states = append(states, result.Record().Value())
	}
	if result.Err() != nil || len(states) != 1 || states[0] != "out_of_range" {
		t.Fatalf("stored quality states = %v, %v", states, result.Err())
	}

	readers := newParityReaders(client, bucket)
	device := observation.Resolved.DeviceID
	pond := scenario.LegacyPondByAsset[observation.Resolved.AssetID]
	for _, scope := range []string{"", device} {
		for _, aggregation := range parityAggregations {
			rule := alertevaluator.EvaluationRule{
				Rule: alertevaluator.Rule{TenantID: scenario.TenantID}, PondID: pond, DeviceID: scope, Metric: "do_mg_l",
			}
			spec := dissolvedOxygenSpec(at, scope, aggregation)
			legacy, v2 := evaluateBoth(t, readers, rule, spec)
			if legacy.Quality != alertevaluator.QualitySufficient || legacy.Value != observation.Observation.Value || legacy.ValidPointCount != 1 {
				t.Errorf("scope %q %s: legacy no longer shows the out-of-range value: %#v", scope, aggregation, legacy)
			}
			if v2.Quality != alertevaluator.QualityInsufficientData || v2.ValidPointCount != 0 || v2.Value != 0 || !v2.LatestSampleAt.IsZero() {
				t.Errorf("scope %q %s: v2 quality-gated read includes the out-of-range value: %#v", scope, aggregation, v2)
			}
			query, err := BuildV2Query(bucket.Name, rule, spec)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := readers.v2.Querier.Query(context.Background(), query)
			if err != nil || len(rows) != 0 {
				t.Errorf("scope %q: v2 query returned %#v, %v; want the quality filter to drop the point", scope, rows, err)
			}
		}
	}
}

func TestInfluxIntegrationV1RuleSeesNoNativeProbes(t *testing.T) {
	vectors := loadContractVectors(t)
	scenario := vectors.Scenarios["two_probes_same_metric"]
	if len(scenario.LegacyLines) != 0 || len(scenario.V2Lines) != 2 || len(scenario.Devices) != 1 {
		t.Fatalf("two_probes_same_metric scenario changed: %#v", scenario)
	}
	at := onlyObservationTime(t, scenario)
	client, bucket := newIntegrationBucket(t)
	seedScenario(t, client, bucket, scenario)
	readers := newParityReaders(client, bucket)
	pond := scenario.LegacyPondByAsset[scenario.Observations[0].Resolved.AssetID]
	for _, scope := range []string{"", scenario.Devices[0]} {
		rule := alertevaluator.EvaluationRule{
			Rule: alertevaluator.Rule{TenantID: scenario.TenantID}, PondID: pond, DeviceID: scope, Metric: "do_mg_l",
		}
		spec := dissolvedOxygenSpec(at, scope, alertevaluator.AggregationMean)
		// The native points match the query; only the default-Component
		// filter keeps them away from /v1 rules.
		query, err := BuildV2Query(bucket.Name, rule, spec)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := readers.v2.Querier.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		components := map[string]bool{}
		for _, row := range rows {
			components[row.ComponentID] = true
		}
		if len(rows) != 2 || !components[scenario.Components[0]] || !components[scenario.Components[1]] {
			t.Fatalf("scope %q: raw v2 rows = %#v, want both native probes", scope, rows)
		}
		for _, aggregation := range parityAggregations {
			spec.Aggregation = aggregation
			legacy, v2 := evaluateBoth(t, readers, rule, spec)
			if v2.ValidPointCount != 0 || v2.Quality != alertevaluator.QualityInsufficientData || !v2.LatestSampleAt.IsZero() {
				t.Errorf("scope %q %s: /v1 rule sees native probes: %#v", scope, aggregation, v2)
			}
			assertSameWindowResult(t, fmt.Sprintf("scope %q %s", scope, aggregation), legacy, v2)
		}
	}
}

func TestInfluxIntegrationParityExcludesNativeProbesBesideLegacyDevice(t *testing.T) {
	vectors := loadContractVectors(t)
	legacySample := vectors.Scenarios["legacy_sample"]
	native := vectors.Scenarios["two_probes_same_metric"]
	at := onlyObservationTime(t, legacySample)
	if !onlyObservationTime(t, native).Equal(at) {
		t.Fatal("native and legacy scenarios no longer share an instant")
	}
	client, bucket := newIntegrationBucket(t)
	seedScenario(t, client, bucket, legacySample)
	seedScenario(t, client, bucket, native)
	readers := newParityReaders(client, bucket)
	for _, aggregation := range parityAggregations {
		rule := alertevaluator.EvaluationRule{
			Rule: alertevaluator.Rule{TenantID: legacySample.TenantID}, PondID: "pond_local_001", Metric: "do_mg_l",
		}
		legacy, v2 := evaluateBoth(t, readers, rule, dissolvedOxygenSpec(at, "", aggregation))
		if legacy.ValidPointCount != 1 || legacy.ContributingDeviceCount != 1 || legacy.Quality != alertevaluator.QualitySufficient {
			t.Fatalf("%s: legacy = %#v, want only the legacy device", aggregation, legacy)
		}
		assertSameWindowResult(t, string(aggregation), legacy, v2)
	}
}

func TestInfluxIntegrationParityQueryErrorsMatchLegacy(t *testing.T) {
	vectors := loadContractVectors(t)
	scenario := vectors.Scenarios["legacy_sample"]
	at := onlyObservationTime(t, scenario)
	client, bucket := newIntegrationBucket(t)
	seedScenario(t, client, bucket, scenario)
	missing := *bucket
	missing.Name = bucket.Name + "_missing"
	readers := newParityReaders(client, &missing)
	rule := alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: scenario.TenantID}, PondID: "pond_local_001", Metric: "do_mg_l",
	}
	spec := dissolvedOxygenSpec(at, "", alertevaluator.AggregationMean)
	legacy, legacyErr := readers.legacy.Evaluate(context.Background(), rule, spec)
	v2, v2Err := readers.v2.Evaluate(context.Background(), rule, spec)
	if legacyErr == nil || v2Err == nil {
		t.Fatalf("errors = (%v, %v), want both readers to fail so the runner records query_error", legacyErr, v2Err)
	}
	if legacy != (alertevaluator.WindowResult{}) || v2 != (alertevaluator.WindowResult{}) {
		t.Fatalf("results on error = %#v, %#v; want zero values", legacy, v2)
	}
}

func TestInfluxIntegrationParityTwoDevicesInOnePond(t *testing.T) {
	// multi_point_series puts each device in its own pond. Move pond_south's
	// lines into pond_north (pond tag for legacy, asset tag for v2) so a pond
	// rule sums two devices per bucket; components stay the legacy defaults.
	vectors := loadContractVectors(t)
	scenario := vectors.Scenarios["multi_point_series"]
	southAsset, northAsset := AssetIDForLegacyPond(scenario.TenantID, "pond_south"), AssetIDForLegacyPond(scenario.TenantID, "pond_north")
	if scenario.LegacyPondByAsset[southAsset] != "pond_south" || scenario.LegacyPondByAsset[northAsset] != "pond_north" {
		t.Fatalf("multi_point_series ponds changed: %v", scenario.LegacyPondByAsset)
	}
	var lines []string
	for _, line := range scenario.LegacyLines {
		lines = append(lines, strings.Replace(line, ",pond_id=pond_south,", ",pond_id=pond_north,", 1))
	}
	for _, line := range scenario.V2Lines {
		lines = append(lines, strings.Replace(line, ",asset_id="+southAsset+",", ",asset_id="+northAsset+",", 1))
	}
	client, bucket := newIntegrationBucket(t)
	writeLines(t, client, bucket, lines...)
	readers := newParityReaders(client, bucket)

	first, last := scenarioSpan(t, scenario)
	start := first.Truncate(time.Minute)
	for _, metric := range []string{"temp_c", "do_mg_l"} {
		for _, aggregation := range parityAggregations {
			rule := alertevaluator.EvaluationRule{
				Rule: alertevaluator.Rule{TenantID: scenario.TenantID}, PondID: "pond_north", Metric: metric,
			}
			spec := alertevaluator.WindowSpec{
				Start: start, End: last.Truncate(time.Minute).Add(time.Minute), ExpectedSampleInterval: time.Minute,
				EvaluationCadence: time.Minute, MinimumCoverageRatio: .8, MinimumPoints: 3,
				MaximumSampleAge: 2 * time.Minute, Aggregation: aggregation,
			}
			legacy, v2 := evaluateBoth(t, readers, rule, spec)
			if legacy.ContributingDeviceCount != 2 || legacy.ValidPointCount != 60 || legacy.Quality != alertevaluator.QualitySufficient {
				t.Fatalf("%s %s: legacy = %#v, want two devices with 30 points each", metric, aggregation, legacy)
			}
			assertSameWindowResult(t, fmt.Sprintf("%s %s", metric, aggregation), legacy, v2)
		}
	}
}
