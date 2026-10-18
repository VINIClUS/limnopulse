package influx

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

const (
	vectorTenant       = "tnt_local_001"
	vectorPond         = "pond_local_001"
	vectorDevice       = "local-device-001"
	vectorPondAsset    = "ast_751707aef3455ab2ba60733d238b594f"
	vectorDeviceDefCmp = "cmp_32c73f5ac8dc596e9446956875639204"
	nativeComponentA   = "cmp_a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	nativeComponentB   = "cmp_a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2"
)

type fakeV2Querier struct {
	calls int
	query string
	rows  []V2Row
	err   error
}

func (querier *fakeV2Querier) Query(_ context.Context, flux string) ([]V2Row, error) {
	querier.calls++
	querier.query = flux
	return querier.rows, querier.err
}

var _ V2Querier = (*fakeV2Querier)(nil)
var _ alertevaluator.WindowReader = V2WindowReader{}

func vectorRule(deviceID, metric string) alertevaluator.EvaluationRule {
	return alertevaluator.EvaluationRule{
		Rule:   alertevaluator.Rule{TenantID: vectorTenant},
		PondID: vectorPond, DeviceID: deviceID, Metric: metric,
	}
}

func queryLines(query string) []string {
	var lines []string
	for _, line := range strings.Split(query, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func assertQueryLines(t *testing.T, query string, want []string) {
	t.Helper()
	got := queryLines(query)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("query lines differ\n got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestBuildV2QueryPondWideMatchesContract(t *testing.T) {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	query, err := BuildV2Query("limnopulse_raw", vectorRule("", "do_mg_l"), alertevaluator.WindowSpec{Start: start, End: start.Add(5 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	assertQueryLines(t, query, []string{
		`from(bucket: "limnopulse_raw")`,
		`|> range(start: time(v: "2026-07-09T11:55:00Z"), stop: time(v: "2026-07-09T12:00:00Z"))`,
		`|> filter(fn: (r) => r["_measurement"] == "observation_numeric")`,
		`|> filter(fn: (r) => r["tenant_id"] == "tnt_local_001")`,
		`|> filter(fn: (r) => r["asset_id"] == "` + vectorPondAsset + `")`,
		`|> filter(fn: (r) => r["metric_key"] == "water.dissolved_oxygen")`,
		`|> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "sequence" or r["_field"] == "quality_state")`,
		`|> pivot(rowKey: ["_time", "device_id", "component_id"], columnKey: ["_field"], valueColumn: "_value")`,
		`|> filter(fn: (r) => r["quality_state"] == "valid")`,
		`|> keep(columns: ["_time", "device_id", "component_id", "value", "sequence"])`,
		`|> group()`,
		`|> sort(columns: ["_time", "device_id", "component_id"])`,
	})
}

func TestBuildV2QueryDeviceScopedAddsDeviceFilterAfterAsset(t *testing.T) {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	query, err := BuildV2Query("limnopulse_raw", vectorRule(vectorDevice, "rssi"), alertevaluator.WindowSpec{Start: start, End: start.Add(5 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	assertQueryLines(t, query, []string{
		`from(bucket: "limnopulse_raw")`,
		`|> range(start: time(v: "2026-07-09T11:55:00Z"), stop: time(v: "2026-07-09T12:00:00Z"))`,
		`|> filter(fn: (r) => r["_measurement"] == "observation_numeric")`,
		`|> filter(fn: (r) => r["tenant_id"] == "tnt_local_001")`,
		`|> filter(fn: (r) => r["asset_id"] == "` + vectorPondAsset + `")`,
		`|> filter(fn: (r) => r["device_id"] == "local-device-001")`,
		`|> filter(fn: (r) => r["metric_key"] == "device.rssi")`,
		`|> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "sequence" or r["_field"] == "quality_state")`,
		`|> pivot(rowKey: ["_time", "device_id", "component_id"], columnKey: ["_field"], valueColumn: "_value")`,
		`|> filter(fn: (r) => r["quality_state"] == "valid")`,
		`|> keep(columns: ["_time", "device_id", "component_id", "value", "sequence"])`,
		`|> group()`,
		`|> sort(columns: ["_time", "device_id", "component_id"])`,
	})
}

func TestBuildV2QueryMapsEveryLegacyMetricToItsCatalogKey(t *testing.T) {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	for _, row := range loadContractVectors(t).MetricCatalog {
		if row.LegacyField == nil {
			continue
		}
		query, err := BuildV2Query("raw", vectorRule("", *row.LegacyField), alertevaluator.WindowSpec{Start: start, End: start.Add(time.Minute)})
		if err != nil {
			t.Fatalf("BuildV2Query(%q): %v", *row.LegacyField, err)
		}
		want := `|> filter(fn: (r) => r["metric_key"] == "` + row.MetricKey + `")`
		if !strings.Contains(query, want) {
			t.Fatalf("query for %q misses %q:\n%s", *row.LegacyField, want, query)
		}
		if strings.Contains(query, `"`+*row.LegacyField+`"`) {
			t.Fatalf("query for %q still filters on the legacy field name:\n%s", *row.LegacyField, query)
		}
	}
}

func TestBuildV2QueryUsesTheLegacyReaderWindowBounds(t *testing.T) {
	// Parity needs the exact legacy range, sub-second bounds and non-UTC
	// inputs included.
	zone := time.FixedZone("BRT", -3*60*60)
	start := time.Date(2026, 7, 9, 8, 59, 59, 750_000_000, zone)
	spec := alertevaluator.WindowSpec{Start: start, End: start.Add(90*time.Second + 250*time.Millisecond)}
	legacy, err := BuildQuery("raw", vectorRule("", "ph"), spec)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := BuildV2Query("raw", vectorRule("", "ph"), spec)
	if err != nil {
		t.Fatal(err)
	}
	legacyRange, v2Range := queryLines(legacy)[1], queryLines(v2)[1]
	if legacyRange != v2Range {
		t.Fatalf("v2 range %q differs from legacy range %q", v2Range, legacyRange)
	}
	if want := `|> range(start: time(v: "2026-07-09T11:59:59.75Z"), stop: time(v: "2026-07-09T12:01:30Z"))`; v2Range != want {
		t.Fatalf("v2 range = %q, want %q", v2Range, want)
	}
}

func TestBuildV2QueryEscapesInterpolationButKeepsLoneDollar(t *testing.T) {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	spec := alertevaluator.WindowSpec{Start: start, End: start.Add(time.Minute)}
	tenant := "t${x}\"q${y}"
	pond := "p${w}"
	device := "a$b$${c}\n"
	rule := alertevaluator.EvaluationRule{
		Rule:   alertevaluator.Rule{TenantID: tenant},
		PondID: pond, DeviceID: device, Metric: "ph",
	}
	query, err := BuildV2Query("raw$archive", rule, spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`from(bucket: "raw$archive")`,
		`r["tenant_id"] == "t\${x}\"q\${y}")`,
		`r["asset_id"] == "` + AssetIDForLegacyPond(tenant, pond) + `")`,
		`r["device_id"] == "a$b$\${c}\n")`,
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query misses %q:\n%s", want, query)
		}
	}
	if strings.Contains(strings.ReplaceAll(query, `\${`, ""), "${") {
		t.Fatalf("query keeps an unescaped interpolation:\n%s", query)
	}
	if strings.Contains(query, "p$") || strings.Contains(query, "pond_id") {
		t.Fatalf("query leaks the legacy pond instead of its asset:\n%s", query)
	}

	query, err = BuildV2Query("b${z}", vectorRule("", "ph"), spec)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, `from(bucket: "b\${z}")`) {
		t.Fatalf("bucket interpolation is not escaped:\n%s", query)
	}
}

func TestBuildV2QueryRejectsUnknownMetricAndInvalidWindow(t *testing.T) {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		metric string
		end    time.Time
	}{
		{name: "v2 metric key", metric: "water.temperature", end: start.Add(time.Minute)},
		{name: "sequence field", metric: "seq", end: start.Add(time.Minute)},
		{name: "injection", metric: `ph") |> yield()`, end: start.Add(time.Minute)},
		{name: "empty", metric: "", end: start.Add(time.Minute)},
		{name: "empty window", metric: "ph", end: start},
		{name: "inverted window", metric: "ph", end: start.Add(-time.Second)},
	} {
		t.Run(test.name, func(t *testing.T) {
			query, err := BuildV2Query("raw", vectorRule("", test.metric), alertevaluator.WindowSpec{Start: start, End: test.end})
			if err == nil || query != "" {
				t.Fatalf("BuildV2Query = (%q, %v), want an error", query, err)
			}
		})
	}
}

func TestLegacyBuildQueryIsUnchanged(t *testing.T) {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	query, err := BuildQuery("raw$archive", vectorRule(vectorDevice, "do_mg_l"), alertevaluator.WindowSpec{Start: start, End: start.Add(5 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	want := `from(bucket: "raw$archive")
  |> range(start: time(v: "2026-07-09T11:55:00Z"), stop: time(v: "2026-07-09T12:00:00Z"))
  |> filter(fn: (r) => r["_measurement"] == "water_quality")
  |> filter(fn: (r) => r["tenant_id"] == "tnt_local_001")
  |> filter(fn: (r) => r["pond_id"] == "pond_local_001")
  |> filter(fn: (r) => r["device_id"] == "local-device-001")
  |> filter(fn: (r) => r["_field"] == "do_mg_l" or r["_field"] == "seq")
  |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "device_id", "do_mg_l", "seq"])
  |> sort(columns: ["_time", "device_id"])`
	if query != want {
		t.Fatalf("legacy query changed:\n%s", query)
	}

	// The v2-only `${` escape must not leak into the legacy builder.
	query, err = BuildQuery("b${z}", alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: "t${x}${y}"}, PondID: "p${w}", DeviceID: "d${v}", Metric: "ph",
	}, alertevaluator.WindowSpec{Start: start, End: start.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`from(bucket: "b${z}")`, `r["tenant_id"] == "t${x}${y}")`, `r["pond_id"] == "p${w}")`, `r["device_id"] == "d${v}")`} {
		if !strings.Contains(query, want) {
			t.Fatalf("legacy query misses %q (as on main):\n%s", want, query)
		}
	}
	if strings.Contains(query, `\$`) {
		t.Fatalf("legacy query gained a v2 escape:\n%s", query)
	}
}

func readerSpec(end time.Time, aggregation alertevaluator.Aggregation) alertevaluator.WindowSpec {
	return alertevaluator.WindowSpec{
		Start: end.Add(-time.Minute), End: end, ExpectedSampleInterval: 10 * time.Second,
		EvaluationCadence: time.Minute, MinimumCoverageRatio: .8, MinimumPoints: 3,
		MaximumSampleAge: 30 * time.Second, Aggregation: aggregation,
	}
}

func TestV2WindowReaderSendsBuiltQueryAndEvaluatesDefaultComponentRows(t *testing.T) {
	end := time.Date(2026, 7, 9, 12, 1, 0, 0, time.UTC)
	spec := readerSpec(end, alertevaluator.AggregationMean)
	rule := vectorRule("", "ph")
	querier := &fakeV2Querier{}
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		querier.rows = append(querier.rows, V2Row{
			Time: spec.Start.Add(offset), DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp,
			Value: 7.25, Sequence: int64(offset / time.Second),
		})
	}
	result, err := V2WindowReader{Bucket: "raw$archive", Querier: querier}.Evaluate(context.Background(), rule, spec)
	if err != nil {
		t.Fatal(err)
	}
	want, err := BuildV2Query("raw$archive", rule, spec)
	if err != nil {
		t.Fatal(err)
	}
	if querier.calls != 1 || querier.query != want {
		t.Fatalf("querier got %d calls with query:\n%s\nwant:\n%s", querier.calls, querier.query, want)
	}
	if result.Quality != alertevaluator.QualitySufficient || result.Value != 7.25 || result.ValidPointCount != 6 ||
		result.ContributingDeviceCount != 1 || !result.LatestSampleAt.Equal(spec.Start.Add(50*time.Second)) {
		t.Fatalf("result = %#v", result)
	}
}

func TestV2WindowReaderDropsRowsOutsideLegacyDefaultComponents(t *testing.T) {
	end := time.Date(2026, 7, 9, 12, 1, 0, 0, time.UTC)
	spec := readerSpec(end, alertevaluator.AggregationMean)
	tenant := "tnt_1"
	rule := alertevaluator.EvaluationRule{Rule: alertevaluator.Rule{TenantID: tenant}, PondID: "pond_1", Metric: "ph"}
	defaultA := DefaultComponentIDForLegacyDevice(tenant, "dev-a")
	intruders := []struct {
		device    string
		component string
	}{
		{device: "dev-a", component: nativeComponentA},
		{device: "dev-a", component: DefaultComponentIDForLegacyDevice("tnt_2", "dev-a")},
		{device: "dev-a", component: DefaultComponentIDForLegacyDevice(tenant, "dev-b")},
		{device: "dev-b", component: defaultA},
		{device: "dev_22222222222222222222222222222222", component: nativeComponentB},
		{device: "dev-a", component: ""},
	}
	querier := &fakeV2Querier{}
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		querier.rows = append(querier.rows, V2Row{
			Time: spec.Start.Add(offset), DeviceID: "dev-a", ComponentID: defaultA, Value: 4, Sequence: 1,
		})
		for index, intruder := range intruders {
			// Later in the bucket and with a higher sequence, so a kept
			// intruder would win the bucket or add a device.
			querier.rows = append(querier.rows, V2Row{
				Time:     spec.Start.Add(offset + time.Duration(index+1)*time.Second),
				DeviceID: intruder.device, ComponentID: intruder.component, Value: 100, Sequence: 1000,
			})
		}
	}
	result, err := V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), rule, spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Quality != alertevaluator.QualitySufficient || result.Value != 4 || result.ValidPointCount != 6 ||
		result.ContributingDeviceCount != 1 || !result.LatestSampleAt.Equal(spec.Start.Add(50*time.Second)) {
		t.Fatalf("non-default components leaked into the result: %#v", result)
	}

	querier.rows = querier.rows[:0]
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		querier.rows = append(querier.rows, V2Row{Time: spec.Start.Add(offset), DeviceID: "dev-a", ComponentID: nativeComponentA, Value: 4})
	}
	result, err = V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), rule, spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Quality != alertevaluator.QualityInsufficientData || result.ValidPointCount != 0 || !result.LatestSampleAt.IsZero() {
		t.Fatalf("native-only rows produced samples: %#v", result)
	}
}

func singleBucketSpec(start time.Time) alertevaluator.WindowSpec {
	return alertevaluator.WindowSpec{
		Start: start, End: start.Add(10 * time.Second), ExpectedSampleInterval: 10 * time.Second,
		EvaluationCadence: 10 * time.Second, MinimumCoverageRatio: 1, MinimumPoints: 1,
		MaximumSampleAge: time.Minute, Aggregation: alertevaluator.AggregationLast,
	}
}

func TestV2WindowReaderMapsAbsentSequenceSentinelToZero(t *testing.T) {
	start := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	at := start.Add(2 * time.Second)
	for _, test := range []struct {
		name          string
		first, second int64
		want          float64
	}{
		// -1 must become 0: the tie with the second row keeps the first one,
		// exactly as two legacy rows without seq would.
		{name: "sentinel ties with zero", first: -1, second: 0, want: 1},
		// Real sequences still break ties.
		{name: "higher sequence wins", first: 3, second: 5, want: 2},
		{name: "sentinel loses to a real sequence", first: -1, second: 1, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			querier := &fakeV2Querier{rows: []V2Row{
				{Time: at, DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: 1, Sequence: test.first},
				{Time: at, DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: 2, Sequence: test.second},
			}}
			result, err := V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), vectorRule("", "ph"), singleBucketSpec(start))
			if err != nil {
				t.Fatal(err)
			}
			if result.Quality != alertevaluator.QualitySufficient || result.Value != test.want || result.ValidPointCount != 1 {
				t.Fatalf("result = %#v, want value %v", result, test.want)
			}
		})
	}
}

func TestV2WindowReaderNormalizesSampleTimesToUTC(t *testing.T) {
	start := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	local := start.Add(3 * time.Second).In(time.FixedZone("BRT", -3*60*60))
	querier := &fakeV2Querier{rows: []V2Row{{Time: local, DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: 3, Sequence: 1}}}
	result, err := V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), vectorRule("", "ph"), singleBucketSpec(start))
	if err != nil {
		t.Fatal(err)
	}
	if !result.LatestSampleAt.Equal(local) || result.LatestSampleAt.Location() != time.UTC {
		t.Fatalf("latest sample at %v (%v), want %v in UTC like the legacy reader", result.LatestSampleAt, result.LatestSampleAt.Location(), local.UTC())
	}
}

func TestV2WindowReaderDropsNonFiniteValues(t *testing.T) {
	start := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	querier := &fakeV2Querier{rows: []V2Row{
		{Time: start.Add(time.Second), DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: 3, Sequence: 1},
		{Time: start.Add(2 * time.Second), DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: math.NaN(), Sequence: 2},
		{Time: start.Add(3 * time.Second), DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: math.Inf(1), Sequence: 3},
	}}
	result, err := V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), vectorRule("", "ph"), singleBucketSpec(start))
	if err != nil {
		t.Fatal(err)
	}
	if result.Quality != alertevaluator.QualitySufficient || result.Value != 3 || !result.LatestSampleAt.Equal(start.Add(time.Second)) {
		t.Fatalf("result = %#v", result)
	}

	querier.rows = []V2Row{{Time: start.Add(time.Second), DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: math.NaN()}}
	result, err = V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), vectorRule("", "ph"), singleBucketSpec(start))
	if err != nil {
		t.Fatal(err)
	}
	if result.ValidPointCount != 0 || result.Quality != alertevaluator.QualityInsufficientData {
		t.Fatalf("NaN produced a sample: %#v", result)
	}
}

func TestV2WindowReaderPropagatesQueryFailure(t *testing.T) {
	sentinel := errors.New("influx unavailable")
	querier := &fakeV2Querier{err: sentinel, rows: []V2Row{{Time: time.Now(), DeviceID: vectorDevice, ComponentID: vectorDeviceDefCmp, Value: 1}}}
	start := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	result, err := V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), vectorRule("", "ph"), singleBucketSpec(start))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap %v", err, sentinel)
	}
	if result != (alertevaluator.WindowResult{}) {
		t.Fatalf("result = %#v, want the zero result on error", result)
	}
}

func TestV2WindowReaderRejectsInvalidRuleWithoutQuerying(t *testing.T) {
	querier := &fakeV2Querier{}
	start := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	_, err := V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), vectorRule("", "water.ph"), singleBucketSpec(start))
	if err == nil || querier.calls != 0 {
		t.Fatalf("err = %v, calls = %d; want an error before any query", err, querier.calls)
	}
}
