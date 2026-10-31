package alertevaluator

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func qualitySpec(aggregation Aggregation) WindowSpec {
	end := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	return WindowSpec{
		Start:                  end.Add(-time.Minute),
		End:                    end,
		ExpectedSampleInterval: 10 * time.Second,
		EvaluationCadence:      time.Minute,
		MinimumCoverageRatio:   0.8,
		MinimumPoints:          3,
		MaximumSampleAge:       30 * time.Second,
		Aggregation:            aggregation,
	}
}

func TestEvaluateSamplesDeduplicatesByDeviceAndQualityBucket(t *testing.T) {
	spec := qualitySpec(AggregationMean)
	samples := []Sample{
		{Time: spec.Start.Add(time.Second), DeviceID: "dev_1", Value: 1, Seq: 1},
		{Time: spec.Start.Add(8 * time.Second), DeviceID: "dev_1", Value: 3, Seq: 2},
		{Time: spec.Start.Add(8 * time.Second), DeviceID: "dev_1", Value: 4, Seq: 3},
	}
	for offset := 10 * time.Second; offset < time.Minute; offset += 10 * time.Second {
		samples = append(samples, Sample{Time: spec.Start.Add(offset), DeviceID: "dev_1", Value: 4, Seq: int64(offset / time.Second)})
	}

	result := EvaluateSamples(spec, samples)

	if result.Quality != QualitySufficient || result.ValidPointCount != 6 || result.OccupiedBucketCount != 6 {
		t.Fatalf("result = %#v", result)
	}
	if result.Value != 4 {
		t.Fatalf("mean = %v, want 4", result.Value)
	}
}

func TestPondMeanWeightsDevicesEquallyInsideBucket(t *testing.T) {
	spec := qualitySpec(AggregationMean)
	var samples []Sample
	for bucket := time.Duration(0); bucket < time.Minute; bucket += 10 * time.Second {
		for i := 0; i < 5; i++ {
			samples = append(samples, Sample{Time: spec.Start.Add(bucket + time.Duration(i)*time.Second), DeviceID: "fast", Value: 10, Seq: int64(i)})
		}
		samples = append(samples, Sample{Time: spec.Start.Add(bucket + 9*time.Second), DeviceID: "slow", Value: 20, Seq: 1})
	}

	result := EvaluateSamples(spec, samples)

	if result.Quality != QualitySufficient || math.Abs(result.Value-15) > 0.0001 {
		t.Fatalf("pond mean = %#v", result)
	}
	if result.ContributingDeviceCount != 2 {
		t.Fatalf("devices = %d", result.ContributingDeviceCount)
	}
}

func TestCoverageCountsOccupiedTimeBucketsNotRawPoints(t *testing.T) {
	spec := qualitySpec(AggregationMax)
	var samples []Sample
	for i := 0; i < 50; i++ {
		samples = append(samples, Sample{Time: spec.Start.Add(time.Duration(i%2) * 10 * time.Second), DeviceID: "dev", Value: float64(i), Seq: int64(i)})
	}

	result := EvaluateSamples(spec, samples)

	if result.Quality != QualityInsufficientData || result.OccupiedBucketCount != 2 || result.ExpectedBucketCount != 6 {
		t.Fatalf("coverage = %#v", result)
	}
}

func TestLastDoesNotFallBackWhenLatestEvaluationSlotIsEmpty(t *testing.T) {
	spec := qualitySpec(AggregationLast)
	spec.Start = spec.End.Add(-2 * time.Minute)
	var samples []Sample
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		samples = append(samples, Sample{Time: spec.Start.Add(offset), DeviceID: "dev", Value: 7})
	}

	result := EvaluateSamples(spec, samples)

	if result.Quality != QualityInsufficientData {
		t.Fatalf("last result = %#v", result)
	}
}

func TestFreshnessIsIndependentFromCoverage(t *testing.T) {
	spec := qualitySpec(AggregationMin)
	spec.MaximumSampleAge = 15 * time.Second
	var samples []Sample
	for offset := time.Duration(0); offset < 50*time.Second; offset += 10 * time.Second {
		samples = append(samples, Sample{Time: spec.Start.Add(offset), DeviceID: "dev", Value: 2})
	}

	result := EvaluateSamples(spec, samples)

	if result.CoverageRatio < 0.8 || result.Quality != QualityStaleData {
		t.Fatalf("freshness result = %#v", result)
	}
}

func TestTargetDeviceFiltersOtherSeries(t *testing.T) {
	spec := qualitySpec(AggregationMax)
	spec.DeviceID = "target"
	var samples []Sample
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		samples = append(samples,
			Sample{Time: spec.Start.Add(offset), DeviceID: "target", Value: 3},
			Sample{Time: spec.Start.Add(offset), DeviceID: "other", Value: 100},
		)
	}

	result := EvaluateSamples(spec, samples)

	if result.Quality != QualitySufficient || result.Value != 3 || result.ContributingDeviceCount != 1 {
		t.Fatalf("target result = %#v", result)
	}
}

// orderFixtureDevices are listed in byte-wise ascending order (sort.Strings):
// uppercase before lowercase, "dev-10" before "dev-9". A case-insensitive or
// natural sort gives a different order.
var orderFixtureDevices = []string{"B", "Z", "a", "dev-10", "dev-9"}

// Device orders a wrong implementation could plausibly use; the fixture
// self-check proves each one changes the result bits.
var orderFixtureWrongDeviceOrders = [][]int{
	{4, 3, 2, 1, 0}, // descending
	{2, 0, 1, 3, 4}, // case-insensitive
	{0, 1, 2, 4, 3}, // natural (numeric-aware)
	{3, 0, 4, 2, 1}, // mean sample insertion order
	{4, 2, 0, 3, 1}, // last sample insertion order
	{1, 2, 0, 3, 4},
}

// orderFixtureValues[bucket][device] is chosen so that float addition order
// changes the result: any bucket or device order other than ascending gives
// different bits (asserted by the fixture self-check below).
var orderFixtureValues = [][]float64{
	{0.7, 0.2, -3e15, 0.3, -1e16},
	{0.1, 0.1, 4, 0.3, 4},
	{-3e15, 1e16, 1e15, 0.7, -3e15},
	{1.1, 4, 1e16, 3.399, 0.001},
	{0.3, 2.2, -3e15, 0.7, 1e15},
	{2.2, 0.7, 1e15, 0.001, 1},
}

// orderFixtureSamples gives every device one canonical sample per bucket for
// mean, plus a superseded earlier sample in the same bucket. All devices
// share the same timestamps, so time order cannot stand in for ID order.
func orderFixtureSamples(spec WindowSpec) []Sample {
	var samples []Sample
	for _, device := range []int{3, 0, 4, 2, 1} {
		for bucket, values := range orderFixtureValues {
			base := spec.Start.Add(time.Duration(bucket) * spec.ExpectedSampleInterval)
			samples = append(samples,
				Sample{Time: base.Add(time.Second), DeviceID: orderFixtureDevices[device], Value: 1e9, Seq: 1},
				Sample{Time: base.Add(5 * time.Second), DeviceID: orderFixtureDevices[device], Value: values[device], Seq: 2},
			)
		}
	}
	return samples
}

// orderedMean is the contract: buckets ascending, devices ascending inside a
// bucket, each bucket weighted by its device count.
func orderedMean(bucketOrder, deviceOrder []int) float64 {
	var temporal float64
	for _, bucket := range bucketOrder {
		var sum float64
		for _, device := range deviceOrder {
			sum += orderFixtureValues[bucket][device]
		}
		temporal += sum / float64(len(deviceOrder))
	}
	return temporal / float64(len(bucketOrder))
}

// orderFixtureLatest[device] is each device's latest value for last.
var orderFixtureLatest = []float64{2.2, 0.1, -3e15, 0.25, 1e15}

// orderedLast sums each device's latest value in the given device order.
func orderedLast(deviceOrder []int) float64 {
	var sum float64
	for _, device := range deviceOrder {
		sum += orderFixtureLatest[device]
	}
	return sum / float64(len(deviceOrder))
}

func orderFixtureLastSamples(spec WindowSpec) []Sample {
	var samples []Sample
	for _, device := range []int{4, 2, 0, 3, 1} {
		for bucket := 0; bucket < 6; bucket++ {
			value := 1e9
			if bucket == 5 {
				value = orderFixtureLatest[device]
			}
			samples = append(samples, Sample{
				Time:     spec.Start.Add(time.Duration(bucket)*spec.ExpectedSampleInterval + 5*time.Second),
				DeviceID: orderFixtureDevices[device], Value: value, Seq: int64(bucket),
			})
		}
	}
	return samples
}

func TestOrderFixtureIsOrderSensitive(t *testing.T) {
	if !sort.StringsAreSorted(orderFixtureDevices) {
		t.Fatalf("fixture devices are not in ascending order: %v", orderFixtureDevices)
	}
	ascBuckets := []int{0, 1, 2, 3, 4, 5}
	ascDevices := []int{0, 1, 2, 3, 4}
	want := orderedMean(ascBuckets, ascDevices)
	for _, buckets := range [][]int{{5, 4, 3, 2, 1, 0}, {0, 2, 1, 3, 4, 5}, {0, 1, 2, 3, 5, 4}, {2, 1, 0, 3, 4, 5}} {
		if math.Float64bits(orderedMean(buckets, ascDevices)) == math.Float64bits(want) {
			t.Fatalf("mean fixture is not sensitive to bucket order %v", buckets)
		}
	}
	for _, devices := range orderFixtureWrongDeviceOrders {
		if math.Float64bits(orderedMean(ascBuckets, devices)) == math.Float64bits(want) {
			t.Fatalf("mean fixture is not sensitive to device order %v", devices)
		}
		if math.Float64bits(orderedLast(devices)) == math.Float64bits(orderedLast(ascDevices)) {
			t.Fatalf("last fixture is not sensitive to device order %v", devices)
		}
	}
}

func assertBitIdentical(t *testing.T, label string, got WindowResult, want float64) {
	t.Helper()
	if got.Quality != QualitySufficient || math.Float64bits(got.Value) != math.Float64bits(want) {
		t.Fatalf("%s: value = %v (%#x), quality %s; want %v (%#x)", label, got.Value, math.Float64bits(got.Value), got.Quality, want, math.Float64bits(want))
	}
}

func shuffledCopies(samples []Sample, count int) [][]Sample {
	random := rand.New(rand.NewSource(20261018))
	copies := make([][]Sample, 0, count)
	for i := 0; i < count; i++ {
		shuffled := append([]Sample(nil), samples...)
		random.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		copies = append(copies, shuffled)
	}
	return copies
}

func TestMeanSumsBucketsAndDevicesInAscendingOrder(t *testing.T) {
	spec := qualitySpec(AggregationMean)
	samples := orderFixtureSamples(spec)
	want := orderedMean([]int{0, 1, 2, 3, 4, 5}, []int{0, 1, 2, 3, 4})
	for i := 0; i < 200; i++ {
		assertBitIdentical(t, "repeated mean", EvaluateSamples(spec, samples), want)
	}
	for i, shuffled := range shuffledCopies(samples, 50) {
		result := EvaluateSamples(spec, shuffled)
		assertBitIdentical(t, "shuffled mean", result, want)
		if result.ValidPointCount != 30 || result.OccupiedBucketCount != 6 || result.ContributingDeviceCount != 5 {
			t.Fatalf("shuffle %d: counts = %#v", i, result)
		}
	}
}

func TestLastSumsDevicesInAscendingOrder(t *testing.T) {
	spec := qualitySpec(AggregationLast)
	samples := orderFixtureLastSamples(spec)
	want := orderedLast([]int{0, 1, 2, 3, 4})
	for i := 0; i < 200; i++ {
		assertBitIdentical(t, "repeated last", EvaluateSamples(spec, samples), want)
	}
	for _, shuffled := range shuffledCopies(samples, 50) {
		assertBitIdentical(t, "shuffled last", EvaluateSamples(spec, shuffled), want)
	}
}

func TestDeterministicOrderKeepsOtherResultFields(t *testing.T) {
	spec := qualitySpec(AggregationMean)
	samples := orderFixtureSamples(spec)
	first := EvaluateSamples(spec, samples)
	wantLatest := spec.Start.Add(5*spec.ExpectedSampleInterval + 5*time.Second)
	if first.ExpectedBucketCount != 6 || first.CoverageRatio != 1 || !first.LatestSampleAt.Equal(wantLatest) {
		t.Fatalf("result = %#v", first)
	}
	for _, shuffled := range shuffledCopies(samples, 20) {
		if got := EvaluateSamples(spec, shuffled); got != first {
			t.Fatalf("shuffled result %#v differs from %#v", got, first)
		}
	}
	for _, aggregation := range []Aggregation{AggregationMin, AggregationMax} {
		spec := qualitySpec(aggregation)
		result := EvaluateSamples(spec, orderFixtureSamples(spec))
		want := -1e16
		if aggregation == AggregationMax {
			want = 1e16
		}
		if result.Quality != QualitySufficient || result.Value != want {
			t.Fatalf("%s = %#v, want %v", aggregation, result, want)
		}
	}
}

func fullWindow(spec WindowSpec, device string, value func(index int) float64) []Sample {
	var samples []Sample
	index := 0
	for offset := time.Duration(0); offset < spec.End.Sub(spec.Start); offset += spec.ExpectedSampleInterval {
		samples = append(samples, Sample{Time: spec.Start.Add(offset + time.Second), DeviceID: device, Value: value(index), Seq: int64(index)})
		index++
	}
	return samples
}

func TestSufficientZeroAggregateIsAPresentMeasurement(t *testing.T) {
	for _, aggregation := range []Aggregation{AggregationMin, AggregationMax, AggregationMean, AggregationLast} {
		spec := qualitySpec(aggregation)
		result := EvaluateSamples(spec, fullWindow(spec, "dev_1", func(int) float64 { return 0 }))

		value, ok := result.Measurement().Get()
		if result.Quality != QualitySufficient || !ok || value != 0 {
			t.Fatalf("%s: result = %#v, measurement = %v, want present 0", aggregation, result, result.Measurement())
		}
	}
}

func TestNonFiniteAggregateIsInvalidAndAbsent(t *testing.T) {
	// Each sample is finite, but their sum overflows to +Inf.
	spec := qualitySpec(AggregationMean)
	result := EvaluateSamples(spec, fullWindow(spec, "dev_1", func(int) float64 { return math.MaxFloat64 }))

	if result.Quality != QualityInsufficientData || result.Measurement().IsPresent() {
		t.Fatalf("overflowing mean = %#v, measurement = %v, want absent insufficient_data", result, result.Measurement())
	}

	last := qualitySpec(AggregationLast)
	samples := append(
		fullWindow(last, "dev_a", func(int) float64 { return -math.MaxFloat64 }),
		fullWindow(last, "dev_b", func(int) float64 { return -math.MaxFloat64 })...,
	)
	result = EvaluateSamples(last, samples)
	if result.Quality != QualityInsufficientData || result.Measurement().IsPresent() {
		t.Fatalf("overflowing last = %#v, measurement = %v, want absent insufficient_data", result, result.Measurement())
	}

	// One bucket overflows to +Inf and the next to -Inf, so the mean is NaN.
	// This relies on the fixed summation order pinned by
	// TestMeanSumsBucketsAndDevicesInAscendingOrder.
	nanSpec := qualitySpec(AggregationMean)
	var nanSamples []Sample
	for bucket := 0; bucket < 6; bucket++ {
		value := math.MaxFloat64
		if bucket%2 == 1 {
			value = -math.MaxFloat64
		}
		at := nanSpec.Start.Add(time.Duration(bucket)*nanSpec.ExpectedSampleInterval + time.Second)
		nanSamples = append(nanSamples,
			Sample{Time: at, DeviceID: "dev_a", Value: value},
			Sample{Time: at, DeviceID: "dev_b", Value: value})
	}
	result = EvaluateSamples(nanSpec, nanSamples)
	if result.Quality != QualityInsufficientData || result.Measurement().IsPresent() {
		t.Fatalf("NaN mean = %#v, measurement = %v, want absent insufficient_data", result, result.Measurement())
	}
}

func TestOutOfRangeAggregateIsInvalidBeforeStaleClassification(t *testing.T) {
	spec := qualitySpec(AggregationMax)
	spec.MinimumCoverageRatio = 0.4
	var samples []Sample
	for offset := time.Duration(0); offset < 30*time.Second; offset += spec.ExpectedSampleInterval {
		samples = append(samples, Sample{Time: spec.Start.Add(offset + time.Second), DeviceID: "dev_1", Value: 1e30})
	}

	result := EvaluateSamples(spec, samples)

	if result.Quality != QualityInsufficientData || result.Measurement().IsPresent() {
		t.Fatalf("stale out-of-range window = %#v, want insufficient_data", result)
	}
}

func TestOutOfRangeAggregateIsInvalidAndAbsent(t *testing.T) {
	for _, test := range []struct {
		aggregation Aggregation
		value       float64
	}{
		{AggregationMax, 1e30},
		{AggregationMin, -1e200},
		{AggregationMean, 1e-120},
		{AggregationLast, 5e-324},
	} {
		spec := qualitySpec(test.aggregation)
		result := EvaluateSamples(spec, fullWindow(spec, "dev_1", func(int) float64 { return test.value }))

		if result.Quality != QualityInsufficientData || result.Measurement().IsPresent() {
			t.Fatalf("%s of %v = %#v, want absent insufficient_data", test.aggregation, test.value, result)
		}
	}
}

func TestWindowMeasurementIsPresentOnlyForSufficientQuality(t *testing.T) {
	tests := []struct {
		result      WindowResult
		wantPresent bool
	}{
		{WindowResult{Quality: QualitySufficient, Value: 0}, true},
		{WindowResult{Quality: QualitySufficient, Value: -3.5}, true},
		{WindowResult{Quality: QualityStaleData, Value: 9.9}, false},
		{WindowResult{Quality: QualityInsufficientData}, false},
		{WindowResult{Quality: QualityQueryError, Value: 1}, false},
		{WindowResult{}, false},
		{WindowResult{Quality: QualitySufficient, Value: math.Inf(1)}, false},
		{WindowResult{Quality: QualitySufficient, Value: math.NaN()}, false},
		{WindowResult{Quality: QualitySufficient, Value: 1e200}, false},
	}
	for _, test := range tests {
		value, ok := test.result.Measurement().Get()
		if ok != test.wantPresent || (ok && value != test.result.Value) {
			t.Fatalf("Measurement(%#v) = (%v, %v), want present=%v", test.result, value, ok, test.wantPresent)
		}
	}
}

func TestStaleWindowKeepsItsRawAggregateButNoMeasurement(t *testing.T) {
	spec := qualitySpec(AggregationMin)
	spec.MaximumSampleAge = 30 * time.Second
	var samples []Sample
	for offset := time.Duration(0); offset < 30*time.Second; offset += spec.ExpectedSampleInterval {
		samples = append(samples, Sample{Time: spec.Start.Add(offset + time.Second), DeviceID: "dev_1", Value: 9.9})
	}
	spec.MinimumCoverageRatio = 0.4

	result := EvaluateSamples(spec, samples)

	if result.Quality != QualityStaleData || result.Value != 9.9 || result.Measurement().IsPresent() {
		t.Fatalf("stale result = %#v, measurement = %v", result, result.Measurement())
	}
}
