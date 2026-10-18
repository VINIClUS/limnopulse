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
// uppercase before lowercase, "dev-10" before "dev-9".
var orderFixtureDevices = []string{"B", "a", "dev-10", "dev-9", "z"}

// orderFixtureValues[bucket][device] is chosen so that float addition order
// changes the result: any bucket or device order other than ascending gives
// different bits (asserted by the fixture self-check below).
var orderFixtureValues = [][]float64{
	{0.2, -1e16, -1e16, 1e16, 1},
	{1e16, 1.1, 6.1, 7.3, 1.1},
	{6.1, -3e15, 1e15, 0.1, 1e15},
	{4, 0.7, 1e15, 0.001, 0.1},
	{1e16, 0.1, 0.3, -1e16, 3.399},
	{7.3, 1e16, 0.3, 0.1, -1e16},
}

// orderFixtureSamples gives every device one canonical sample per bucket for
// mean, plus a superseded earlier sample in the same bucket.
func orderFixtureSamples(spec WindowSpec) []Sample {
	var samples []Sample
	for _, device := range []int{3, 0, 4, 2, 1} {
		for bucket, values := range orderFixtureValues {
			base := spec.Start.Add(time.Duration(bucket) * spec.ExpectedSampleInterval)
			samples = append(samples,
				Sample{Time: base.Add(time.Second), DeviceID: orderFixtureDevices[device], Value: 1e9, Seq: 1},
				Sample{Time: base.Add(time.Duration(2+device) * time.Second), DeviceID: orderFixtureDevices[device], Value: values[device], Seq: 2},
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

// orderedLast sums each device's latest value in ascending device order.
func orderedLast(deviceOrder []int) float64 {
	latest := orderFixtureLatest
	var sum float64
	for _, device := range deviceOrder {
		sum += latest[device]
	}
	return sum / float64(len(deviceOrder))
}

// orderFixtureLatest[device] is each device's latest value for last.
var orderFixtureLatest = []float64{0.1, 0.2, 0.3, 1e16, -1e16}

func orderFixtureLastSamples(spec WindowSpec) []Sample {
	latest := orderFixtureLatest
	var samples []Sample
	for _, device := range []int{4, 2, 0, 3, 1} {
		for bucket := 0; bucket < 6; bucket++ {
			value := 1e9
			if bucket == 5 {
				value = latest[device]
			}
			samples = append(samples, Sample{
				Time:     spec.Start.Add(time.Duration(bucket)*spec.ExpectedSampleInterval + time.Duration(device+1)*time.Second),
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
	ascBuckets, descBuckets := []int{0, 1, 2, 3, 4, 5}, []int{5, 4, 3, 2, 1, 0}
	ascDevices, descDevices := []int{0, 1, 2, 3, 4}, []int{4, 3, 2, 1, 0}
	want := orderedMean(ascBuckets, ascDevices)
	for _, alternative := range []float64{
		orderedMean(descBuckets, ascDevices), orderedMean(ascBuckets, descDevices), orderedMean(descBuckets, descDevices),
		orderedMean(ascBuckets, []int{3, 0, 4, 2, 1}), // sample insertion order
	} {
		if math.Float64bits(alternative) == math.Float64bits(want) {
			t.Fatalf("mean fixture is not order-sensitive: %v == %v", alternative, want)
		}
	}
	for _, alternative := range [][]int{descDevices, {4, 2, 0, 3, 1}} {
		if math.Float64bits(orderedLast(ascDevices)) == math.Float64bits(orderedLast(alternative)) {
			t.Fatalf("last fixture is not order-sensitive for %v", alternative)
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
	wantLatest := spec.Start.Add(5*spec.ExpectedSampleInterval + 6*time.Second)
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
