package alertevaluator

import (
	"math"
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
