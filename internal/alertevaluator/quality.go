package alertevaluator

import (
	"math"
	"time"
)

type Aggregation string

const (
	AggregationMin  Aggregation = "min"
	AggregationMax  Aggregation = "max"
	AggregationMean Aggregation = "mean"
	AggregationLast Aggregation = "last"
)

type Sample struct {
	Time     time.Time
	DeviceID string
	Value    float64
	Seq      int64
}

type WindowSpec struct {
	Start                  time.Time
	End                    time.Time
	DeviceID               string
	ExpectedSampleInterval time.Duration
	EvaluationCadence      time.Duration
	MinimumCoverageRatio   float64
	MinimumPoints          int
	MaximumSampleAge       time.Duration
	Aggregation            Aggregation
}

type WindowResult struct {
	Quality                 Quality
	Value                   float64
	CoverageRatio           float64
	ExpectedBucketCount     int
	OccupiedBucketCount     int
	ValidPointCount         int
	ContributingDeviceCount int
	LatestSampleAt          time.Time
}

type sampleKey struct {
	device string
	bucket int
}

func EvaluateSamples(spec WindowSpec, samples []Sample) WindowResult {
	expected := bucketCount(spec.End.Sub(spec.Start), spec.ExpectedSampleInterval)
	result := WindowResult{Quality: QualityInsufficientData, ExpectedBucketCount: expected}
	if expected == 0 || spec.ExpectedSampleInterval <= 0 || !spec.End.After(spec.Start) {
		return result
	}

	canonical := make(map[sampleKey]Sample)
	for _, sample := range samples {
		if sample.Time.Before(spec.Start) || !sample.Time.Before(spec.End) {
			continue
		}
		if spec.DeviceID != "" && sample.DeviceID != spec.DeviceID {
			continue
		}
		if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
			continue
		}
		bucket := int(sample.Time.Sub(spec.Start) / spec.ExpectedSampleInterval)
		key := sampleKey{device: sample.DeviceID, bucket: bucket}
		previous, exists := canonical[key]
		if !exists || sample.Time.After(previous.Time) ||
			(sample.Time.Equal(previous.Time) && sample.Seq > previous.Seq) {
			canonical[key] = sample
		}
	}

	occupied := make(map[int]struct{})
	devices := make(map[string]struct{})
	values := make([]Sample, 0, len(canonical))
	for key, sample := range canonical {
		occupied[key.bucket] = struct{}{}
		devices[key.device] = struct{}{}
		values = append(values, sample)
		if result.LatestSampleAt.IsZero() || sample.Time.After(result.LatestSampleAt) {
			result.LatestSampleAt = sample.Time
		}
	}
	result.ValidPointCount = len(canonical)
	result.OccupiedBucketCount = len(occupied)
	result.ContributingDeviceCount = len(devices)
	result.CoverageRatio = math.Min(1, float64(len(occupied))/float64(expected))

	if result.CoverageRatio < spec.MinimumCoverageRatio {
		return result
	}
	if spec.Aggregation != AggregationLast && len(occupied) < spec.MinimumPoints {
		return result
	}
	value, ok := aggregateSamples(spec, canonical, values)
	if !ok {
		return result
	}
	result.Value = value
	if spec.MaximumSampleAge > 0 && spec.End.Sub(result.LatestSampleAt) > spec.MaximumSampleAge {
		result.Quality = QualityStaleData
		return result
	}
	result.Quality = QualitySufficient
	return result
}

func aggregateSamples(spec WindowSpec, canonical map[sampleKey]Sample, values []Sample) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	switch spec.Aggregation {
	case AggregationMin:
		value := values[0].Value
		for _, sample := range values[1:] {
			value = math.Min(value, sample.Value)
		}
		return value, true
	case AggregationMax:
		value := values[0].Value
		for _, sample := range values[1:] {
			value = math.Max(value, sample.Value)
		}
		return value, true
	case AggregationMean:
		bucketSums := make(map[int]float64)
		bucketDevices := make(map[int]int)
		for key, sample := range canonical {
			bucketSums[key.bucket] += sample.Value
			bucketDevices[key.bucket]++
		}
		var temporalSum float64
		for bucket, sum := range bucketSums {
			temporalSum += sum / float64(bucketDevices[bucket])
		}
		return temporalSum / float64(len(bucketSums)), true
	case AggregationLast:
		latestSlotStart := spec.End.Add(-spec.EvaluationCadence)
		latestByDevice := make(map[string]Sample)
		for _, sample := range values {
			if sample.Time.Before(latestSlotStart) {
				continue
			}
			previous, exists := latestByDevice[sample.DeviceID]
			if !exists || sample.Time.After(previous.Time) ||
				(sample.Time.Equal(previous.Time) && sample.Seq > previous.Seq) {
				latestByDevice[sample.DeviceID] = sample
			}
		}
		if len(latestByDevice) == 0 {
			return 0, false
		}
		var sum float64
		for _, sample := range latestByDevice {
			sum += sample.Value
		}
		return sum / float64(len(latestByDevice)), true
	default:
		return 0, false
	}
}

func bucketCount(window, interval time.Duration) int {
	if window <= 0 || interval <= 0 {
		return 0
	}
	return int((window + interval - 1) / interval)
}
