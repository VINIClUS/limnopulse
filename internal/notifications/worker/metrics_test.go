package worker

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestTokenLimiterIsContextAware(t *testing.T) {
	limiter, err := NewTokenLimiter(0.01, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := limiter.Wait(ctx); err == nil {
		t.Fatal("cancelled limiter wait error = nil")
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("limiter ignored context cancellation")
	}
}

func TestTokenLimiterRejectsNonFiniteRates(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, -1} {
		if _, err := NewTokenLimiter(value, 1); err == nil {
			t.Fatalf("rate %v accepted", value)
		}
	}
}

func TestMetricsSnapshotContainsOnlyAggregateNoPIIValues(t *testing.T) {
	metrics := NewMetrics(12.5)
	waitDone := metrics.BeginLimiterWait()
	waitDone()
	metrics.ProviderCallStarted(true)
	metrics.ProviderCallFinished()
	metrics.RecordRetry(ErrorRetryableThrottling, true)
	metrics.RecordSucceeded(true)
	metrics.RecordUnknown()
	snapshot := metrics.Snapshot()
	if snapshot.ConfiguredRate != 12.5 || snapshot.Attempts != 1 || snapshot.SendStarted != 1 ||
		snapshot.PossibleDuplicates != 1 || snapshot.Retries != 1 || snapshot.Ambiguous != 1 ||
		snapshot.Throttling != 1 || snapshot.SucceededAfterRetry != 1 || snapshot.Unknown != 1 ||
		snapshot.ActiveConcurrency != 0 || snapshot.LimiterWaiters != 0 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestMetricsCountsTelegramRateLimitAsThrottling(t *testing.T) {
	metrics := NewMetrics(25)
	metrics.RecordRetry(ErrorTelegramRateLimited, false)
	if snapshot := metrics.Snapshot(); snapshot.Retries != 1 || snapshot.Throttling != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}
