package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecorderEmitsLiveNoPIIWorkerMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	recorder, err := newRecorder(reader)
	if err != nil {
		t.Fatal(err)
	}
	recorder.ConfiguredRate(3.5)
	recorder.LimiterWaitStarted()
	recorder.LimiterWaitFinished(5 * time.Millisecond)
	recorder.ProviderCallStarted(true)
	recorder.ProviderCallFinished()
	recorder.Retry(worker.ErrorRetryableThrottling, true)
	recorder.Retry(worker.ErrorRetryableQuota, false)
	recorder.SucceededAfterRetry()
	recorder.Unknown()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, scope := range collected.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			names[measurement.Name] = true
		}
	}
	for _, name := range []string{
		"notification_worker_attempts_total", "notification_worker_ambiguous_total",
		"notification_worker_retries_total", "notification_worker_possible_duplicates_total",
		"notification_worker_unknown_total", "notification_worker_succeeded_after_retry_total",
		"notification_worker_limiter_wait_seconds", "notification_worker_limiter_waiters",
		"notification_worker_send_started_total", "notification_worker_throttling_total",
		"notification_worker_quota_total", "notification_worker_configured_rate",
		"notification_worker_active_concurrency",
	} {
		if !names[name] {
			t.Errorf("metric %s not collected: %#v", name, names)
		}
	}
}

func TestEmptyEndpointDisablesWorkerOTLP(t *testing.T) {
	recorder, err := New(context.Background(), "")
	if err != nil || recorder != nil {
		t.Fatalf("recorder=%#v err=%v", recorder, err)
	}
}
