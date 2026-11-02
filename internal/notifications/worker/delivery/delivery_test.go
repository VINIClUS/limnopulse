package delivery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
)

type failingQueue struct{ receives int }

func (queue *failingQueue) Receive(context.Context, int, time.Duration, time.Duration) ([]worker.QueueMessage, error) {
	queue.receives++
	return nil, errors.New("queue unavailable")
}
func (*failingQueue) Delete(context.Context, worker.QueueMessage) error { return nil }
func (*failingQueue) ChangeVisibility(context.Context, worker.QueueMessage, time.Duration) error {
	return nil
}

func consumer(queue worker.Queue) Consumer {
	return Consumer{
		OwnerPrefix: "test_", Queue: queue, Rate: 2.5,
		Concurrency: 1, ReceiveBatch: 10, ReceiveWait: time.Second, Visibility: time.Minute,
		ProcessingLease: time.Minute, ProviderTimeout: time.Second, InvalidVisibility: time.Minute,
		RenewalInterval: 20 * time.Second, DrainTimeout: time.Second, OTLPFlushTimeout: 200 * time.Millisecond,
	}
}

func TestRunStopsOnAQueueFailureAndKeepsTheSendMetrics(t *testing.T) {
	queue := &failingQueue{}
	summary := consumer(queue).Run(context.Background(), nil)
	if queue.receives != 1 || summary.ExitCode != 1 || summary.ErrorCategories["queue_receive_failure"] != 1 {
		t.Fatalf("receives=%d summary=%#v", queue.receives, summary)
	}
	if summary.Metrics.ConfiguredRate != 2.5 || summary.TelemetryExportError != "" {
		t.Fatalf("metrics=%#v telemetry=%q", summary.Metrics, summary.TelemetryExportError)
	}
}

func TestRunReportsATelemetryExportFailure(t *testing.T) {
	configured := consumer(&failingQueue{})
	configured.OTLPEndpoint = "http://127.0.0.1:9"
	summary := configured.Run(context.Background(), nil)
	if summary.TelemetryExportError != "export_failed" {
		t.Fatalf("telemetry = %q", summary.TelemetryExportError)
	}
	// The export failure never changes the run's own outcome.
	if summary.ExitCode != 1 || strings.Contains(summary.TelemetryExportError, "127.0.0.1") {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestNewMetricsKeepsAggregateMetricsWhenTelemetryFailed(t *testing.T) {
	metrics, state := NewMetrics(2.5, nil, errors.New("collector unavailable"))
	if metrics == nil || metrics.Snapshot().ConfiguredRate != 2.5 || state != "initialization_failed" {
		t.Fatalf("metrics=%#v state=%q", metrics, state)
	}
	if metrics, state := NewMetrics(1, nil, nil); metrics == nil || state != "" {
		t.Fatalf("metrics=%#v state=%q", metrics, state)
	}
}
