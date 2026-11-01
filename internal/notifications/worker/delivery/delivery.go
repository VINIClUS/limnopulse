// Package delivery runs one delivery consumer, email or Telegram: the processor with its lease
// guard over one jobs queue, under a runner, with the send metrics and their telemetry. The
// channel's compose package supplies the queue, store, sender and limiter.
package delivery

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workertelemetry "github.com/VINIClUS/limnopulse/internal/notifications/worker/telemetry"
	"github.com/google/uuid"
)

type Consumer struct {
	// OwnerPrefix starts the lease owner of this process, followed by a random UUID.
	OwnerPrefix string
	Queue       worker.Queue
	Store       worker.Store
	Sender      worker.EmailSender
	Limiter     worker.Limiter
	// Rate is the configured provider-call rate the metrics report.
	Rate         float64
	OTLPEndpoint string

	Concurrency       int
	ReceiveBatch      int
	ReceiveWait       time.Duration
	Visibility        time.Duration
	ProcessingLease   time.Duration
	ProviderTimeout   time.Duration
	InvalidVisibility time.Duration
	RenewalInterval   time.Duration
	DrainTimeout      time.Duration
	OTLPFlushTimeout  time.Duration
}

// Run consumes until ctx is cancelled, reporting to loop, then adds the send metrics to the
// summary and flushes their telemetry with a fresh budget.
func (consumer Consumer) Run(ctx context.Context, loop worker.Loop) worker.RunSummary {
	recorder, telemetryErr := workertelemetry.New(ctx, consumer.OTLPEndpoint)
	metrics, telemetryState := NewMetrics(consumer.Rate, recorder, telemetryErr)
	now := func() time.Time { return time.Now().UTC() }
	processor := worker.Processor{
		Store: consumer.Store, Sender: consumer.Sender, Limiter: consumer.Limiter,
		Owner:           consumer.OwnerPrefix + uuid.NewString(),
		ProcessingLease: consumer.ProcessingLease, ProviderTimeout: consumer.ProviderTimeout,
		InvalidVisibility: consumer.InvalidVisibility, Now: now,
		NewAttemptID:   func() string { return "att_" + uuid.NewString() },
		JitterFraction: rand.Float64, Metrics: metrics,
	}
	processor.Guard = worker.RenewalGuard{
		Store: consumer.Store, Queue: consumer.Queue, Interval: consumer.RenewalInterval,
		LeaseTTL: consumer.ProcessingLease, Visibility: consumer.Visibility, Now: now,
	}
	summary := (worker.Runner{
		Queue: consumer.Queue, Handler: processor, Concurrency: consumer.Concurrency,
		ReceiveBatch: consumer.ReceiveBatch, ReceiveWait: consumer.ReceiveWait,
		Visibility: consumer.Visibility, DrainTimeout: consumer.DrainTimeout, Loop: loop,
	}).Run(ctx)
	summary.Metrics = metrics.Snapshot()
	if telemetryState != "" {
		summary.TelemetryExportError = telemetryState
		return summary
	}
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consumer.OTLPFlushTimeout)
	defer cancel()
	if err := recorder.Shutdown(flushCtx); err != nil {
		summary.TelemetryExportError = "export_failed"
	}
	return summary
}

// NewMetrics keeps the aggregate send metrics even when the telemetry exporter failed to start.
func NewMetrics(rate float64, observer worker.MetricsObserver, initializationErr error) (*worker.Metrics, string) {
	if initializationErr != nil {
		return worker.NewMetrics(rate), "initialization_failed"
	}
	return worker.NewMetricsWithObserver(rate, observer), ""
}
