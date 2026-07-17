package telemetry

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/semconv/v1.40.0"
)

type Recorder struct {
	provider          *sdkmetric.MeterProvider
	counters          map[string]metric.Int64Counter
	limiterWait       metric.Float64Histogram
	limiterWaiters    metric.Int64UpDownCounter
	activeConcurrency metric.Int64UpDownCounter
	configuredRate    metric.Float64Gauge
}

func New(ctx context.Context, endpoint string) (*Recorder, error) {
	if endpoint == "" {
		return nil, nil
	}
	exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(metricsEndpoint(endpoint)))
	if err != nil {
		return nil, err
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(15*time.Second))
	return newRecorder(reader)
}

func newRecorder(reader sdkmetric.Reader) (*Recorder, error) {
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL, semconv.ServiceName("notification-worker"),
		)),
	)
	meter := provider.Meter("github.com/VINIClUS/limnopulse/notification-worker")
	recorder := &Recorder{provider: provider, counters: make(map[string]metric.Int64Counter)}
	for _, name := range []string{
		"notification_worker_attempts_total", "notification_worker_ambiguous_total",
		"notification_worker_retries_total", "notification_worker_possible_duplicates_total",
		"notification_worker_unknown_total", "notification_worker_succeeded_after_retry_total",
		"notification_worker_send_started_total", "notification_worker_throttling_total",
		"notification_worker_quota_total",
	} {
		counter, err := meter.Int64Counter(name)
		if err != nil {
			return nil, err
		}
		recorder.counters[name] = counter
	}
	var err error
	if recorder.limiterWait, err = meter.Float64Histogram("notification_worker_limiter_wait_seconds", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if recorder.limiterWaiters, err = meter.Int64UpDownCounter("notification_worker_limiter_waiters", metric.WithUnit("{waiter}")); err != nil {
		return nil, err
	}
	if recorder.activeConcurrency, err = meter.Int64UpDownCounter("notification_worker_active_concurrency", metric.WithUnit("{send}")); err != nil {
		return nil, err
	}
	if recorder.configuredRate, err = meter.Float64Gauge("notification_worker_configured_rate", metric.WithUnit("{send}/s")); err != nil {
		return nil, err
	}
	return recorder, nil
}

func (recorder *Recorder) ConfiguredRate(value float64) {
	if recorder != nil {
		recorder.configuredRate.Record(context.Background(), value)
	}
}

func (recorder *Recorder) LimiterWaitStarted() {
	if recorder != nil {
		recorder.limiterWaiters.Add(context.Background(), 1)
	}
}

func (recorder *Recorder) LimiterWaitFinished(duration time.Duration) {
	if recorder == nil {
		return
	}
	recorder.limiterWaiters.Add(context.Background(), -1)
	recorder.limiterWait.Record(context.Background(), duration.Seconds())
}

func (recorder *Recorder) ProviderCallStarted(possibleDuplicate bool) {
	if recorder == nil {
		return
	}
	recorder.counters["notification_worker_attempts_total"].Add(context.Background(), 1)
	recorder.counters["notification_worker_send_started_total"].Add(context.Background(), 1)
	if possibleDuplicate {
		recorder.counters["notification_worker_possible_duplicates_total"].Add(context.Background(), 1)
	}
	recorder.activeConcurrency.Add(context.Background(), 1)
}

func (recorder *Recorder) ProviderCallFinished() {
	if recorder != nil {
		recorder.activeConcurrency.Add(context.Background(), -1)
	}
}

func (recorder *Recorder) Retry(category worker.SendErrorCategory, ambiguous bool) {
	if recorder == nil {
		return
	}
	recorder.counters["notification_worker_retries_total"].Add(context.Background(), 1)
	if ambiguous {
		recorder.counters["notification_worker_ambiguous_total"].Add(context.Background(), 1)
	}
	if category == worker.ErrorRetryableThrottling {
		recorder.counters["notification_worker_throttling_total"].Add(context.Background(), 1)
	}
	if category == worker.ErrorRetryableQuota {
		recorder.counters["notification_worker_quota_total"].Add(context.Background(), 1)
	}
}

func (recorder *Recorder) SucceededAfterRetry() {
	if recorder != nil {
		recorder.counters["notification_worker_succeeded_after_retry_total"].Add(context.Background(), 1)
	}
}

func (recorder *Recorder) Unknown() {
	if recorder != nil {
		recorder.counters["notification_worker_unknown_total"].Add(context.Background(), 1)
	}
}

func (recorder *Recorder) Shutdown(ctx context.Context) error {
	if recorder == nil {
		return nil
	}
	return errors.Join(recorder.provider.ForceFlush(ctx), recorder.provider.Shutdown(ctx))
}

func metricsEndpoint(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return endpoint
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/v1/metrics"
	}
	return parsed.String()
}

var _ worker.MetricsObserver = (*Recorder)(nil)
