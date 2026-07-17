package telemetry

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/semconv/v1.40.0"
)

type Recorder struct {
	provider   *sdkmetric.MeterProvider
	counters   map[string]metric.Int64Counter
	backlog    metric.Int64Histogram
	oldestAge  metric.Int64Histogram
	durationMS metric.Float64Histogram
}

func New(ctx context.Context, endpoint string) (*Recorder, error) {
	if endpoint == "" {
		return nil, nil
	}
	exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(metricsEndpoint(endpoint)))
	if err != nil {
		return nil, err
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Hour))
	return newRecorder(reader)
}

func newRecorder(reader sdkmetric.Reader) (*Recorder, error) {
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL, semconv.ServiceName("notification-relay"),
		)),
	)
	meter := provider.Meter("github.com/VINIClUS/limnopulse/notification-relay")
	recorder := &Recorder{provider: provider, counters: make(map[string]metric.Int64Counter)}
	for _, name := range []string{
		"notification_relay_deadline_reached_total", "notification_relay_cap_reached_total",
		"notification_relay_intents_processed_total", "notification_relay_dependencies_processed_total",
		"notification_relay_deliveries_created_total", "notification_relay_deliveries_cancelled_total",
		"notification_relay_recipients_filtered_total", "notification_relay_recipient_filtered_by_severity_total",
		"notification_relay_jobs_published_total", "notification_relay_sqs_errors_total",
		"notification_relay_work_errors_total",
	} {
		counter, err := meter.Int64Counter(name)
		if err != nil {
			return nil, err
		}
		recorder.counters[name] = counter
	}
	var err error
	if recorder.backlog, err = meter.Int64Histogram(
		"notification_relay_backlog_items", metric.WithUnit("{item}"),
	); err != nil {
		return nil, err
	}
	if recorder.oldestAge, err = meter.Int64Histogram(
		"notification_relay_oldest_backlog_age_seconds", metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if recorder.durationMS, err = meter.Float64Histogram(
		"notification_relay_run_duration_ms", metric.WithUnit("ms"),
	); err != nil {
		return nil, err
	}
	return recorder, nil
}

func (recorder *Recorder) Record(ctx context.Context, summary relay.RunSummary) {
	if recorder == nil {
		return
	}
	options := metric.WithAttributes(attribute.String("result", summary.Result))
	recorder.backlog.Record(ctx, int64(summary.Backlog), options)
	recorder.oldestAge.Record(ctx, summary.OldestBacklogAgeSeconds, options)
	recorder.durationMS.Record(ctx, float64(summary.Duration)/float64(time.Millisecond), options)
	deadline := int64(0)
	if summary.DeadlineReached {
		deadline = 1
	}
	capReached := int64(0)
	if summary.CapReached {
		capReached = 1
	}
	values := map[string]int64{
		"notification_relay_deadline_reached_total":               deadline,
		"notification_relay_cap_reached_total":                    capReached,
		"notification_relay_intents_processed_total":              int64(summary.IntentsProcessed),
		"notification_relay_dependencies_processed_total":         int64(summary.DependenciesProcessed),
		"notification_relay_deliveries_created_total":             int64(summary.DeliveriesCreated),
		"notification_relay_deliveries_cancelled_total":           int64(summary.DeliveriesCancelled),
		"notification_relay_recipients_filtered_total":            int64(summary.RecipientsFiltered),
		"notification_relay_recipient_filtered_by_severity_total": int64(summary.RecipientFilteredBySeverity),
		"notification_relay_jobs_published_total":                 int64(summary.PublishedJobs),
		"notification_relay_sqs_errors_total":                     int64(summary.SQSErrors),
		"notification_relay_work_errors_total":                    int64(summary.WorkErrors),
	}
	for name, value := range values {
		recorder.counters[name].Add(ctx, value, options)
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
	} else if !strings.HasSuffix(parsed.Path, "/v1/metrics") {
		return endpoint
	}
	return parsed.String()
}
