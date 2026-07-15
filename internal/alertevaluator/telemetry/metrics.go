package telemetry

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/semconv/v1.40.0"
)

type Recorder struct {
	provider   *sdkmetric.MeterProvider
	evaluated  metric.Int64Counter
	delayed    metric.Int64Counter
	fired      metric.Int64Counter
	recovered  metric.Int64Counter
	skipped    metric.Int64Counter
	errors     metric.Int64Counter
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

func newRecorder(reader sdkmetric.Reader) (*Recorder, error) {
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("alert-evaluator"),
		)),
	)
	meter := provider.Meter("github.com/VINIClUS/limnopulse/alert-evaluator")
	recorder := &Recorder{provider: provider}
	var err error
	if recorder.evaluated, err = meter.Int64Counter("alert_evaluator_rules_evaluated_total"); err != nil {
		return nil, err
	}
	if recorder.delayed, err = meter.Int64Counter("alert_evaluator_rules_delayed_total"); err != nil {
		return nil, err
	}
	if recorder.fired, err = meter.Int64Counter("alert_evaluator_incidents_fired_total"); err != nil {
		return nil, err
	}
	if recorder.recovered, err = meter.Int64Counter("alert_evaluator_incidents_recovered_total"); err != nil {
		return nil, err
	}
	if recorder.skipped, err = meter.Int64Counter("alert_evaluator_rules_skipped_total"); err != nil {
		return nil, err
	}
	if recorder.errors, err = meter.Int64Counter("alert_evaluator_rules_errors_total"); err != nil {
		return nil, err
	}
	recorder.durationMS, err = meter.Float64Histogram(
		"alert_evaluator_run_duration_ms",
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}
	return recorder, nil
}

func (recorder *Recorder) Record(ctx context.Context, summary alertevaluator.RunSummary) {
	if recorder == nil {
		return
	}
	options := metric.WithAttributes(attribute.String("result", summary.Result))
	recorder.evaluated.Add(ctx, int64(summary.RulesEvaluated), options)
	recorder.delayed.Add(ctx, int64(summary.CoalescedRules), options)
	recorder.fired.Add(ctx, int64(summary.IncidentsFired), options)
	recorder.recovered.Add(ctx, int64(summary.IncidentsRecovered), options)
	recorder.skipped.Add(ctx, int64(summary.RulesSkipped), options)
	recorder.errors.Add(ctx, int64(summary.RulesWithError), options)
	recorder.durationMS.Record(ctx, float64(summary.Duration)/float64(time.Millisecond), options)
}

func (recorder *Recorder) Shutdown(ctx context.Context) error {
	if recorder == nil {
		return nil
	}
	flushErr := recorder.provider.ForceFlush(ctx)
	shutdownErr := recorder.provider.Shutdown(ctx)
	return errors.Join(flushErr, shutdownErr)
}
