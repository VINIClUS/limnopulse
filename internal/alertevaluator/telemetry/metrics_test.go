package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecorderEmitsBoundedRunMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	recorder, err := newRecorder(reader)
	if err != nil {
		t.Fatal(err)
	}
	recorder.Record(context.Background(), alertevaluator.RunSummary{
		Result: "success", RulesEvaluated: 7, MissedSlots: 8, CoalescedRules: 2, IncidentsFired: 1,
		IncidentsRecovered: 1, RulesSkipped: 3, RulesWithError: 1,
		Duration: 1250 * time.Millisecond,
	})
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
		"alert_evaluator_rules_evaluated_total", "alert_evaluator_rules_delayed_total",
		"alert_evaluator_incidents_fired_total", "alert_evaluator_incidents_recovered_total",
		"alert_evaluator_rules_skipped_total", "alert_evaluator_rules_errors_total",
		"alert_evaluator_run_duration_ms",
	} {
		if !names[name] {
			t.Fatalf("metric %s not collected: %#v", name, names)
		}
	}
}

func TestEmptyEndpointDisablesOTLP(t *testing.T) {
	recorder, err := New(context.Background(), "")
	if err != nil || recorder != nil {
		t.Fatalf("recorder = %#v, err = %v", recorder, err)
	}
}

func TestMetricsEndpointUsesStandardOTLPHTTPPath(t *testing.T) {
	if got := metricsEndpoint("http://collector:4318"); got != "http://collector:4318/v1/metrics" {
		t.Fatalf("metricsEndpoint() = %q", got)
	}
	if got := metricsEndpoint("http://collector/custom"); got != "http://collector/custom" {
		t.Fatalf("custom metricsEndpoint() = %q", got)
	}
}
