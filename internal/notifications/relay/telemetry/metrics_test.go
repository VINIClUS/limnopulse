package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecorderEmitsNoPIIRelayRunMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	recorder, err := newRecorder(reader)
	if err != nil {
		t.Fatal(err)
	}
	recorder.Record(context.Background(), relay.RunSummary{
		Result: "partial_failure", Backlog: 9, OldestBacklogAgeSeconds: 120,
		DeadlineReached: true, CapReached: true, IntentsProcessed: 2, DependenciesProcessed: 1,
		DeliveriesCreated: 4, DeliveriesCancelled: 3, RecipientsFiltered: 5,
		RecipientFilteredBySeverity: 2, PublishedJobs: 6, SQSErrors: 1, WorkErrors: 1,
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
		"notification_relay_backlog_items", "notification_relay_oldest_backlog_age_seconds",
		"notification_relay_deadline_reached_total", "notification_relay_cap_reached_total",
		"notification_relay_intents_processed_total", "notification_relay_dependencies_processed_total",
		"notification_relay_deliveries_created_total", "notification_relay_deliveries_cancelled_total",
		"notification_relay_recipients_filtered_total", "notification_relay_recipient_filtered_by_severity_total",
		"notification_relay_jobs_published_total", "notification_relay_sqs_errors_total",
		"notification_relay_work_errors_total", "notification_relay_run_duration_ms",
	} {
		if !names[name] {
			t.Fatalf("metric %s not collected: %#v", name, names)
		}
	}
}

func TestEmptyEndpointDisablesRelayOTLP(t *testing.T) {
	recorder, err := New(context.Background(), "")
	if err != nil || recorder != nil {
		t.Fatalf("recorder = %#v, err = %v", recorder, err)
	}
}
