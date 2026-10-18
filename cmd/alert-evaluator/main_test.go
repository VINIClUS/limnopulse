package main

import (
	"testing"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	influxadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/influx"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func TestActiveAlertIndexBackfillRequiresExplicitTenant(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	if got := runMain([]string{"backfill-active-alert-index"}); got == 0 {
		t.Fatal("expected missing tenant scope to fail")
	}
}

func TestActiveAlertIndexBackfillRejectsInvalidFlag(t *testing.T) {
	if got := runMain([]string{"backfill-active-alert-index", "--limit=-1"}); got == 0 {
		t.Fatal("expected invalid limit to fail")
	}
}

func TestActiveAlertIndexBackfillAcceptsTenantIDAlias(t *testing.T) {
	config, err := parseActiveAlertIndexBackfillArgs([]string{"--tenant-id", "tnt_123"})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.tenants) != 1 || config.tenants[0] != "tnt_123" {
		t.Fatalf("tenants = %#v", config.tenants)
	}
}

func TestWindowReaderForSelectsLegacyReaderByDefault(t *testing.T) {
	config, err := alertevaluator.LoadRunConfig(nil, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	queryAPI := influxdb2.NewClient("http://127.0.0.1:1", "token").QueryAPI("org")
	reader := windowReaderFor(config, queryAPI)
	legacy, ok := reader.(influxadapter.WindowReader)
	if !ok {
		t.Fatalf("default reader = %T, want influx.WindowReader", reader)
	}
	querier, ok := legacy.Querier.(influxadapter.ClientQuerier)
	if legacy.Bucket != config.InfluxDBBucket || !ok || querier.API != queryAPI {
		t.Fatalf("legacy reader = %#v", legacy)
	}
}

func TestWindowReaderForSelectsV2Reader(t *testing.T) {
	config, err := alertevaluator.LoadRunConfig([]string{"--telemetry-source=v2"}, func(key string) (string, bool) {
		if key == "INFLUXDB_BUCKET_RAW" {
			return "raw$archive", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	queryAPI := influxdb2.NewClient("http://127.0.0.1:1", "token").QueryAPI("org")
	reader := windowReaderFor(config, queryAPI)
	v2, ok := reader.(influxadapter.V2WindowReader)
	if !ok {
		t.Fatalf("v2 reader = %T, want influx.V2WindowReader", reader)
	}
	querier, ok := v2.Querier.(influxadapter.ClientV2Querier)
	if v2.Bucket != "raw$archive" || !ok || querier.API != queryAPI {
		t.Fatalf("v2 reader = %#v", v2)
	}
}

func TestRunRejectsInvalidTelemetrySourceAsConfigurationError(t *testing.T) {
	t.Setenv("ALERT_EVALUATOR_TELEMETRY_SOURCE", "bogus")
	if got := runMain([]string{"run"}); got != alertevaluator.ExitFatal {
		t.Fatalf("exit = %d, want %d", got, alertevaluator.ExitFatal)
	}
	t.Setenv("ALERT_EVALUATOR_TELEMETRY_SOURCE", "legacy")
	if got := runMain([]string{"run", "--telemetry-source=V2"}); got != alertevaluator.ExitFatal {
		t.Fatalf("exit = %d, want %d", got, alertevaluator.ExitFatal)
	}
}
