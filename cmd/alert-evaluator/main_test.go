package main

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	dynamoadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/dynamo"
	influxadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/influx"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

func TestNewRunnerWiresTheSelectedWindowReader(t *testing.T) {
	dynamoClient := dynamodb.NewFromConfig(aws.Config{Region: "us-east-1"})
	client := influxdb2.NewClient("http://127.0.0.1:1", "token")
	t.Cleanup(client.Close)
	queryAPI := client.QueryAPI("org")
	for _, test := range []struct {
		args   []string
		wantV2 bool
	}{{args: nil}, {args: []string{"--telemetry-source=v2"}, wantV2: true}} {
		config, err := alertevaluator.LoadRunConfig(test.args, func(key string) (string, bool) {
			switch key {
			case "INFLUXDB_BUCKET_RAW":
				return "raw_bucket", true
			case "TELEGRAM_DELIVERY_ENABLED":
				return "true", true
			}
			return "", false
		})
		if err != nil {
			t.Fatal(err)
		}
		runner := newRunner(config, dynamoClient, queryAPI)
		store, ok := runner.Store.(dynamoadapter.Store)
		if !ok || store.Table != config.DynamoDBTable || store.Client != dynamoClient || !store.TelegramDeliveryEnabled {
			t.Fatalf("store = %#v", runner.Store)
		}
		switch reader := runner.Windows.(type) {
		case influxadapter.V2WindowReader:
			querier, ok := reader.Querier.(influxadapter.ClientV2Querier)
			if !test.wantV2 || reader.Bucket != "raw_bucket" || !ok || querier.API != queryAPI {
				t.Fatalf("args %v: windows = %#v", test.args, reader)
			}
		case influxadapter.WindowReader:
			querier, ok := reader.Querier.(influxadapter.ClientQuerier)
			if test.wantV2 || reader.Bucket != "raw_bucket" || !ok || querier.API != queryAPI {
				t.Fatalf("args %v: windows = %#v", test.args, reader)
			}
		default:
			t.Fatalf("args %v: windows = %T", test.args, runner.Windows)
		}
	}
}

// captureStdout runs fn and returns what it wrote to os.Stdout.
func captureStdout(t *testing.T, fn func() int) (int, []byte) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = write
	code := fn()
	os.Stdout = original
	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	return code, output
}

func TestRunRejectsInvalidTelemetrySourceAsConfigurationError(t *testing.T) {
	// Fail fast instead of reaching real AWS if validation regresses.
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("DYNAMODB_ENDPOINT_URL", "http://127.0.0.1:1")
	t.Setenv("INFLUXDB_URL", "http://127.0.0.1:1")
	for _, test := range []struct {
		env  string
		args []string
	}{
		{env: "bogus", args: []string{"run"}},
		{env: "legacy", args: []string{"run", "--telemetry-source=V2"}},
	} {
		t.Setenv("ALERT_EVALUATOR_TELEMETRY_SOURCE", test.env)
		code, output := captureStdout(t, func() int { return runMain(test.args) })
		var summary struct {
			Result          string         `json:"result"`
			ErrorCategories map[string]int `json:"error_categories"`
		}
		if err := json.Unmarshal(output, &summary); err != nil {
			t.Fatalf("env %q args %v: output %q: %v", test.env, test.args, output, err)
		}
		if code != alertevaluator.ExitFatal || summary.Result != "fatal_failure" ||
			len(summary.ErrorCategories) != 1 || summary.ErrorCategories["configuration"] != 1 {
			t.Fatalf("env %q args %v: exit %d, summary %+v; want a configuration failure", test.env, test.args, code, summary)
		}
	}
}
