package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	dynamoadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/dynamo"
	influxadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/influx"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

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

type failureLine struct {
	Result           string         `json:"result"`
	ExitCode         int            `json:"exit_code"`
	RetryRecommended bool           `json:"retry_recommended"`
	ErrorCategories  map[string]int `json:"error_categories"`
	Error            string         `json:"error"`
}

func environment(extra map[string]string) func(string) (string, bool) {
	values := map[string]string{"DYNAMODB_ENDPOINT_URL": "http://127.0.0.1:1", "INFLUXDB_URL": "http://127.0.0.1:1"}
	for key, value := range extra {
		values[key] = value
	}
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func credentialsDirectory(t *testing.T, influxToken string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"aws-access-key-id": "AKIDEVALUATOR", "aws-secret-access-key": "evaluator-secret"}
	if influxToken != "" {
		files["influxdb-token"] = influxToken
	}
	for name, value := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// evaluateWith runs runMain with a fake pass that records its configuration.
func evaluateWith(t *testing.T, args []string, lookup func(string) (string, bool), summary alertevaluator.RunSummary, err error) (int, *alertevaluator.RunConfig, string) {
	t.Helper()
	var captured *alertevaluator.RunConfig
	var output bytes.Buffer
	code := runMain(context.Background(), args, dependencies{
		Output: &output, LookupEnv: lookup,
		Evaluate: func(_ context.Context, config alertevaluator.RunConfig, load workercmd.AWSLoader) (alertevaluator.RunSummary, error) {
			if load == nil {
				t.Error("no AWS loader")
			}
			captured = &config
			return summary, err
		},
	})
	return code, captured, output.String()
}

func decodeFailure(t *testing.T, output string) failureLine {
	t.Helper()
	var line failureLine
	if err := json.Unmarshal([]byte(output), &line); err != nil {
		t.Fatalf("output %q: %v", output, err)
	}
	return line
}

func TestEvaluatorAcceptsOnlyTheRunCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"backfill-schedule"}, {"backfill-active-alert-index", "--tenant=a"}, {"evaluate"}} {
		code, captured, output := evaluateWith(t, args, environment(nil), alertevaluator.RunSummary{}, nil)
		line := decodeFailure(t, output)
		if code != workerproc.ExitConfiguration || captured != nil || line.ExitCode != code ||
			line.RetryRecommended || line.ErrorCategories["usage"] != 1 || !strings.Contains(line.Error, "limnopulse-admin") {
			t.Fatalf("args %v: exit=%d started=%t line=%+v", args, code, captured != nil, line)
		}
	}
}

func TestEvaluatorPassesTheSummaryExitCodeThrough(t *testing.T) {
	for _, want := range []int{alertevaluator.ExitSuccess, alertevaluator.ExitFatal, alertevaluator.ExitPartial} {
		code, captured, output := evaluateWith(t, []string{"run"}, environment(nil),
			alertevaluator.RunSummary{RunID: "run-1", ExitCode: want}, nil)
		var summary alertevaluator.RunSummary
		if err := json.Unmarshal([]byte(output), &summary); err != nil || code != want || captured == nil || summary.RunID != "run-1" {
			t.Fatalf("want %d: exit=%d summary=%+v err=%v", want, code, summary, err)
		}
	}
}

func TestEvaluatorExitsOneWhenItCannotCompose(t *testing.T) {
	code, _, output := evaluateWith(t, []string{"run"}, environment(nil), alertevaluator.RunSummary{}, errors.New("no AWS config"))
	line := decodeFailure(t, output)
	if code != workerproc.ExitFailure || !line.RetryRecommended || line.ErrorCategories["aws_configuration"] != 1 {
		t.Fatalf("exit=%d line=%+v", code, line)
	}
}

func TestRunRejectsInvalidSettingsAsConfigurationErrors(t *testing.T) {
	for _, test := range []struct {
		env  string
		args []string
	}{
		{env: "bogus", args: []string{"run"}},
		{env: "legacy", args: []string{"run", "--telemetry-source=V2"}},
		{env: "legacy", args: []string{"run", "--no-such-flag"}},
		{env: "legacy", args: []string{"run", "-h"}},
	} {
		code, captured, output := evaluateWith(t, test.args,
			environment(map[string]string{"ALERT_EVALUATOR_TELEMETRY_SOURCE": test.env}), alertevaluator.RunSummary{}, nil)
		line := decodeFailure(t, output)
		if code != workerproc.ExitConfiguration || captured != nil || line.Result != "fatal_failure" || line.RetryRecommended ||
			len(line.ErrorCategories) != 1 || line.ErrorCategories["configuration"] != 1 {
			t.Fatalf("env %q args %v: exit %d, line %+v; want a configuration failure", test.env, test.args, code, line)
		}
	}
}

func TestEvaluatorReadsTheInfluxTokenFromItsCredential(t *testing.T) {
	code, captured, output := evaluateWith(t, []string{"run"}, environment(map[string]string{
		workerproc.SupervisorEnv: "systemd", "CREDENTIALS_DIRECTORY": credentialsDirectory(t, "credential-influx-token\n"),
	}), alertevaluator.RunSummary{}, nil)
	if code != 0 || captured == nil || captured.InfluxDBToken != "credential-influx-token" {
		t.Fatalf("exit=%d started=%t output=%s", code, captured != nil, output)
	}
	if strings.Contains(output, "credential-influx-token") {
		t.Fatalf("summary leaked the token: %s", output)
	}
}

func TestEvaluatorKeepsTheEnvTokenForLocalRuns(t *testing.T) {
	code, captured, _ := evaluateWith(t, []string{"run"}, environment(map[string]string{"INFLUXDB_TOKEN": "local-token"}),
		alertevaluator.RunSummary{}, nil)
	if code != 0 || captured == nil || captured.InfluxDBToken != "local-token" {
		t.Fatalf("exit=%d started=%t", code, captured != nil)
	}
}

func TestEvaluatorExits78OnCredentialErrors(t *testing.T) {
	for name, lookup := range map[string]func(string) (string, bool){
		"systemd without credentials": environment(map[string]string{workerproc.SupervisorEnv: "systemd"}),
		"env token next to the credential": environment(map[string]string{
			"CREDENTIALS_DIRECTORY": credentialsDirectory(t, "credential-token"), "INFLUXDB_TOKEN": "env-file-token",
		}),
		"missing token credential": environment(map[string]string{"CREDENTIALS_DIRECTORY": credentialsDirectory(t, "")}),
	} {
		code, captured, output := evaluateWith(t, []string{"run"}, lookup, alertevaluator.RunSummary{}, nil)
		line := decodeFailure(t, output)
		if code != workerproc.ExitConfiguration || captured != nil || line.ErrorCategories["credentials"] != 1 ||
			strings.Contains(output, "env-file-token") || strings.Contains(output, "credential-token") {
			t.Fatalf("%s: exit=%d started=%t output=%s", name, code, captured != nil, output)
		}
	}
}

type refusingHTTPClient struct{ calls atomic.Int32 }

func (client *refusingHTTPClient) Do(*http.Request) (*http.Response, error) {
	client.calls.Add(1)
	return nil, errors.New("connection refused")
}

func TestEvaluateComposesTheStoreFromTheLoadedAWSConfig(t *testing.T) {
	config, err := alertevaluator.LoadRunConfig(nil, environment(nil))
	if err != nil {
		t.Fatal(err)
	}
	client := &refusingHTTPClient{}
	summary, err := evaluate(context.Background(), config, func(_ context.Context, region, endpoint string) (aws.Config, error) {
		return aws.Config{
			Region: region, BaseEndpoint: aws.String(endpoint), HTTPClient: client,
			Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), Retryer: func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	})
	// Discovery reached the composed DynamoDB client, which refused every call.
	if err != nil || summary.ExitCode != alertevaluator.ExitFatal || summary.ErrorCategories["discovery"] != 1 || client.calls.Load() == 0 {
		t.Fatalf("summary=%+v err=%v calls=%d", summary, err, client.calls.Load())
	}
}

func TestEvaluateReportsAnAWSLoadError(t *testing.T) {
	config, err := alertevaluator.LoadRunConfig(nil, environment(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evaluate(context.Background(), config, func(context.Context, string, string) (aws.Config, error) {
		return aws.Config{}, errors.New("no profile")
	}); err == nil {
		t.Fatal("expected the AWS load error")
	}
}
