package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	notificationdynamo "github.com/VINIClUS/limnopulse/internal/notifications/dynamo"
	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestWorkerLoadsContinuousConfigAndReturnsGracefulExitWithoutPII(t *testing.T) {
	privateQueue := "http://sqs:9324/queue/private-jobs"
	privateFeedbackQueue := "http://sqs:9324/queue/private-ses-events"
	environment := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "private-domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb:8000", "SQS_NOTIFICATION_JOBS_URL": privateQueue,
		"SQS_SES_EVENTS_URL": privateFeedbackQueue,
		"SQS_ENDPOINT_URL":   "http://sqs:9324", "SES_FROM_EMAIL": "private-sender@example.com",
		"SES_ENDPOINT_URL": "http://ses:8080",
	}
	var captured workerconfig.RunConfig
	var output bytes.Buffer
	exitCode := runMain(context.Background(), []string{
		"worker", "--send-concurrency=7", "--feedback-concurrency=3", "--max-send-rate=3.5", "--send-burst=2",
	}, dependencies{
		Output:    &output,
		LookupEnv: func(key string) (string, bool) { value, ok := environment[key]; return value, ok },
		RunWorker: func(_ context.Context, config workerconfig.RunConfig) (worker.RunSummary, error) {
			captured = config
			return worker.RunSummary{Graceful: true, Result: "success", ExitCode: 0,
				Metrics: worker.MetricsSnapshot{ConfiguredRate: config.MaxSendRate}}, nil
		},
	})
	if exitCode != 0 || captured.SendConcurrency != 7 || captured.FeedbackConcurrency != 3 ||
		captured.SQSFeedbackURL != privateFeedbackQueue || captured.MaxSendRate != 3.5 || captured.SendBurst != 2 ||
		captured.ReceiveWait != 20*time.Second || captured.ReceiveBatch != 10 ||
		captured.VisibilityTimeout != time.Minute || captured.ProcessingLease != time.Minute ||
		captured.ProviderTimeout != 15*time.Second || captured.DrainTimeout != 30*time.Second ||
		captured.SQSReceiveTimeout != 25*time.Second || captured.SQSRequestTimeout != 5*time.Second {
		t.Fatalf("exit=%d config=%#v output=%s", exitCode, captured, output.String())
	}
	for _, private := range []string{privateQueue, privateFeedbackQueue, "private-domain", "private-sender@example.com", "dynamodb:8000", "ses:8080"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("worker summary leaked %q: %s", private, output.String())
		}
	}
	var summary worker.RunSummary
	if err := json.Unmarshal(output.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Result != "success" || summary.ExitCode != 0 || summary.Metrics.ConfiguredRate != 3.5 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestWorkerRejectsConfigurationBeforeAWSAndMapsFatalRun(t *testing.T) {
	validEnv := map[string]string{
		"APP_ENV": "test", "AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "queue", "SQS_SES_EVENTS_URL": "feedback-queue", "SES_FROM_EMAIL": "sender@example.com",
	}
	tests := []struct {
		name                string
		args                []string
		env                 map[string]string
		run                 worker.RunSummary
		runErr              error
		wantCalls, wantExit int
	}{
		{name: "missing", args: []string{"worker"}, env: map[string]string{}, wantExit: 1},
		{name: "bad flag", args: []string{"worker", "--send-concurrency=0"}, env: validEnv, wantExit: 1},
		{name: "positional", args: []string{"worker", "extra"}, env: validEnv, wantExit: 1},
		{name: "fatal provider", args: []string{"worker"}, env: validEnv,
			run: worker.RunSummary{Fatal: true, Result: "fatal_failure", ExitCode: 1}, wantCalls: 1, wantExit: 1},
		{name: "setup error", args: []string{"worker"}, env: validEnv, runErr: errors.New("private setup"), wantCalls: 1, wantExit: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			var output bytes.Buffer
			exit := runMain(context.Background(), test.args, dependencies{
				Output:    &output,
				LookupEnv: func(key string) (string, bool) { value, ok := test.env[key]; return value, ok },
				RunWorker: func(context.Context, workerconfig.RunConfig) (worker.RunSummary, error) {
					calls++
					return test.run, test.runErr
				},
			})
			if exit != test.wantExit || calls != test.wantCalls || strings.Contains(output.String(), "private setup") {
				t.Fatalf("exit=%d calls=%d output=%s", exit, calls, output.String())
			}
		})
	}
}

func TestWorkerTelemetryInitializationFailureKeepsAggregateMetricsAvailable(t *testing.T) {
	metrics, state := newWorkerMetrics(2.5, nil, errors.New("collector unavailable"))
	if metrics == nil || metrics.Snapshot().ConfiguredRate != 2.5 || state != "initialization_failed" {
		t.Fatalf("metrics=%#v state=%q", metrics, state)
	}
}

func TestWorkerSummaryIncludesBoundedFeedbackMetricsWithoutPII(t *testing.T) {
	summary := worker.RunSummary{
		FeedbackMetrics: feedback.MetricsSnapshot{
			Applied: 1, Duplicates: 2, Ignored: 3, Malformed: 4,
			AwaitingDLQ: 5, PersistenceErrors: 6, Suppressed: 7,
		},
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	metrics, ok := document["feedback_metrics"].(map[string]any)
	if !ok || len(metrics) != 7 || metrics["applied"] != float64(1) || metrics["suppressed"] != float64(7) {
		t.Fatalf("feedback metrics = %#v", document["feedback_metrics"])
	}
	for _, forbidden := range []string{"owner@example.com", "delivery_id", "attempt_id", "provider_message_id"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("feedback metrics leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestWorkerAWSConfigsKeepRealSESOffLocalDynamoAndSQSCredentials(t *testing.T) {
	config := workerconfig.RunConfig{
		AWSRegion: "us-east-1", EmailSenderMode: "aws",
		DynamoDBEndpoint: "http://dynamodb.local", SQSEndpoint: "http://sqs.local",
	}
	var endpoints []string
	configs, err := loadWorkerAWSConfigs(context.Background(), config,
		func(_ context.Context, _ string, endpoint string) (aws.Config, error) {
			endpoints = append(endpoints, endpoint)
			source := "real"
			if endpoint != "" {
				source = "local"
			}
			return aws.Config{Credentials: credentials.NewStaticCredentialsProvider(source, "secret", "")}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	sesCredentials, err := configs.SES.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(endpoints, []string{"http://dynamodb.local", "http://sqs.local", ""}) ||
		sesCredentials.AccessKeyID != "real" {
		t.Fatalf("endpoints=%#v SES source=%q", endpoints, sesCredentials.AccessKeyID)
	}
}

func TestWorkerSESCredentialStartupPreflightFailsBeforeRuntime(t *testing.T) {
	calls := 0
	provider := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		calls++
		return aws.Credentials{}, errors.New("credentials unavailable")
	})
	httpClient := &countingHTTPClient{}
	_, err := prepareSESAWSConfig(context.Background(), aws.Config{Credentials: provider, HTTPClient: httpClient})
	if err == nil || calls != 1 || httpClient.calls != 0 {
		t.Fatalf("err=%v credential_calls=%d HTTP_calls=%d", err, calls, httpClient.calls)
	}
}

type countingHTTPClient struct{ calls int }

func (client *countingHTTPClient) Do(*http.Request) (*http.Response, error) {
	client.calls++
	return nil, errors.New("unexpected HTTP call")
}

func TestRelayLoadsValidatedOneShotConfigAndWritesNoPIISummary(t *testing.T) {
	privateQueue := "http://sqs:9324/queue/private-jobs"
	environment := map[string]string{
		"AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "private-domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb:8000", "SQS_NOTIFICATION_JOBS_URL": privateQueue,
	}
	var captured relayconfig.RunConfig
	var capturedDeadline time.Time
	runCalls := 0
	var output bytes.Buffer
	exitCode := runMain(context.Background(), []string{
		"relay", "--relay-time=2026-07-16T13:00:00Z", "--shard=1", "--shard-count=2",
		"--query-parallelism=3", "--work-parallelism=5", "--max-work=77", "--fanout-page-size=19",
	}, dependencies{
		Output: &output,
		LookupEnv: func(key string) (string, bool) {
			value, ok := environment[key]
			return value, ok
		},
		RunRelay: func(ctx context.Context, config relayconfig.RunConfig) (relay.RunSummary, error) {
			runCalls++
			captured = config
			capturedDeadline, _ = ctx.Deadline()
			return relay.RunSummary{
				RunID: "run_1", RelayTime: *config.RelayTime, Shard: config.Shard,
				ShardCount: config.ShardCount, Result: "success", ExitCode: relay.ExitSuccess,
				ScopeCompleted: true,
			}, nil
		},
	})
	if exitCode != relay.ExitSuccess || runCalls != 1 {
		t.Fatalf("exit = %d, run calls = %d, output = %s", exitCode, runCalls, output.String())
	}
	if captured.RelayTime == nil || captured.Shard != 1 || captured.ShardCount != 2 ||
		captured.QueryParallelism != 3 || captured.WorkParallelism != 5 || captured.MaxWork != 77 ||
		captured.FanoutPageSize != 19 || captured.GlobalDeadline != 45*time.Second ||
		captured.SoftDeadline != 40*time.Second || captured.ItemTimeout != 10*time.Second ||
		captured.LeaseTTL != 20*time.Second || captured.SQSRequestTimeout != 5*time.Second {
		t.Fatalf("captured config = %#v", captured)
	}
	if captured.BudgetStartedAt == nil || capturedDeadline.IsZero() ||
		capturedDeadline.Before(captured.BudgetStartedAt.Add(44*time.Second)) ||
		capturedDeadline.After(captured.BudgetStartedAt.Add(45*time.Second)) {
		t.Fatalf("budget start = %#v, deadline = %s", captured.BudgetStartedAt, capturedDeadline)
	}
	for _, private := range []string{privateQueue, "private-domain", "dynamodb:8000"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("relay summary leaked %q: %s", private, output.String())
		}
	}
	var summary relay.RunSummary
	if err := json.Unmarshal(output.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v; output = %s", err, output.String())
	}
	if summary.Result != "success" || summary.RunID != "run_1" {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRelayRejectsMissingEnvironmentAndInvalidFlagsBeforeAWS(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		lookupEnv func(string) (string, bool)
	}{
		{name: "missing environment", args: []string{"relay"}, lookupEnv: func(string) (string, bool) { return "", false }},
		{name: "invalid flag", args: []string{"relay", "--max-work=0"}, lookupEnv: relayCommandLookup},
		{name: "positional", args: []string{"relay", "extra"}, lookupEnv: relayCommandLookup},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runCalls := 0
			var output bytes.Buffer
			exitCode := runMain(context.Background(), test.args, dependencies{
				Output: &output, LookupEnv: test.lookupEnv,
				RunRelay: func(context.Context, relayconfig.RunConfig) (relay.RunSummary, error) {
					runCalls++
					return relay.RunSummary{}, nil
				},
			})
			if exitCode != relay.ExitFatal || runCalls != 0 {
				t.Fatalf("exit = %d, run calls = %d, output = %s", exitCode, runCalls, output.String())
			}
		})
	}
}

func relayCommandLookup(key string) (string, bool) {
	values := map[string]string{
		"AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb:8000", "SQS_NOTIFICATION_JOBS_URL": "queue",
	}
	value, ok := values[key]
	return value, ok
}

type fakeDynamo struct {
	queryOutput  *awssdk.QueryOutput
	queryError   error
	queryInputs  []*awssdk.QueryInput
	updateErrors []error
	updateInputs []*awssdk.UpdateItemInput
}

func (client *fakeDynamo) Query(
	_ context.Context,
	input *awssdk.QueryInput,
	_ ...func(*awssdk.Options),
) (*awssdk.QueryOutput, error) {
	client.queryInputs = append(client.queryInputs, input)
	if client.queryError != nil {
		return nil, client.queryError
	}
	if client.queryOutput != nil {
		return client.queryOutput, nil
	}
	return &awssdk.QueryOutput{}, nil
}

func (client *fakeDynamo) UpdateItem(
	_ context.Context,
	input *awssdk.UpdateItemInput,
	_ ...func(*awssdk.Options),
) (*awssdk.UpdateItemOutput, error) {
	client.updateInputs = append(client.updateInputs, input)
	if len(client.updateErrors) > 0 {
		err := client.updateErrors[0]
		client.updateErrors = client.updateErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	return &awssdk.UpdateItemOutput{}, nil
}

func TestBackfillRelayResolvesTenantFlagsAndFilesInFirstSeenOrderBeforeAWS(t *testing.T) {
	tenantFile := filepath.Join(t.TempDir(), "tenants.txt")
	if err := os.WriteFile(tenantFile, []byte(" tnt_2 \n\ntnt_1\ntnt_3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &fakeDynamo{}
	loadCalls := 0
	var output bytes.Buffer
	deps := dependencies{
		Output:    &output,
		LookupEnv: func(string) (string, bool) { return "", false },
		LoadDynamo: func(context.Context, string, string) (notificationdynamo.Client, error) {
			loadCalls++
			return client, nil
		},
	}

	exitCode := runMain(context.Background(), []string{
		"backfill-relay",
		"--tenant", " tnt_1 ",
		"--tenant-file", tenantFile,
		"--tenant", "tnt_4",
		"--tenant", "tnt_2",
	}, deps)
	if exitCode != 0 {
		t.Fatalf("exit code = %d, output = %s", exitCode, output.String())
	}
	if loadCalls != 1 {
		t.Fatalf("AWS load calls = %d, want 1", loadCalls)
	}
	if len(client.queryInputs) != 4 {
		t.Fatalf("Query calls = %d, want 4", len(client.queryInputs))
	}
	gotTenants := make([]string, 0, len(client.queryInputs))
	for _, input := range client.queryInputs {
		var values map[string]string
		if err := attributevalue.UnmarshalMap(input.ExpressionAttributeValues, &values); err != nil {
			t.Fatal(err)
		}
		gotTenants = append(gotTenants, strings.TrimPrefix(values[":pk"], "TENANT#"))
		if input.Limit == nil || *input.Limit != 25 {
			t.Fatalf("query limit = %#v, want 25", input.Limit)
		}
	}
	if want := []string{"tnt_1", "tnt_2", "tnt_3", "tnt_4"}; !reflect.DeepEqual(gotTenants, want) {
		t.Fatalf("tenant order = %v, want %v", gotTenants, want)
	}
	for _, sensitive := range []string{"tnt_1", "tnt_2", "tnt_3", "tnt_4", tenantFile} {
		if strings.Contains(output.String(), sensitive) {
			t.Fatalf("JSON output contains sensitive value %q: %s", sensitive, output.String())
		}
	}
	var result commandResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("decode output: %v; output = %s", err, output.String())
	}
	if result.Result != "success" || result.ExitCode != 0 || result.Summary == nil || result.Summary.Tenants != 4 {
		t.Fatalf("result = %#v", result)
	}
}

func TestBackfillRelayRejectsInvalidScopeAndPageSizeBeforeAWS(t *testing.T) {
	directory := t.TempDir()
	emptyFile := filepath.Join(directory, "empty.txt")
	if err := os.WriteFile(emptyFile, []byte("\n  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nulFile := filepath.Join(directory, "nul.txt")
	if err := os.WriteFile(nulFile, []byte("tnt_1\x00suffix\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missingFile := filepath.Join(directory, "sensitive-missing.txt")
	tests := []struct {
		name      string
		args      []string
		sensitive string
	}{
		{name: "no tenant", args: []string{"backfill-relay"}},
		{name: "empty effective file", args: []string{"backfill-relay", "--tenant-file", emptyFile}, sensitive: emptyFile},
		{name: "missing file", args: []string{"backfill-relay", "--tenant-file", missingFile}, sensitive: missingFile},
		{name: "NUL file", args: []string{"backfill-relay", "--tenant-file", nulFile}, sensitive: nulFile},
		{name: "empty flag", args: []string{"backfill-relay", "--tenant", "  "}},
		{name: "NUL flag", args: []string{"backfill-relay", "--tenant", "private\x00tenant"}, sensitive: "private"},
		{name: "zero page size", args: []string{"backfill-relay", "--tenant", "private_tenant", "--page-size", "0"}, sensitive: "private_tenant"},
		{name: "oversized page", args: []string{"backfill-relay", "--tenant", "private_tenant", "--page-size", "2147483648"}, sensitive: "private_tenant"},
		{name: "positional argument", args: []string{"backfill-relay", "--tenant", "private_tenant", "extra"}, sensitive: "private_tenant"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			loadCalls := 0
			var output bytes.Buffer
			exitCode := runMain(context.Background(), test.args, dependencies{
				Output:    &output,
				LookupEnv: func(string) (string, bool) { return "", false },
				LoadDynamo: func(context.Context, string, string) (notificationdynamo.Client, error) {
					loadCalls++
					return &fakeDynamo{}, nil
				},
			})
			if exitCode != 1 {
				t.Fatalf("exit code = %d, output = %s", exitCode, output.String())
			}
			if loadCalls != 0 {
				t.Fatalf("AWS load calls = %d, want 0", loadCalls)
			}
			if test.sensitive != "" && strings.Contains(output.String(), test.sensitive) {
				t.Fatalf("JSON contains sensitive input %q: %s", test.sensitive, output.String())
			}
			var result commandResult
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				t.Fatalf("decode output: %v; output = %s", err, output.String())
			}
			if result.Result != "fatal_failure" || result.ExitCode != 1 || result.ScopeCompleted {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestBackfillRelayExitCodesCoverSuccessPartialAndFatalResults(t *testing.T) {
	legacy := commandOutbox(t, map[string]any{
		"PK": "TENANT#private_tenant", "SK": "NOTIFICATION_OUTBOX#private_outbox",
		"entity_type": "notification_outbox", "tenant_id": "private_tenant", "outbox_id": "private_outbox",
		"channel": "email", "status": "ready", "created_at": "2026-07-15T12:00:45.000000000Z",
	})
	conflictValues := map[string]any{
		"PK": "TENANT#private_tenant", "SK": "NOTIFICATION_OUTBOX#private_outbox",
		"entity_type": "notification_outbox", "tenant_id": "private_tenant", "outbox_id": "private_outbox",
		"channel": "email", "status": "ready", "created_at": "2026-07-15T12:00:45.000000000Z",
		"relay_schema_version": 2,
	}
	conflict := commandOutbox(t, conflictValues)
	tests := []struct {
		name             string
		args             []string
		client           *fakeDynamo
		loadError        error
		lookupEnv        func(string) (string, bool)
		wantExit         int
		wantResult       string
		wantUpdates      int
		wantCategory     string
		wantCategoryOnly bool
		wantSummary      bool
	}{
		{
			name: "dry-run success", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client:   &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{legacy}}},
			wantExit: 0, wantResult: "success", wantSummary: true,
		},
		{
			name: "apply success", args: []string{"backfill-relay", "--tenant", "private_tenant", "--apply"},
			client:   &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{legacy}}},
			wantExit: 0, wantResult: "success", wantUpdates: 1, wantSummary: true,
		},
		{
			name: "schema conflict", args: []string{"backfill-relay", "--tenant", "private_tenant", "--apply"},
			client:   &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{conflict}}},
			wantExit: 2, wantResult: "partial_failure", wantCategory: "schema_conflict", wantCategoryOnly: true, wantSummary: true,
		},
		{
			name: "concurrent change", args: []string{"backfill-relay", "--tenant", "private_tenant", "--apply"},
			client: &fakeDynamo{
				queryOutput:  &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{legacy}},
				updateErrors: []error{&types.ConditionalCheckFailedException{Message: aws.String("private race")}},
			},
			wantExit: 2, wantResult: "partial_failure", wantUpdates: 1,
			wantCategory: "concurrent_change", wantCategoryOnly: true, wantSummary: true,
		},
		{
			name: "query failure", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client:   &fakeDynamo{queryError: errors.New("private query detail")},
			wantExit: 1, wantResult: "fatal_failure", wantCategory: "query_failure", wantCategoryOnly: true, wantSummary: true,
		},
		{
			name: "AWS load failure", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client: &fakeDynamo{}, loadError: errors.New("private credentials detail"),
			wantExit: 1, wantResult: "fatal_failure", wantCategory: "aws_configuration", wantCategoryOnly: true,
		},
		{
			name: "empty table configuration", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client: &fakeDynamo{},
			lookupEnv: func(key string) (string, bool) {
				if key == "DYNAMODB_DOMAIN_TABLE" {
					return "", true
				}
				return "", false
			},
			wantExit: 1, wantResult: "fatal_failure", wantCategory: "configuration", wantCategoryOnly: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			lookupEnv := test.lookupEnv
			if lookupEnv == nil {
				lookupEnv = func(string) (string, bool) { return "", false }
			}
			exitCode := runMain(context.Background(), test.args, dependencies{
				Output: &output, LookupEnv: lookupEnv,
				LoadDynamo: func(context.Context, string, string) (notificationdynamo.Client, error) {
					if test.loadError != nil {
						return nil, test.loadError
					}
					return test.client, nil
				},
			})
			if exitCode != test.wantExit {
				t.Fatalf("exit code = %d, want %d; output = %s", exitCode, test.wantExit, output.String())
			}
			if len(test.client.updateInputs) != test.wantUpdates {
				t.Fatalf("UpdateItem calls = %d, want %d", len(test.client.updateInputs), test.wantUpdates)
			}
			for _, sensitive := range []string{"private_tenant", "private_outbox", "private query detail", "private credentials detail", "private race"} {
				if strings.Contains(output.String(), sensitive) {
					t.Fatalf("JSON contains sensitive value %q: %s", sensitive, output.String())
				}
			}
			var result commandResult
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				t.Fatalf("decode output: %v; output = %s", err, output.String())
			}
			if result.Result != test.wantResult || result.ExitCode != test.wantExit {
				t.Fatalf("result = %#v", result)
			}
			if test.wantCategory != "" && result.ErrorCategories[test.wantCategory] != 1 {
				t.Fatalf("error categories = %#v", result.ErrorCategories)
			}
			if test.wantCategoryOnly && len(result.ErrorCategories) != 1 {
				t.Fatalf("error categories = %#v, want one nonzero category", result.ErrorCategories)
			}
			if (result.Summary != nil) != test.wantSummary {
				t.Fatalf("summary presence = %v, want %v; result = %#v", result.Summary != nil, test.wantSummary, result)
			}
		})
	}
}

func commandOutbox(t *testing.T, value map[string]any) map[string]types.AttributeValue {
	t.Helper()
	item, err := attributevalue.MarshalMap(value)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
