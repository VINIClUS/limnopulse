package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	alertdynamo "github.com/VINIClUS/limnopulse/internal/alertevaluator/dynamo"
	notificationdynamo "github.com/VINIClUS/limnopulse/internal/notifications/dynamo"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

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
	if value, ok := input.ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS); ok &&
		strings.HasPrefix(value.Value, "NOTIFICATION_OUTBOX#") {
		return &awssdk.QueryOutput{}, nil
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

func (client *fakeDynamo) GetItem(context.Context, *awssdk.GetItemInput, ...func(*awssdk.Options)) (*awssdk.GetItemOutput, error) {
	return nil, errors.New("no backfill reads single items")
}

func (client *fakeDynamo) TransactWriteItems(context.Context, *awssdk.TransactWriteItemsInput, ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error) {
	return nil, errors.New("no backfill writes transactions")
}

func noEnvironment(string) (string, bool) { return "", false }

// fakeDependencies serves client to every command and counts how often one connected.
func fakeDependencies(output *bytes.Buffer, client *fakeDynamo, loadCalls *int) dependencies {
	return dependencies{
		Output: output, LookupEnv: noEnvironment,
		LoadDynamo: func(context.Context, workercmd.AWSLoader, string, string) (dynamoClient, error) {
			*loadCalls++
			return client, nil
		},
	}
}

type decodedResult[S any] struct {
	Result           string         `json:"result"`
	ExitCode         int            `json:"exit_code"`
	ScopeCompleted   bool           `json:"scope_completed"`
	RetryRecommended bool           `json:"retry_recommended"`
	ErrorCategories  map[string]int `json:"error_categories"`
	Settings         []string       `json:"settings"`
	Summary          *S             `json:"summary"`
}

func decode[S any](t *testing.T, output *bytes.Buffer) decodedResult[S] {
	t.Helper()
	var result decodedResult[S]
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("decode output: %v; output = %s", err, output.String())
	}
	return result
}

func item(t *testing.T, value map[string]any) map[string]types.AttributeValue {
	t.Helper()
	encoded, err := attributevalue.MarshalMap(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func unmarshalValues(t *testing.T, values map[string]types.AttributeValue) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := attributevalue.UnmarshalMap(values, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestAdminExits78OnAMissingOrUnknownCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"run"}, {"backfill"}, {"worker"}} {
		var output bytes.Buffer
		loadCalls := 0
		code := runMain(context.Background(), args, fakeDependencies(&output, &fakeDynamo{}, &loadCalls))
		result := decode[struct{}](t, &output)
		if code != workerproc.ExitConfiguration || loadCalls != 0 || result.ExitCode != code ||
			result.RetryRecommended || result.ErrorCategories["usage"] != 1 {
			t.Fatalf("args %v: exit=%d loads=%d result=%+v", args, code, loadCalls, result)
		}
	}
}

func TestAdminExits78WithoutCredentialsUnderSystemd(t *testing.T) {
	var output bytes.Buffer
	loadCalls := 0
	deps := fakeDependencies(&output, &fakeDynamo{}, &loadCalls)
	deps.LookupEnv = func(key string) (string, bool) {
		if key == workerproc.SupervisorEnv {
			return "systemd", true
		}
		return "", false
	}
	code := runMain(context.Background(), []string{"backfill-relay", "--tenant", "private_tenant"}, deps)
	result := decode[struct{}](t, &output)
	if code != workerproc.ExitConfiguration || loadCalls != 0 || result.ErrorCategories["credentials"] != 1 {
		t.Fatalf("exit=%d loads=%d result=%+v", code, loadCalls, result)
	}
}

func TestLoadDynamoBuildsTheClientFromTheAWSLoader(t *testing.T) {
	var gotRegion, gotEndpoint string
	client, err := loadDynamo(context.Background(), func(_ context.Context, region, endpoint string) (aws.Config, error) {
		gotRegion, gotEndpoint = region, endpoint
		return aws.Config{Region: region}, nil
	}, "eu-west-1", "http://dynamodb:8000")
	if err != nil || client == nil || gotRegion != "eu-west-1" || gotEndpoint != "http://dynamodb:8000" {
		t.Fatalf("client=%v err=%v region=%q endpoint=%q", client, err, gotRegion, gotEndpoint)
	}
	if _, err := loadDynamo(context.Background(), func(context.Context, string, string) (aws.Config, error) {
		return aws.Config{}, errors.New("no profile")
	}, "eu-west-1", ""); err == nil {
		t.Fatal("expected the AWS load error")
	}
}

func TestBackfillRelayResolvesTenantFlagsAndFilesInFirstSeenOrderBeforeAWS(t *testing.T) {
	tenantFile := filepath.Join(t.TempDir(), "tenants.txt")
	if err := os.WriteFile(tenantFile, []byte(" tnt_2 \n\ntnt_1\ntnt_3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &fakeDynamo{}
	loadCalls := 0
	var output bytes.Buffer
	exitCode := runMain(context.Background(), []string{
		"backfill-relay",
		"--tenant", " tnt_1 ",
		"--tenant-file", tenantFile,
		"--tenant", "tnt_4",
		"--tenant", "tnt_2",
	}, fakeDependencies(&output, client, &loadCalls))
	if exitCode != 0 || loadCalls != 1 || len(client.queryInputs) != 4 {
		t.Fatalf("exit=%d loads=%d queries=%d output=%s", exitCode, loadCalls, len(client.queryInputs), output.String())
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
	result := decode[notificationdynamo.BackfillSummary](t, &output)
	if result.Result != "success" || result.ExitCode != 0 || result.Summary == nil || result.Summary.Tenants != 4 {
		t.Fatalf("result = %#v", result)
	}
}

func TestBackfillTelegramUsesExplicitTenantQueryAndEnablesV2Relay(t *testing.T) {
	client := &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{
		item(t, map[string]any{
			"PK": "TENANT#private_tenant", "SK": "NOTIFICATION_OUTBOX#private_outbox",
			"entity_type": "notification_outbox", "tenant_id": "private_tenant", "outbox_id": "private_outbox",
			"channel": "telegram", "kind": "opening", "status": "ready",
			"created_at":       "2026-07-15T12:00:45.000000000Z",
			"expansion_status": "deferred_unsupported_channel",
		}),
	}}}
	var output bytes.Buffer
	loadCalls := 0
	exit := runMain(context.Background(), []string{
		"backfill-telegram", "--tenant", "private_tenant", "--apply",
	}, fakeDependencies(&output, client, &loadCalls))
	if exit != 0 || len(client.queryInputs) != 1 || len(client.updateInputs) != 1 {
		t.Fatalf("exit=%d queries=%d updates=%d output=%s", exit, len(client.queryInputs), len(client.updateInputs), output.String())
	}
	values := unmarshalValues(t, client.updateInputs[0].ExpressionAttributeValues)
	if fmt.Sprint(values[":relay_schema_version"]) != "2" || values[":expansion_status"] != "pending" ||
		strings.Contains(output.String(), "private_") {
		t.Fatalf("values=%#v output=%s", values, output.String())
	}
}

func TestBackfillRelayExits78OnInvalidScopeOrFlagsBeforeAWS(t *testing.T) {
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
		category  string
		sensitive string
		setting   string
	}{
		{name: "no tenant", args: []string{"backfill-relay"}, category: "tenant_scope"},
		{name: "empty effective file", args: []string{"backfill-relay", "--tenant-file", emptyFile}, category: "tenant_scope", sensitive: emptyFile},
		{name: "missing file", args: []string{"backfill-relay", "--tenant-file", missingFile}, category: "tenant_scope", sensitive: missingFile, setting: "tenant-file"},
		{name: "NUL file", args: []string{"backfill-relay", "--tenant-file", nulFile}, category: "tenant_scope", sensitive: nulFile},
		{name: "blank file path", args: []string{"backfill-relay", "--tenant-file", " "}, category: "tenant_scope"},
		{name: "empty flag", args: []string{"backfill-relay", "--tenant", "  "}, category: "tenant_scope"},
		{name: "NUL flag", args: []string{"backfill-telegram", "--tenant", "private\x00tenant"}, category: "tenant_scope", sensitive: "private"},
		{name: "zero page size", args: []string{"backfill-relay", "--tenant", "private_tenant", "--page-size", "0"}, category: "configuration", sensitive: "private_tenant", setting: "page-size"},
		{name: "oversized page", args: []string{"backfill-relay", "--tenant", "private_tenant", "--page-size", "2147483648"}, category: "configuration", sensitive: "private_tenant"},
		{name: "zero max rows", args: []string{"backfill-relay", "--tenant", "private_tenant", "--max-rows", "0"}, category: "configuration", sensitive: "private_tenant"},
		{name: "zero timeout", args: []string{"backfill-relay", "--tenant", "private_tenant", "--timeout", "0s"}, category: "configuration", sensitive: "private_tenant"},
		{name: "long timeout", args: []string{"backfill-relay", "--tenant", "private_tenant", "--timeout", "25h"}, category: "configuration", sensitive: "private_tenant"},
		{name: "positional argument", args: []string{"backfill-relay", "--tenant", "private_tenant", "extra"}, category: "configuration", sensitive: "private_tenant"},
		{name: "unknown flag", args: []string{"backfill-relay", "--tenant", "private_tenant", "--tenants=x"}, category: "configuration", sensitive: "private_tenant", setting: "tenants"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			loadCalls := 0
			var output bytes.Buffer
			exitCode := runMain(context.Background(), test.args, fakeDependencies(&output, &fakeDynamo{}, &loadCalls))
			if exitCode != workerproc.ExitConfiguration || loadCalls != 0 {
				t.Fatalf("exit=%d loads=%d output=%s", exitCode, loadCalls, output.String())
			}
			if test.sensitive != "" && strings.Contains(output.String(), test.sensitive) {
				t.Fatalf("JSON contains sensitive input %q: %s", test.sensitive, output.String())
			}
			result := decode[notificationdynamo.BackfillSummary](t, &output)
			if result.Result != "fatal_failure" || result.ExitCode != workerproc.ExitConfiguration || result.ScopeCompleted ||
				result.RetryRecommended || result.ErrorCategories[test.category] != 1 ||
				(test.setting != "" && !reflect.DeepEqual(result.Settings, []string{test.setting})) {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestBackfillRelayReportsIncompleteScopeAtTotalRowLimit(t *testing.T) {
	row := func(id string) map[string]types.AttributeValue {
		return item(t, map[string]any{
			"PK": "TENANT#private_tenant", "SK": "NOTIFICATION_OUTBOX#" + id,
			"entity_type": "notification_outbox", "tenant_id": "private_tenant", "outbox_id": id,
			"channel": "email", "kind": "opening", "status": "ready",
			"created_at": "2026-07-15T12:00:45.000000000Z",
		})
	}
	client := &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{row("private_1"), row("private_2")}}}
	var output bytes.Buffer
	loadCalls := 0
	exitCode := runMain(context.Background(), []string{
		"backfill-relay", "--tenant", "private_tenant", "--max-rows", "1",
	}, fakeDependencies(&output, client, &loadCalls))
	result := decode[notificationdynamo.BackfillSummary](t, &output)
	if exitCode != workerproc.ExitFailure || result.Result != "incomplete" || result.ScopeCompleted || !result.RetryRecommended ||
		result.ErrorCategories["row_limit_reached"] != 1 || result.Summary == nil ||
		!result.Summary.LimitReached || result.Summary.RowsQueried != 1 {
		t.Fatalf("exit=%d result = %#v", exitCode, result)
	}
	if strings.Contains(output.String(), "private_") {
		t.Fatalf("bounded summary leaked identifiers: %s", output.String())
	}
}

func TestBackfillRelayReportsIncompleteScopeAtDeadline(t *testing.T) {
	client := &fakeDynamo{queryError: context.DeadlineExceeded}
	var output bytes.Buffer
	loadCalls := 0
	exitCode := runMain(context.Background(), []string{
		"backfill-relay", "--tenant", "private_tenant",
	}, fakeDependencies(&output, client, &loadCalls))
	result := decode[notificationdynamo.BackfillSummary](t, &output)
	if exitCode != workerproc.ExitFailure || result.Result != "incomplete" || result.ScopeCompleted || !result.RetryRecommended ||
		result.ErrorCategories["deadline_reached"] != 1 || result.Summary == nil ||
		!result.Summary.DeadlineReached || result.Summary.RowFailures != 0 {
		t.Fatalf("exit=%d result = %#v", exitCode, result)
	}
}

func TestBackfillRelayExitCodesCoverSuccessPartialAndFatalResults(t *testing.T) {
	outbox := map[string]any{
		"PK": "TENANT#private_tenant", "SK": "NOTIFICATION_OUTBOX#private_outbox",
		"entity_type": "notification_outbox", "tenant_id": "private_tenant", "outbox_id": "private_outbox",
		"channel": "email", "kind": "opening", "status": "ready",
		"created_at": "2026-07-15T12:00:45.000000000Z",
	}
	legacy := item(t, outbox)
	conflictValues := map[string]any{"relay_schema_version": 2}
	for key, value := range outbox {
		conflictValues[key] = value
	}
	conflict := item(t, conflictValues)
	tests := []struct {
		name         string
		args         []string
		client       *fakeDynamo
		loadError    error
		lookupEnv    func(string) (string, bool)
		wantExit     int
		wantResult   string
		wantUpdates  int
		wantCategory string
		wantSummary  bool
		wantSettings []string
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
			wantExit: workerproc.ExitPartial, wantResult: "partial_failure", wantCategory: "schema_conflict", wantSummary: true,
		},
		{
			name: "concurrent change", args: []string{"backfill-relay", "--tenant", "private_tenant", "--apply"},
			client: &fakeDynamo{
				queryOutput:  &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{legacy}},
				updateErrors: []error{&types.ConditionalCheckFailedException{Message: aws.String("private race")}},
			},
			wantExit: workerproc.ExitPartial, wantResult: "partial_failure", wantUpdates: 1,
			wantCategory: "concurrent_change", wantSummary: true,
		},
		{
			name: "query failure", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client:   &fakeDynamo{queryError: errors.New("private query detail")},
			wantExit: workerproc.ExitFailure, wantResult: "fatal_failure", wantCategory: "query_failure", wantSummary: true,
		},
		{
			name: "AWS load failure", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client: &fakeDynamo{}, loadError: errors.New("private credentials detail"),
			wantExit: workerproc.ExitFailure, wantResult: "fatal_failure", wantCategory: "aws_configuration",
		},
		{
			name: "empty table configuration", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client:    &fakeDynamo{},
			lookupEnv: emptySetting("DYNAMODB_DOMAIN_TABLE"),
			wantExit:  workerproc.ExitConfiguration, wantResult: "fatal_failure", wantCategory: "configuration",
			wantSettings: []string{"DYNAMODB_DOMAIN_TABLE"},
		},
		{
			name: "empty region configuration", args: []string{"backfill-relay", "--tenant", "private_tenant"},
			client:    &fakeDynamo{},
			lookupEnv: emptySetting("AWS_REGION"),
			wantExit:  workerproc.ExitConfiguration, wantResult: "fatal_failure", wantCategory: "configuration",
			wantSettings: []string{"AWS_REGION"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			lookupEnv := test.lookupEnv
			if lookupEnv == nil {
				lookupEnv = noEnvironment
			}
			exitCode := runMain(context.Background(), test.args, dependencies{
				Output: &output, LookupEnv: lookupEnv,
				LoadDynamo: func(context.Context, workercmd.AWSLoader, string, string) (dynamoClient, error) {
					if test.loadError != nil {
						return nil, test.loadError
					}
					return test.client, nil
				},
			})
			if exitCode != test.wantExit || len(test.client.updateInputs) != test.wantUpdates {
				t.Fatalf("exit=%d want %d; updates=%d want %d; output=%s",
					exitCode, test.wantExit, len(test.client.updateInputs), test.wantUpdates, output.String())
			}
			for _, sensitive := range []string{"private_tenant", "private_outbox", "private query detail", "private credentials detail", "private race"} {
				if strings.Contains(output.String(), sensitive) {
					t.Fatalf("JSON contains sensitive value %q: %s", sensitive, output.String())
				}
			}
			result := decode[notificationdynamo.BackfillSummary](t, &output)
			if result.Result != test.wantResult || result.ExitCode != test.wantExit {
				t.Fatalf("result = %#v", result)
			}
			if test.wantCategory != "" && (result.ErrorCategories[test.wantCategory] != 1 || len(result.ErrorCategories) != 1) {
				t.Fatalf("error categories = %#v, want only %s", result.ErrorCategories, test.wantCategory)
			}
			if (result.Summary != nil) != test.wantSummary {
				t.Fatalf("summary presence = %v, want %v; result = %#v", result.Summary != nil, test.wantSummary, result)
			}
			if !reflect.DeepEqual(result.Settings, test.wantSettings) {
				t.Fatalf("settings = %#v, want %#v", result.Settings, test.wantSettings)
			}
		})
	}
}

func alertRule(t *testing.T, id string, enabled bool) map[string]types.AttributeValue {
	return item(t, map[string]any{
		"PK": "TENANT#private_tenant", "SK": "ALERT_RULE#" + id, "rule_id": id,
		"enabled": enabled, "status": "active",
	})
}

func TestBackfillScheduleCountsDryRunAndWritesOnApply(t *testing.T) {
	for _, apply := range []bool{false, true} {
		client := &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{
			alertRule(t, "private_rule", true), alertRule(t, "private_disabled", false),
		}}}
		args := []string{"backfill-schedule", "--tenant=private_tenant", "--evaluation-time=2026-07-15T12:00:00Z"}
		if apply {
			args = append(args, "--apply")
		}
		var output bytes.Buffer
		loadCalls := 0
		code := runMain(context.Background(), args, fakeDependencies(&output, client, &loadCalls))
		result := decode[alertdynamo.BackfillSummary](t, &output)
		wantUpdates := 0
		if apply {
			wantUpdates = 1
		}
		if code != 0 || result.Result != "success" || !result.ScopeCompleted || result.Summary == nil ||
			result.Summary.Queried != 2 || result.Summary.Eligible != 1 || result.Summary.Updated != wantUpdates ||
			len(client.updateInputs) != wantUpdates || result.Summary.DryRun == apply ||
			!result.Summary.EvaluationTime.Equal(time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)) {
			t.Fatalf("apply=%t exit=%d result=%+v summary=%+v", apply, code, result, result.Summary)
		}
		if strings.Contains(output.String(), "private_") {
			t.Fatalf("summary leaked identifiers: %s", output.String())
		}
	}
}

func TestBackfillScheduleReportsAQueryFailureWithoutItsDetail(t *testing.T) {
	var output bytes.Buffer
	loadCalls := 0
	code := runMain(context.Background(), []string{"backfill-schedule", "--tenant=private_tenant"},
		fakeDependencies(&output, &fakeDynamo{queryError: errors.New("private query detail")}, &loadCalls))
	result := decode[alertdynamo.BackfillSummary](t, &output)
	if code != workerproc.ExitFailure || result.Result != "fatal_failure" || !result.RetryRecommended ||
		result.ErrorCategories["query_failure"] != 1 || strings.Contains(output.String(), "private_") ||
		strings.Contains(output.String(), "private query detail") {
		t.Fatalf("exit=%d output=%s", code, output.String())
	}
}

func TestAlertBackfillsExit78OnInvalidScopeOrFlagsBeforeAWS(t *testing.T) {
	for _, test := range []struct {
		args     []string
		category string
	}{
		{args: []string{"backfill-schedule"}, category: "tenant_scope"},
		{args: []string{"backfill-schedule", "--tenant= "}, category: "tenant_scope"},
		{args: []string{"backfill-schedule", "--tenant=a", "--evaluation-time=yesterday"}, category: "configuration"},
		{args: []string{"backfill-schedule", "--tenant=a", "--page-size=0"}, category: "configuration"},
		{args: []string{"backfill-schedule", "--tenant=a", "--tenant-file=tenants.txt"}, category: "configuration"},
		{args: []string{"backfill-schedule", "--tenant=a", "extra"}, category: "configuration"},
		{args: []string{"backfill-active-alert-index"}, category: "tenant_scope"},
		{args: []string{"backfill-active-alert-index", "--tenant-id=a", "--limit=-1"}, category: "configuration"},
		{args: []string{"backfill-active-alert-index", "--tenant-id=a", "--page-size=0"}, category: "configuration"},
	} {
		var output bytes.Buffer
		loadCalls := 0
		code := runMain(context.Background(), test.args, fakeDependencies(&output, &fakeDynamo{}, &loadCalls))
		result := decode[alertdynamo.BackfillSummary](t, &output)
		if code != workerproc.ExitConfiguration || loadCalls != 0 || result.ErrorCategories[test.category] != 1 {
			t.Fatalf("args %v: exit=%d loads=%d result=%+v", test.args, code, loadCalls, result)
		}
	}
}

func activeEvent(t *testing.T, id string) map[string]types.AttributeValue {
	return item(t, map[string]any{
		"PK": "TENANT#private_tenant", "SK": "ALERT_EVENT#" + id, "event_id": id,
		"tenant_id": "private_tenant", "status": "open", "opened_at": "2026-07-15T12:00:00.000000000Z",
	})
}

func TestBackfillActiveAlertIndexAcceptsTheTenantIDAliasAndApplies(t *testing.T) {
	client := &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{
		activeEvent(t, "private_1"), activeEvent(t, "private_2"),
	}}}
	var output bytes.Buffer
	loadCalls := 0
	code := runMain(context.Background(), []string{"backfill-active-alert-index", "--tenant-id", "private_tenant", "--apply"},
		fakeDependencies(&output, client, &loadCalls))
	result := decode[alertdynamo.BackfillSummary](t, &output)
	if code != 0 || !result.ScopeCompleted || result.Summary == nil || result.Summary.AlertEventsUpdated != 2 ||
		len(client.updateInputs) != 2 || strings.Contains(output.String(), "private_") {
		t.Fatalf("exit=%d result=%+v output=%s", code, result, output.String())
	}
}

func TestBackfillActiveAlertIndexReportsALimitedBatchAsNotCompleted(t *testing.T) {
	client := &fakeDynamo{queryOutput: &awssdk.QueryOutput{Items: []map[string]types.AttributeValue{
		activeEvent(t, "private_1"), activeEvent(t, "private_2"),
	}}}
	var output bytes.Buffer
	loadCalls := 0
	code := runMain(context.Background(), []string{"backfill-active-alert-index", "--tenant=private_tenant", "--limit=1"},
		fakeDependencies(&output, client, &loadCalls))
	result := decode[alertdynamo.BackfillSummary](t, &output)
	if code != 0 || result.Result != "success" || result.ScopeCompleted || result.Summary == nil ||
		result.Summary.AlertEventsEligible != 1 || len(client.updateInputs) != 0 {
		t.Fatalf("exit=%d result=%+v", code, result)
	}
}

func TestAdminPrintsACommandsUsageAndExits0OnHelp(t *testing.T) {
	for _, test := range []struct{ command, flag string }{
		{"backfill-relay", "-tenant-file"},
		{"backfill-telegram", "-max-rows"},
		{"backfill-schedule", "-evaluation-time"},
		{"backfill-active-alert-index", "-tenant-id"},
	} {
		var output bytes.Buffer
		loadCalls := 0
		code := runMain(context.Background(), []string{test.command, "-h"}, fakeDependencies(&output, &fakeDynamo{}, &loadCalls))
		if code != 0 || loadCalls != 0 || !strings.Contains(output.String(), test.flag) ||
			!strings.HasPrefix(output.String(), "Usage of limnopulse-admin "+test.command+":") {
			t.Fatalf("%s: exit=%d loads=%d output=%s", test.command, code, loadCalls, output.String())
		}
	}
}

// emptySetting is an environment that sets key, and only key, to the empty string.
func emptySetting(key string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		return "", name == key
	}
}
