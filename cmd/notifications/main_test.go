package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	notificationdynamo "github.com/VINIClUS/limnopulse/internal/notifications/dynamo"
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
