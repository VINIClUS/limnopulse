package dynamo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeClient struct {
	queryOutputs []*awssdk.QueryOutput
	queryErrors  []error
	queryInputs  []*awssdk.QueryInput
	updateErrors []error
	updateInputs []*awssdk.UpdateItemInput
	scanCalls    int
}

func (client *fakeClient) Query(
	_ context.Context,
	input *awssdk.QueryInput,
	_ ...func(*awssdk.Options),
) (*awssdk.QueryOutput, error) {
	client.queryInputs = append(client.queryInputs, input)
	if len(client.queryErrors) > 0 {
		err := client.queryErrors[0]
		client.queryErrors = client.queryErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	output := client.queryOutputs[0]
	client.queryOutputs = client.queryOutputs[1:]
	return output, nil
}

func (client *fakeClient) UpdateItem(
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

func (client *fakeClient) Scan(
	_ context.Context,
	_ *awssdk.ScanInput,
	_ ...func(*awssdk.Options),
) (*awssdk.ScanOutput, error) {
	client.scanCalls++
	return &awssdk.ScanOutput{}, nil
}

func TestBackfillRelayDryRunQueriesTenantOutboxesAndPaginatesWithoutScan(t *testing.T) {
	lastKey := mustMarshalMap(t, map[string]any{
		"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_OUTBOX#outbox_email",
	})
	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{
		{
			Items:            []map[string]types.AttributeValue{mustMarshalMap(t, legacyOutbox("outbox_email", "email", "ready"))},
			LastEvaluatedKey: lastKey,
		},
		{
			Items: []map[string]types.AttributeValue{mustMarshalMap(t, legacyOutbox("outbox_telegram", "telegram", "ready"))},
		},
	}}
	store := Store{Table: "domain", Client: client}

	summary, err := store.BackfillRelay(context.Background(), BackfillOptions{
		Tenants: []string{"tnt_1"}, PageSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !summary.DryRun || summary.Tenants != 1 || summary.RowsQueried != 2 ||
		summary.RowsNeedingUpdate != 2 || summary.WouldUpdate != 2 ||
		summary.Updated != 0 || summary.RowFailures != 0 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(client.queryInputs) != 2 {
		t.Fatalf("Query calls = %d, want 2", len(client.queryInputs))
	}
	for _, input := range client.queryInputs {
		if input.IndexName != nil || input.KeyConditionExpression == nil ||
			!strings.Contains(*input.KeyConditionExpression, "begins_with(SK, :prefix)") ||
			input.Limit == nil || *input.Limit != 1 {
			t.Fatalf("query input = %#v", input)
		}
	}
	if client.queryInputs[0].ExclusiveStartKey != nil {
		t.Fatalf("first ExclusiveStartKey = %#v", client.queryInputs[0].ExclusiveStartKey)
	}
	if !reflect.DeepEqual(client.queryInputs[1].ExclusiveStartKey, lastKey) {
		t.Fatalf("second ExclusiveStartKey = %#v, want %#v", client.queryInputs[1].ExclusiveStartKey, lastKey)
	}
	if len(client.updateInputs) != 0 {
		t.Fatalf("dry-run made %d UpdateItem calls", len(client.updateInputs))
	}
	if client.scanCalls != 0 {
		t.Fatalf("Scan calls = %d, want 0", client.scanCalls)
	}
}

func TestBackfillRelayStopsAtExplicitTotalRowLimit(t *testing.T) {
	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{{
		Items: []map[string]types.AttributeValue{
			mustMarshalMap(t, legacyOutbox("outbox_1", "email", "ready")),
			mustMarshalMap(t, legacyOutbox("outbox_2", "email", "ready")),
		},
	}}}

	summary, err := (Store{Table: "domain", Client: client}).BackfillRelay(
		context.Background(),
		BackfillOptions{Tenants: []string{"tnt_1"}, PageSize: 25, MaxRows: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.LimitReached || summary.DeadlineReached || summary.RowsQueried != 1 ||
		summary.RowsNeedingUpdate != 1 || summary.WouldUpdate != 1 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(client.queryInputs) != 1 {
		t.Fatalf("Query calls = %d, want 1", len(client.queryInputs))
	}
	if client.queryInputs[0].Limit == nil || *client.queryInputs[0].Limit != 1 {
		t.Fatalf("bounded Query limit = %#v, want 1", client.queryInputs[0].Limit)
	}
}

func TestBackfillRelayStopsCleanlyWhenDeadlineIsAlreadyReached(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	client := &fakeClient{}

	summary, err := (Store{Table: "domain", Client: client}).BackfillRelay(
		ctx,
		BackfillOptions{Tenants: []string{"tnt_1"}, PageSize: 25, MaxRows: 100},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.DeadlineReached || summary.LimitReached || summary.RowsQueried != 0 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(client.queryInputs) != 0 {
		t.Fatalf("deadline run made %d Query calls", len(client.queryInputs))
	}
}

func TestBackfillRelayApplyWritesCanonicalEmailAndTelegramFieldsIdempotently(t *testing.T) {
	readyEmail := legacyOutbox("outbox_ready", "email", "ready")
	blockedEmail := legacyOutbox("outbox_blocked", "email", "blocked")
	blockedEmail["kind"] = "recovery"
	blockedEmail["depends_on_outbox_id"] = "outbox_opening"
	telegram := legacyOutbox("outbox_telegram", "telegram", "ready")

	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
		mustMarshalMap(t, readyEmail), mustMarshalMap(t, blockedEmail), mustMarshalMap(t, telegram),
	}}}}
	summary, err := (Store{Table: "domain", Client: client}).BackfillRelay(
		context.Background(),
		BackfillOptions{Tenants: []string{"tnt_1"}, Apply: true, PageSize: 25},
	)
	if err != nil {
		t.Fatal(err)
	}
	if summary.DryRun || summary.RowsNeedingUpdate != 3 || summary.Updated != 3 ||
		summary.WouldUpdate != 0 || summary.RowFailures != 0 {
		t.Fatalf("apply summary = %#v", summary)
	}
	if len(client.updateInputs) != 3 {
		t.Fatalf("UpdateItem calls = %d, want 3", len(client.updateInputs))
	}

	createdAt, err := time.Parse("2006-01-02T15:04:05.000000000Z", readyEmail["created_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	for index, wantKind := range []notifications.WorkKind{
		notifications.WorkKindIntent,
		notifications.WorkKindDependency,
	} {
		input := client.updateInputs[index]
		values := unmarshalValues(t, input.ExpressionAttributeValues)
		itemID := []string{"outbox_ready", "outbox_blocked"}[index]
		wantKey, err := notifications.BuildRelayIndexKey(wantKind, "tnt_1", itemID, createdAt)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(values[":relay_schema_version"]) != "1" ||
			values[":expansion_status"] != "pending" ||
			values[":available_at"] != readyEmail["created_at"] ||
			values[":relay_work_kind"] != string(wantKind) ||
			values[":relay_gsi_pk"] != wantKey.PartitionKey ||
			values[":relay_gsi_sk"] != wantKey.SortKey {
			t.Fatalf("email update %d values = %#v", index, values)
		}
		if input.ConditionExpression == nil ||
			!strings.Contains(*input.ConditionExpression, "attribute_not_exists(#relay_schema_version)") ||
			!strings.Contains(*input.ConditionExpression, "#created_at = :expected_created_at") {
			t.Fatalf("email update %d condition = %#v", index, input.ConditionExpression)
		}
	}

	telegramUpdate := client.updateInputs[2]
	telegramValues := unmarshalValues(t, telegramUpdate.ExpressionAttributeValues)
	if telegramValues[":expansion_status"] != "deferred_unsupported_channel" {
		t.Fatalf("telegram update values = %#v", telegramValues)
	}
	for _, field := range []string{"#relay_schema_version", "#available_at", "#relay_work_kind", "#relay_gsi_pk", "#relay_gsi_sk"} {
		if strings.Contains(*telegramUpdate.UpdateExpression, field+" =") {
			t.Fatalf("telegram update writes %s: %s", field, *telegramUpdate.UpdateExpression)
		}
	}

	canonicalReady := canonicalEmailOutbox(t, readyEmail, notifications.WorkKindIntent)
	canonicalBlocked := canonicalEmailOutbox(t, blockedEmail, notifications.WorkKindDependency)
	canonicalTelegram := cloneMap(telegram)
	canonicalTelegram["expansion_status"] = "deferred_unsupported_channel"
	secondClient := &fakeClient{queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
		mustMarshalMap(t, canonicalReady), mustMarshalMap(t, canonicalBlocked), mustMarshalMap(t, canonicalTelegram),
	}}}}
	second, err := (Store{Table: "domain", Client: secondClient}).BackfillRelay(
		context.Background(),
		BackfillOptions{Tenants: []string{"tnt_1"}, Apply: true, PageSize: 25},
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.Noop != 3 || second.RowsNeedingUpdate != 0 || second.Updated != 0 || second.RowFailures != 0 {
		t.Fatalf("idempotent summary = %#v", second)
	}
	if len(secondClient.updateInputs) != 0 {
		t.Fatalf("idempotent run made %d updates", len(secondClient.updateInputs))
	}
}

func TestBackfillRelayUpgradesOnlyMissingWorkKindOnCanonicalV1EmailRows(t *testing.T) {
	ready := canonicalEmailOutbox(
		t, legacyOutbox("outbox_ready", "email", "ready"), notifications.WorkKindIntent,
	)
	blockedLegacy := legacyOutbox("outbox_blocked", "email", "blocked")
	blockedLegacy["kind"] = "recovery"
	blockedLegacy["depends_on_outbox_id"] = "outbox_opening"
	blocked := canonicalEmailOutbox(t, blockedLegacy, notifications.WorkKindDependency)
	delete(ready, "relay_work_kind")
	delete(blocked, "relay_work_kind")
	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
		mustMarshalMap(t, ready), mustMarshalMap(t, blocked),
	}}}}

	summary, err := (Store{Table: "domain", Client: client}).BackfillRelay(
		context.Background(),
		BackfillOptions{Tenants: []string{"tnt_1"}, Apply: true, PageSize: 25},
	)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RowsNeedingUpdate != 2 || summary.Updated != 2 ||
		summary.SchemaConflicts != 0 || summary.RowFailures != 0 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(client.updateInputs) != 2 {
		t.Fatalf("UpdateItem calls = %d", len(client.updateInputs))
	}
	for index, wantKind := range []notifications.WorkKind{
		notifications.WorkKindIntent, notifications.WorkKindDependency,
	} {
		input := client.updateInputs[index]
		if aws.ToString(input.UpdateExpression) != "SET #relay_work_kind = :relay_work_kind" {
			t.Fatalf("marker-only update = %s", aws.ToString(input.UpdateExpression))
		}
		condition := aws.ToString(input.ConditionExpression)
		for _, fragment := range []string{
			"#relay_schema_version = :expected_relay_schema_version",
			"#expansion_status = :expected_expansion_status",
			"#available_at = :expected_available_at",
			"#relay_gsi_pk = :expected_relay_gsi_pk",
			"#relay_gsi_sk = :expected_relay_gsi_sk",
			"attribute_not_exists(#relay_work_kind)",
		} {
			if !strings.Contains(condition, fragment) {
				t.Fatalf("marker-only condition missing %q: %s", fragment, condition)
			}
		}
		values := unmarshalValues(t, input.ExpressionAttributeValues)
		if values[":relay_work_kind"] != string(wantKind) {
			t.Fatalf("marker-only values = %#v", values)
		}
	}
}

func TestBackfillRelayMigratesPreviousRelaySortKeyLayout(t *testing.T) {
	legacyLayout := canonicalEmailOutbox(
		t, legacyOutbox("outbox_legacy_layout", "email", "ready"), notifications.WorkKindIntent,
	)
	legacyLayout["relay_gsi_sk"] = "2026-07-15T12:00:45.000000000Z#INTENT#dG50XzE#b3V0Ym94X2xlZ2FjeV9sYXlvdXQ"
	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
		mustMarshalMap(t, legacyLayout),
	}}}}

	summary, err := (Store{Table: "domain", Client: client}).BackfillRelay(
		context.Background(), BackfillOptions{Tenants: []string{"tnt_1"}, Apply: true, PageSize: 25},
	)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RowsNeedingUpdate != 1 || summary.Updated != 1 || summary.SchemaConflicts != 0 ||
		len(client.updateInputs) != 1 {
		t.Fatalf("summary=%#v updates=%#v", summary, client.updateInputs)
	}
	update := client.updateInputs[0]
	if aws.ToString(update.UpdateExpression) != "SET #relay_gsi_sk = :relay_gsi_sk" {
		t.Fatalf("legacy layout update = %s", aws.ToString(update.UpdateExpression))
	}
	values := unmarshalValues(t, update.ExpressionAttributeValues)
	if values[":expected_relay_gsi_sk"] != legacyLayout["relay_gsi_sk"] ||
		values[":relay_gsi_sk"] != canonicalEmailOutbox(t, legacyOutbox("outbox_legacy_layout", "email", "ready"), notifications.WorkKindIntent)["relay_gsi_sk"] {
		t.Fatalf("legacy layout values = %#v", values)
	}
}

func TestBackfillRelayClassifiesVersionMismatchAndDivergentRelayFieldsAsSchemaConflicts(t *testing.T) {
	wrongVersion := legacyOutbox("outbox_wrong_version", "email", "ready")
	wrongVersion["relay_schema_version"] = 2
	wrongVersion["created_at"] = "not-a-timestamp"

	wrongIndex := canonicalEmailOutbox(
		t,
		legacyOutbox("outbox_wrong_index", "email", "ready"),
		notifications.WorkKindIntent,
	)
	delete(wrongIndex, "relay_work_kind")
	wrongIndex["relay_gsi_pk"] = "NOTIFICATION_RELAY#V1#BUCKET#63"

	telegramWithRelayIndex := legacyOutbox("outbox_telegram_indexed", "telegram", "ready")
	telegramWithRelayIndex["expansion_status"] = "deferred_unsupported_channel"
	telegramWithRelayIndex["relay_gsi_pk"] = "NOTIFICATION_RELAY#V1#BUCKET#00"
	partialRelayInvalidTimestamp := legacyOutbox("outbox_partial_invalid_time", "email", "ready")
	partialRelayInvalidTimestamp["created_at"] = "not-a-timestamp"
	partialRelayInvalidTimestamp["expansion_status"] = "pending"

	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
		mustMarshalMap(t, wrongVersion),
		mustMarshalMap(t, wrongIndex),
		mustMarshalMap(t, telegramWithRelayIndex),
		mustMarshalMap(t, partialRelayInvalidTimestamp),
	}}}}
	summary, err := (Store{Table: "domain", Client: client}).BackfillRelay(
		context.Background(),
		BackfillOptions{Tenants: []string{"tnt_1"}, Apply: true, PageSize: 25},
	)
	if err != nil {
		t.Fatal(err)
	}
	if summary.SchemaConflicts != 4 || summary.RowFailures != 4 ||
		summary.DecodeFailures != 0 || summary.RowsNeedingUpdate != 0 || summary.Updated != 0 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(client.updateInputs) != 0 {
		t.Fatalf("schema conflicts made %d updates", len(client.updateInputs))
	}
}

func TestBackfillRelayContinuesAfterIsolatedDecodeConditionalAndUpdateFailures(t *testing.T) {
	invalidTimestamp := legacyOutbox("outbox_invalid_timestamp", "email", "ready")
	invalidTimestamp["created_at"] = "2026-07-15T12:00:45Z"
	conditional := legacyOutbox("outbox_concurrent", "email", "ready")
	updateFailure := legacyOutbox("outbox_update_failure", "telegram", "ready")
	success := legacyOutbox("outbox_success", "email", "blocked")
	success["kind"] = "recovery"
	success["depends_on_outbox_id"] = "outbox_opening"

	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
			mustMarshalMap(t, invalidTimestamp),
			mustMarshalMap(t, conditional),
			mustMarshalMap(t, updateFailure),
			mustMarshalMap(t, success),
		}}},
		updateErrors: []error{
			&types.ConditionalCheckFailedException{Message: aws.String("changed")},
			errors.New("temporary update failure"),
			nil,
		},
	}
	summary, err := (Store{Table: "domain", Client: client}).BackfillRelay(
		context.Background(),
		BackfillOptions{Tenants: []string{"tnt_1"}, Apply: true, PageSize: 25},
	)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RowsQueried != 4 || summary.RowsNeedingUpdate != 3 || summary.Updated != 1 ||
		summary.DecodeFailures != 1 || summary.ConcurrentChanges != 1 ||
		summary.UpdateFailures != 1 || summary.RowFailures != 3 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(client.updateInputs) != 3 {
		t.Fatalf("UpdateItem calls = %d, want 3", len(client.updateInputs))
	}
}

func TestBackfillRelayRejectsInvalidOptionsBeforeQuery(t *testing.T) {
	tests := []struct {
		name    string
		options BackfillOptions
	}{
		{name: "no tenants", options: BackfillOptions{PageSize: 25}},
		{name: "zero page size", options: BackfillOptions{Tenants: []string{"tnt_1"}}},
		{name: "negative page size", options: BackfillOptions{Tenants: []string{"tnt_1"}, PageSize: -1}},
		{name: "negative max rows", options: BackfillOptions{Tenants: []string{"tnt_1"}, PageSize: 25, MaxRows: -1}},
		{name: "empty tenant", options: BackfillOptions{Tenants: []string{""}, PageSize: 25}},
		{name: "blank tenant", options: BackfillOptions{Tenants: []string{"  "}, PageSize: 25}},
		{name: "NUL tenant", options: BackfillOptions{Tenants: []string{"tnt\x00one"}, PageSize: 25}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			_, err := (Store{Table: "domain", Client: client}).BackfillRelay(
				context.Background(),
				test.options,
			)
			if err == nil {
				t.Fatal("BackfillRelay() succeeded")
			}
			if len(client.queryInputs) != 0 {
				t.Fatalf("invalid options made %d Query calls", len(client.queryInputs))
			}
		})
	}
}

func legacyOutbox(outboxID, channel, status string) map[string]any {
	return map[string]any{
		"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_OUTBOX#" + outboxID,
		"entity_type": "notification_outbox", "tenant_id": "tnt_1", "outbox_id": outboxID,
		"event_id": "alert_1", "rule_id": "rule_1", "channel": channel,
		"kind": "opening", "status": status, "depends_on_outbox_id": "",
		"created_at": "2026-07-15T12:00:45.000000000Z",
	}
}

func mustMarshalMap(t *testing.T, value map[string]any) map[string]types.AttributeValue {
	t.Helper()
	item, err := attributevalue.MarshalMap(value)
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func unmarshalValues(t *testing.T, values map[string]types.AttributeValue) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := attributevalue.UnmarshalMap(values, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func canonicalEmailOutbox(t *testing.T, item map[string]any, workKind notifications.WorkKind) map[string]any {
	t.Helper()
	canonical := cloneMap(item)
	createdAt, err := time.Parse("2006-01-02T15:04:05.000000000Z", item["created_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	relayKey, err := notifications.BuildRelayIndexKey(workKind, item["tenant_id"].(string), item["outbox_id"].(string), createdAt)
	if err != nil {
		t.Fatal(err)
	}
	canonical["relay_schema_version"] = 1
	canonical["expansion_status"] = "pending"
	canonical["available_at"] = item["created_at"]
	canonical["relay_work_kind"] = string(workKind)
	canonical["relay_gsi_pk"] = relayKey.PartitionKey
	canonical["relay_gsi_sk"] = relayKey.SortKey
	return canonical
}

func cloneMap(item map[string]any) map[string]any {
	cloned := make(map[string]any, len(item))
	for key, value := range item {
		cloned[key] = value
	}
	return cloned
}
