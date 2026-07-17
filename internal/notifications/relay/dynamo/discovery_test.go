package dynamo

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeClient struct {
	queryInputs    []*awssdk.QueryInput
	queryOutputs   []*awssdk.QueryOutput
	getInputs      []*awssdk.GetItemInput
	getOutputs     []*awssdk.GetItemOutput
	updateInputs   []*awssdk.UpdateItemInput
	updateOutputs  []*awssdk.UpdateItemOutput
	updateErrors   []error
	transactInputs []*awssdk.TransactWriteItemsInput
}

func (client *fakeClient) Query(_ context.Context, input *awssdk.QueryInput, _ ...func(*awssdk.Options)) (*awssdk.QueryOutput, error) {
	client.queryInputs = append(client.queryInputs, input)
	if len(client.queryOutputs) == 0 {
		return &awssdk.QueryOutput{}, nil
	}
	output := client.queryOutputs[0]
	client.queryOutputs = client.queryOutputs[1:]
	return output, nil
}

func (client *fakeClient) GetItem(_ context.Context, input *awssdk.GetItemInput, _ ...func(*awssdk.Options)) (*awssdk.GetItemOutput, error) {
	client.getInputs = append(client.getInputs, input)
	if len(client.getOutputs) == 0 {
		return &awssdk.GetItemOutput{}, nil
	}
	output := client.getOutputs[0]
	client.getOutputs = client.getOutputs[1:]
	return output, nil
}

func (client *fakeClient) UpdateItem(_ context.Context, input *awssdk.UpdateItemInput, _ ...func(*awssdk.Options)) (*awssdk.UpdateItemOutput, error) {
	client.updateInputs = append(client.updateInputs, input)
	if len(client.updateErrors) != 0 {
		err := client.updateErrors[0]
		client.updateErrors = client.updateErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(client.updateOutputs) == 0 {
		return &awssdk.UpdateItemOutput{}, nil
	}
	output := client.updateOutputs[0]
	client.updateOutputs = client.updateOutputs[1:]
	return output, nil
}

func (client *fakeClient) TransactWriteItems(_ context.Context, input *awssdk.TransactWriteItemsInput, _ ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error) {
	client.transactInputs = append(client.transactInputs, input)
	return &awssdk.TransactWriteItemsOutput{}, nil
}

func TestQueryDueUsesRelayGSIAndRoundTripsPaginationToken(t *testing.T) {
	due := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	available := due.Add(-time.Minute)
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindIntent, "tnt_1", "outbox_1", available,
	)
	if err != nil {
		t.Fatal(err)
	}
	item := marshalMap(t, map[string]any{
		"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_OUTBOX#outbox_1",
		"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
	})
	lastKey := map[string]types.AttributeValue{
		"PK": item["PK"], "SK": item["SK"],
		"relay_gsi_pk": item["relay_gsi_pk"], "relay_gsi_sk": item["relay_gsi_sk"],
	}
	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{
		{Items: []map[string]types.AttributeValue{item}, LastEvaluatedKey: lastKey},
		{},
	}}
	store := Store{Table: "domain", Client: client}

	page, err := store.QueryDue(context.Background(), relay.DueRequest{
		Bucket: indexKey.Bucket, DueThrough: due, PageSize: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Candidates) != 1 || page.NextToken == "" {
		t.Fatalf("page = %#v", page)
	}
	candidate := page.Candidates[0]
	if candidate.PK != "TENANT#tnt_1" || candidate.SK != "NOTIFICATION_OUTBOX#outbox_1" ||
		candidate.Kind != notifications.WorkKindIntent || !candidate.AvailableAt.Equal(available) ||
		candidate.RelayPK != indexKey.PartitionKey || candidate.RelaySK != indexKey.SortKey {
		t.Fatalf("candidate = %#v", candidate)
	}
	input := client.queryInputs[0]
	if aws.ToString(input.IndexName) != RelayIndex ||
		aws.ToString(input.KeyConditionExpression) != "#relay_pk = :pk AND #relay_sk <= :due" ||
		aws.ToInt32(input.Limit) != 25 || aws.ToBool(input.ConsistentRead) {
		t.Fatalf("query input = %#v", input)
	}
	if input.ExpressionAttributeNames["#relay_pk"] != "relay_gsi_pk" ||
		input.ExpressionAttributeNames["#relay_sk"] != "relay_gsi_sk" {
		t.Fatalf("query names = %#v", input.ExpressionAttributeNames)
	}
	var values map[string]string
	if err := attributevalue.UnmarshalMap(input.ExpressionAttributeValues, &values); err != nil {
		t.Fatal(err)
	}
	if values[":pk"] != indexKey.PartitionKey || values[":due"] != "2026-07-16T12:30:00.000000000Z#~" {
		t.Fatalf("query values = %#v", values)
	}

	if _, err := store.QueryDue(context.Background(), relay.DueRequest{
		Bucket: indexKey.Bucket, DueThrough: due, PageSize: 25, NextToken: page.NextToken,
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.queryInputs[1].ExclusiveStartKey, lastKey) {
		t.Fatalf("exclusive start key = %#v, want %#v", client.queryInputs[1].ExclusiveStartKey, lastKey)
	}
}

func TestReloadReadsBaseConsistentlyAndSkipsMissingOrIndexDivergentRows(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	available := relayTime.Add(-time.Minute)
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindIntent, "tnt_1", "outbox_1", available,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := relay.Candidate{
		PK: "TENANT#tnt_1", SK: "NOTIFICATION_OUTBOX#outbox_1",
		RelayPK: indexKey.PartitionKey, RelaySK: indexKey.SortKey,
		Kind: notifications.WorkKindIntent, AvailableAt: available,
	}
	base := map[string]any{
		"PK": candidate.PK, "SK": candidate.SK, "entity_type": "notification_outbox",
		"tenant_id": "tnt_1", "outbox_id": "outbox_1", "event_id": "event_1", "rule_id": "rule_1",
		"kind": "opening", "channel": "email", "expansion_status": "pending",
		"expansion_revision": int64(3), "expansion_cursor": "cursor_1",
		"available_at": available.Format(fixedUTCLayout), "relay_schema_version": int64(1),
		"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
	}
	divergent := make(map[string]any, len(base))
	for key, value := range base {
		divergent[key] = value
	}
	divergent["relay_gsi_sk"] = indexKey.SortKey + "#stale"
	terminal := make(map[string]any, len(base))
	for key, value := range base {
		terminal[key] = value
	}
	terminal["expansion_status"] = "expanded"
	client := &fakeClient{getOutputs: []*awssdk.GetItemOutput{
		{},
		{Item: marshalMap(t, divergent)},
		{Item: marshalMap(t, terminal)},
		{Item: marshalMap(t, base)},
	}}
	store := Store{Table: "domain", Client: client}

	if work, current, err := store.Reload(context.Background(), candidate, relayTime); err != nil || current || work != (relay.Work{}) {
		t.Fatalf("missing reload = %#v, %t, %v", work, current, err)
	}
	if work, current, err := store.Reload(context.Background(), candidate, relayTime); err != nil || current || work != (relay.Work{}) {
		t.Fatalf("divergent reload = %#v, %t, %v", work, current, err)
	}
	if work, current, err := store.Reload(context.Background(), candidate, relayTime); err != nil || current || work != (relay.Work{}) {
		t.Fatalf("terminal stale reload = %#v, %t, %v", work, current, err)
	}
	work, current, err := store.Reload(context.Background(), candidate, relayTime)
	if err != nil {
		t.Fatal(err)
	}
	if !current || work.TenantID != "tnt_1" || work.ItemID != "outbox_1" ||
		work.OutboxID != "outbox_1" || work.EventID != "event_1" ||
		work.NotificationKind != notifications.NotificationKindOpening ||
		work.Channel != notifications.ChannelEmail || work.State != "pending" ||
		work.Revision != 3 || work.Cursor != "cursor_1" {
		t.Fatalf("work = %#v", work)
	}
	if len(client.getInputs) != 4 {
		t.Fatalf("GetItem calls = %d", len(client.getInputs))
	}
	for _, input := range client.getInputs {
		if !aws.ToBool(input.ConsistentRead) {
			t.Fatalf("base reload was not consistent: %#v", input)
		}
		var key map[string]string
		if err := attributevalue.UnmarshalMap(input.Key, &key); err != nil {
			t.Fatal(err)
		}
		if key["PK"] != candidate.PK || key["SK"] != candidate.SK {
			t.Fatalf("base key = %#v", key)
		}
	}
}

func TestReloadRejectsNoncanonicalBaseStorageKey(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	available := relayTime.Add(-time.Minute)
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindIntent, "tnt_1", "outbox_1", available,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := relay.Candidate{
		PK: "WRONG#tnt_1", SK: "WRONG#outbox_1",
		RelayPK: indexKey.PartitionKey, RelaySK: indexKey.SortKey,
		Kind: notifications.WorkKindIntent, AvailableAt: available,
	}
	base := map[string]any{
		"PK": candidate.PK, "SK": candidate.SK, "entity_type": "notification_outbox",
		"tenant_id": "tnt_1", "outbox_id": "outbox_1", "event_id": "event_1", "rule_id": "rule_1",
		"kind": "opening", "channel": "email", "expansion_status": "pending",
		"available_at": available.Format(fixedUTCLayout), "relay_schema_version": int64(1),
		"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
	}
	store := Store{Table: "domain", Client: &fakeClient{
		getOutputs: []*awssdk.GetItemOutput{{Item: marshalMap(t, base)}},
	}}

	work, current, err := store.Reload(context.Background(), candidate, relayTime)
	if err == nil || current || work != (relay.Work{}) {
		t.Fatalf("reload = %#v, current = %t, error = %v", work, current, err)
	}
}

func TestReloadRejectsIndexedTelegramWorkBeforeFanout(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	available := relayTime.Add(-time.Minute)
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindIntent, "tnt_1", "outbox_1", available,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := relay.Candidate{
		PK: "TENANT#tnt_1", SK: "NOTIFICATION_OUTBOX#outbox_1",
		RelayPK: indexKey.PartitionKey, RelaySK: indexKey.SortKey,
		Kind: notifications.WorkKindIntent, AvailableAt: available,
	}
	base := map[string]any{
		"PK": candidate.PK, "SK": candidate.SK, "entity_type": "notification_outbox",
		"tenant_id": "tnt_1", "outbox_id": "outbox_1", "event_id": "event_1", "rule_id": "rule_1",
		"kind": "opening", "channel": "telegram", "expansion_status": "pending",
		"available_at": available.Format(fixedUTCLayout), "relay_schema_version": int64(1),
		"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
	}
	store := Store{Table: "domain", Client: &fakeClient{
		getOutputs: []*awssdk.GetItemOutput{{Item: marshalMap(t, base)}},
	}}

	work, current, err := store.Reload(context.Background(), candidate, relayTime)
	if err == nil || current || work != (relay.Work{}) {
		t.Fatalf("reload = %#v, current = %t, error = %v", work, current, err)
	}
}

func marshalMap(t *testing.T, value map[string]any) map[string]types.AttributeValue {
	t.Helper()
	item, err := attributevalue.MarshalMap(value)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
