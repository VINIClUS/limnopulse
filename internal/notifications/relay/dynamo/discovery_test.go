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
		"relay_work_kind": string(notifications.WorkKindIntent),
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
		Bucket: indexKey.Bucket, Kind: notifications.WorkKindIntent,
		DueThrough: due, PageSize: 25,
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
		aws.ToString(input.FilterExpression) !=
			"(#relay_work_kind = :relay_work_kind OR attribute_not_exists(#relay_work_kind))" ||
		aws.ToInt32(input.Limit) != 25 || aws.ToBool(input.ConsistentRead) {
		t.Fatalf("query input = %#v", input)
	}
	if input.ExpressionAttributeNames["#relay_pk"] != "relay_gsi_pk" ||
		input.ExpressionAttributeNames["#relay_sk"] != "relay_gsi_sk" ||
		input.ExpressionAttributeNames["#relay_work_kind"] != "relay_work_kind" {
		t.Fatalf("query names = %#v", input.ExpressionAttributeNames)
	}
	var values map[string]string
	if err := attributevalue.UnmarshalMap(input.ExpressionAttributeValues, &values); err != nil {
		t.Fatal(err)
	}
	if values[":pk"] != indexKey.PartitionKey || values[":due"] != "2026-07-16T12:30:00.000000000Z#~" ||
		values[":relay_work_kind"] != string(notifications.WorkKindIntent) {
		t.Fatalf("query values = %#v", values)
	}

	if _, err := store.QueryDue(context.Background(), relay.DueRequest{
		Bucket: indexKey.Bucket, Kind: notifications.WorkKindIntent,
		DueThrough: due, PageSize: 25, NextToken: page.NextToken,
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.queryInputs[1].ExclusiveStartKey, lastKey) {
		t.Fatalf("exclusive start key = %#v, want %#v", client.queryInputs[1].ExclusiveStartKey, lastKey)
	}
}

func TestQueryDueRoutesCanonicalMarkerlessRowsOnlyToTheirInferredLane(t *testing.T) {
	due := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindIntent, "tnt_1", "outbox_1", due.Add(-time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	markerless := marshalMap(t, map[string]any{
		"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_OUTBOX#outbox_1",
		"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
	})
	deliveryIndexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, "tnt_1", "delivery_1", due.Add(-time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	markerlessDelivery := marshalMap(t, map[string]any{
		"PK": "NOTIFICATION_OUTBOX#outbox_1", "SK": "DELIVERY#delivery_1",
		"relay_gsi_pk": deliveryIndexKey.PartitionKey, "relay_gsi_sk": deliveryIndexKey.SortKey,
	})
	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{
		{Items: []map[string]types.AttributeValue{markerless}},
		{Items: []map[string]types.AttributeValue{markerless}},
		{Items: []map[string]types.AttributeValue{markerlessDelivery}},
	}}
	store := Store{Table: "domain", Client: client}

	wrongLane, err := store.QueryDue(context.Background(), relay.DueRequest{
		Bucket: indexKey.Bucket, Kind: notifications.WorkKindDelivery,
		DueThrough: due, PageSize: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(wrongLane.Candidates) != 0 {
		t.Fatalf("markerless INTENT leaked into DELIVERY lane: %#v", wrongLane)
	}
	intentLane, err := store.QueryDue(context.Background(), relay.DueRequest{
		Bucket: indexKey.Bucket, Kind: notifications.WorkKindIntent,
		DueThrough: due, PageSize: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(intentLane.Candidates) != 1 || intentLane.Candidates[0].Kind != notifications.WorkKindIntent {
		t.Fatalf("markerless INTENT page = %#v", intentLane)
	}
	deliveryLane, err := store.QueryDue(context.Background(), relay.DueRequest{
		Bucket: deliveryIndexKey.Bucket, Kind: notifications.WorkKindDelivery,
		DueThrough: due, PageSize: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveryLane.Candidates) != 1 ||
		deliveryLane.Candidates[0].Kind != notifications.WorkKindDelivery {
		t.Fatalf("markerless DELIVERY page = %#v", deliveryLane)
	}
}

func TestQueryDueRejectsNoncanonicalMarkerlessStorageAndIndexIdentity(t *testing.T) {
	due := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		name   string
		kind   notifications.WorkKind
		itemID string
		mutate func(map[string]any)
	}{
		{
			name: "outbox base key", kind: notifications.WorkKindIntent, itemID: "outbox_1",
			mutate: func(item map[string]any) { item["PK"] = "WRONG#tnt_1" },
		},
		{
			name: "delivery base key", kind: notifications.WorkKindDelivery, itemID: "delivery_1",
			mutate: func(item map[string]any) { item["PK"] = "TENANT#tnt_1" },
		},
		{
			name: "GSI partition", kind: notifications.WorkKindIntent, itemID: "outbox_1",
			mutate: func(item map[string]any) {
				item["relay_gsi_pk"] = "NOTIFICATION_RELAY#V1#BUCKET#63"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indexKey, err := notifications.BuildRelayIndexKey(
				test.kind, "tnt_1", test.itemID, due.Add(-time.Minute),
			)
			if err != nil {
				t.Fatal(err)
			}
			item := map[string]any{
				"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
			}
			if test.kind == notifications.WorkKindDelivery {
				item["PK"] = "NOTIFICATION_OUTBOX#outbox_1"
				item["SK"] = "DELIVERY#" + test.itemID
			} else {
				item["PK"] = "TENANT#tnt_1"
				item["SK"] = "NOTIFICATION_OUTBOX#" + test.itemID
			}
			test.mutate(item)
			store := Store{Table: "domain", Client: &fakeClient{
				queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{marshalMap(t, item)}}},
			}}

			page, err := store.QueryDue(context.Background(), relay.DueRequest{
				Bucket: indexKey.Bucket, Kind: test.kind, DueThrough: due, PageSize: 25,
			})
			if err == nil || len(page.Candidates) != 0 {
				t.Fatalf("page = %#v, error = %v", page, err)
			}
		})
	}
}

func TestQueryDueReturnsTokenForEmptyFilteredPage(t *testing.T) {
	due := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, "tnt_1", "delivery_1", due.Add(-time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	lastKey := marshalMap(t, map[string]any{
		"PK": "NOTIFICATION_OUTBOX#outbox_1", "SK": "DELIVERY#delivery_1",
		"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
	})
	client := &fakeClient{queryOutputs: []*awssdk.QueryOutput{{LastEvaluatedKey: lastKey}}}
	store := Store{Table: "domain", Client: client}

	page, err := store.QueryDue(context.Background(), relay.DueRequest{
		Bucket: indexKey.Bucket, Kind: notifications.WorkKindDelivery,
		DueThrough: due, PageSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Candidates) != 0 || page.NextToken == "" {
		t.Fatalf("page = %#v", page)
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
		"relay_work_kind": string(notifications.WorkKindIntent),
		"relay_gsi_pk":    indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
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
	mismatchedLane := make(map[string]any, len(base))
	for key, value := range base {
		mismatchedLane[key] = value
	}
	mismatchedLane["relay_work_kind"] = string(notifications.WorkKindDependency)
	markerless := make(map[string]any, len(base)-1)
	for key, value := range base {
		if key != "relay_work_kind" {
			markerless[key] = value
		}
	}
	client := &fakeClient{getOutputs: []*awssdk.GetItemOutput{
		{},
		{Item: marshalMap(t, divergent)},
		{Item: marshalMap(t, terminal)},
		{Item: marshalMap(t, mismatchedLane)},
		{Item: marshalMap(t, markerless)},
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
	if work, current, err := store.Reload(context.Background(), candidate, relayTime); err == nil || current || work != (relay.Work{}) {
		t.Fatalf("mismatched lane reload = %#v, %t, %v", work, current, err)
	}
	if work, current, err := store.Reload(context.Background(), candidate, relayTime); err != nil || !current || work.Kind != notifications.WorkKindIntent {
		t.Fatalf("markerless reload = %#v, %t, %v", work, current, err)
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
	if len(client.getInputs) != 6 {
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

func TestReloadAcceptsCanonicalMarkerlessPendingDelivery(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	available := relayTime.Add(-time.Minute)
	deliveryID := "delivery_1"
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, "tnt_1", deliveryID, available,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := relay.Candidate{
		PK: "NOTIFICATION_OUTBOX#outbox_1", SK: "DELIVERY#" + deliveryID,
		RelayPK: indexKey.PartitionKey, RelaySK: indexKey.SortKey,
		Kind: notifications.WorkKindDelivery, AvailableAt: available,
	}
	base := map[string]any{
		"PK": candidate.PK, "SK": candidate.SK, "entity_type": "notification_delivery",
		"tenant_id": "tnt_1", "outbox_id": "outbox_1", "delivery_id": deliveryID,
		"event_id": "event_1", "rule_id": "rule_1", "kind": "opening", "channel": "email",
		"state": "pending", "delivery_revision": int64(1),
		"available_at": available.Format(fixedUTCLayout), "relay_schema_version": int64(1),
		"relay_gsi_pk": indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
	}
	store := Store{Table: "domain", Client: &fakeClient{
		getOutputs: []*awssdk.GetItemOutput{{Item: marshalMap(t, base)}},
	}}

	work, current, err := store.Reload(context.Background(), candidate, relayTime)
	if err != nil || !current || work.Kind != notifications.WorkKindDelivery ||
		work.DeliveryID != deliveryID || work.State != "pending" {
		t.Fatalf("work = %#v, current = %t, error = %v", work, current, err)
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
		"relay_work_kind": string(notifications.WorkKindIntent),
		"relay_gsi_pk":    indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
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
		"relay_work_kind": string(notifications.WorkKindIntent),
		"relay_gsi_pk":    indexKey.PartitionKey, "relay_gsi_sk": indexKey.SortKey,
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
