package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestClaimConditionsCurrentIndexStateRevisionCursorAndFencesLease(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	available := now.Add(-time.Minute)
	indexKey, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindIntent, "tnt_1", "outbox_1", available,
	)
	if err != nil {
		t.Fatal(err)
	}
	work := relay.Work{
		Candidate: relay.Candidate{
			PK: "TENANT#tnt_1", SK: "NOTIFICATION_OUTBOX#outbox_1",
			RelayPK: indexKey.PartitionKey, RelaySK: indexKey.SortKey,
			Kind: notifications.WorkKindIntent, AvailableAt: available,
		},
		TenantID: "tnt_1", ItemID: "outbox_1", OutboxID: "outbox_1", EventID: "event_1",
		NotificationKind: notifications.NotificationKindOpening, Channel: notifications.ChannelEmail,
		State: "pending", Revision: 3, Cursor: "cursor_1",
	}
	claimedItem := map[string]any{
		"PK": work.PK, "SK": work.SK, "entity_type": "notification_outbox",
		"tenant_id": work.TenantID, "outbox_id": work.OutboxID, "event_id": work.EventID,
		"kind": "opening", "channel": "email", "expansion_status": work.State,
		"expansion_revision": work.Revision, "expansion_cursor": work.Cursor,
		"available_at": available.Format(fixedUTCLayout), "relay_schema_version": int64(1),
		"relay_work_kind": string(notifications.WorkKindIntent),
		"relay_gsi_pk":    work.RelayPK, "relay_gsi_sk": work.RelaySK,
		"relay_lease_owner": "run_1", "relay_lease_epoch": int64(4),
		"relay_lease_expires_at": now.Add(20 * time.Second).Format(fixedUTCLayout),
	}
	client := &fakeClient{updateOutputs: []*awssdk.UpdateItemOutput{{Attributes: marshalMap(t, claimedItem)}}}
	store := Store{Table: "domain", Client: client}

	claimed, acquired, err := store.Claim(context.Background(), work, relay.LeaseRequest{
		Owner: "run_1", Now: now, ExpiresAt: now.Add(20 * time.Second), DueThrough: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !acquired || claimed.LeaseEpoch != 4 || claimed.Revision != work.Revision || claimed.Cursor != work.Cursor {
		t.Fatalf("claimed work = %#v, acquired = %t", claimed, acquired)
	}
	if len(client.updateInputs) != 1 {
		t.Fatalf("UpdateItem calls = %d", len(client.updateInputs))
	}
	input := client.updateInputs[0]
	if input.ReturnValues != types.ReturnValueAllNew {
		t.Fatalf("return values = %#v", input.ReturnValues)
	}
	condition := aws.ToString(input.ConditionExpression)
	if strings.Contains(condition, "attribute_not_exists(#revision)") {
		t.Fatalf("nonzero revision was not fenced exactly: %s", condition)
	}
	for _, fragment := range []string{
		"#relay_schema = :schema",
		"(attribute_not_exists(#relay_work_kind) OR #relay_work_kind = :relay_work_kind)",
		"#relay_pk = :relay_pk", "#relay_sk = :relay_sk",
		"#available_at <= :due", "#state = :state", "#revision = :revision",
		"#cursor = :cursor", "attribute_not_exists(#lease_expires)",
		"#lease_expires <= :now", "#lease_owner = :owner",
	} {
		if !strings.Contains(condition, fragment) {
			t.Fatalf("condition missing %q: %s", fragment, condition)
		}
	}
	update := aws.ToString(input.UpdateExpression)
	for _, fragment := range []string{
		"#relay_work_kind = if_not_exists(#relay_work_kind, :relay_work_kind)",
		"#lease_owner = :owner", "#lease_expires = :expires",
		"#lease_epoch = if_not_exists(#lease_epoch, :zero) + :one",
	} {
		if !strings.Contains(update, fragment) {
			t.Fatalf("update missing %q: %s", fragment, update)
		}
	}
	if input.ExpressionAttributeNames["#state"] != "expansion_status" ||
		input.ExpressionAttributeNames["#revision"] != "expansion_revision" ||
		input.ExpressionAttributeNames["#cursor"] != "expansion_cursor" {
		t.Fatalf("condition names = %#v", input.ExpressionAttributeNames)
	}
	var values map[string]any
	if err := attributevalue.UnmarshalMap(input.ExpressionAttributeValues, &values); err != nil {
		t.Fatal(err)
	}
	if values[":owner"] != "run_1" || values[":cursor"] != "cursor_1" ||
		values[":state"] != "pending" || values[":relay_work_kind"] != string(notifications.WorkKindIntent) ||
		fmt.Sprint(values[":revision"]) != "3" {
		t.Fatalf("condition values = %#v", values)
	}
}

func TestClaimLazilyMaterializesWorkKindForMarkerlessOutboxAndDelivery(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		name string
		work relay.Work
	}{
		{name: "outbox", work: openingWork(t, now)},
		{name: "delivery", work: deliveryWork(t, now)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{updateOutputs: []*awssdk.UpdateItemOutput{{Attributes: marshalMap(t, map[string]any{
				"relay_work_kind":   string(test.work.Kind),
				"relay_lease_epoch": test.work.LeaseEpoch + 1,
			})}}}
			store := Store{Table: "domain", Client: client}

			claimed, acquired, err := store.Claim(context.Background(), test.work, relay.LeaseRequest{
				Owner: "run_2", Now: now, ExpiresAt: now.Add(20 * time.Second), DueThrough: now,
			})
			if err != nil || !acquired || claimed.LeaseEpoch != test.work.LeaseEpoch+1 {
				t.Fatalf("claimed = %#v, acquired = %t, error = %v", claimed, acquired, err)
			}
			input := client.updateInputs[0]
			if !strings.Contains(aws.ToString(input.ConditionExpression),
				"(attribute_not_exists(#relay_work_kind) OR #relay_work_kind = :relay_work_kind)") {
				t.Fatalf("condition = %s", aws.ToString(input.ConditionExpression))
			}
			if !strings.Contains(aws.ToString(input.UpdateExpression),
				"#relay_work_kind = if_not_exists(#relay_work_kind, :relay_work_kind)") {
				t.Fatalf("update = %s", aws.ToString(input.UpdateExpression))
			}
		})
	}
}

func TestClaimClassifiesConditionalContentionWithoutMaskingInfrastructureErrors(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		name         string
		updateErr    error
		wantConflict bool
	}{
		{
			name: "conditional contention",
			updateErr: &types.ConditionalCheckFailedException{
				Message: aws.String("lease is already held"),
			},
			wantConflict: true,
		},
		{name: "infrastructure error", updateErr: errors.New("connection reset")},
		{
			name:      "unexpected transaction cancellation",
			updateErr: &types.TransactionCanceledException{Message: aws.String("unexpected service response")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := Store{Table: "domain", Client: &fakeClient{updateErrors: []error{test.updateErr}}}
			claimed, acquired, err := store.Claim(
				context.Background(), openingWork(t, now), relay.LeaseRequest{
					Owner: "run_2", Now: now, ExpiresAt: now.Add(20 * time.Second), DueThrough: now,
				},
			)
			if test.wantConflict {
				if err != nil || acquired || claimed != (relay.Work{}) {
					t.Fatalf("claimed = %#v, acquired = %t, error = %v", claimed, acquired, err)
				}
				return
			}
			if err == nil || acquired || claimed != (relay.Work{}) {
				t.Fatalf("claimed = %#v, acquired = %t, error = %v", claimed, acquired, err)
			}
		})
	}
}
