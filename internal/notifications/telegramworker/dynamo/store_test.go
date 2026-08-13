package dynamo

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeClient struct {
	items        []map[string]types.AttributeValue
	gets         []*awssdk.GetItemInput
	transactions []*awssdk.TransactWriteItemsInput
}

func (client *fakeClient) GetItem(
	_ context.Context,
	input *awssdk.GetItemInput,
	_ ...func(*awssdk.Options),
) (*awssdk.GetItemOutput, error) {
	client.gets = append(client.gets, input)
	if len(client.items) == 0 {
		return &awssdk.GetItemOutput{}, nil
	}
	item := client.items[0]
	client.items = client.items[1:]
	return &awssdk.GetItemOutput{Item: item}, nil
}

func (*fakeClient) UpdateItem(
	_ context.Context,
	_ *awssdk.UpdateItemInput,
	_ ...func(*awssdk.Options),
) (*awssdk.UpdateItemOutput, error) {
	return &awssdk.UpdateItemOutput{}, nil
}

func (client *fakeClient) TransactWriteItems(
	_ context.Context,
	input *awssdk.TransactWriteItemsInput,
	_ ...func(*awssdk.Options),
) (*awssdk.TransactWriteItemsOutput, error) {
	client.transactions = append(client.transactions, input)
	return &awssdk.TransactWriteItemsOutput{}, nil
}

func TestCheckGatesFencesTelegramEventLifecycle(t *testing.T) {
	tests := []struct {
		name        string
		kind        notifications.NotificationKind
		eventStatus string
		allowed     bool
	}{
		{name: "active opening", kind: notifications.NotificationKindOpening, eventStatus: "open", allowed: true},
		{name: "acknowledged opening", kind: notifications.NotificationKindOpening, eventStatus: "acknowledged", allowed: true},
		{name: "resolved opening", kind: notifications.NotificationKindOpening, eventStatus: "resolved"},
		{name: "suppressed opening", kind: notifications.NotificationKindOpening, eventStatus: "suppressed"},
		{name: "resolved recovery", kind: notifications.NotificationKindRecovery, eventStatus: "resolved", allowed: true},
		{name: "premature recovery", kind: notifications.NotificationKindRecovery, eventStatus: "open"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := telegramRecord(t, test.kind)
			client := &fakeClient{items: telegramGateItems(t, record, test.eventStatus)}
			gate, err := New("domain", client).CheckGates(context.Background(), record)
			if err != nil {
				t.Fatal(err)
			}
			if gate.Allowed != test.allowed {
				t.Fatalf("gate = %#v", gate)
			}
			if test.allowed && (gate.Fence.EventStatus != test.eventStatus || !gate.Fence.IsComplete()) {
				t.Fatalf("allowed fence = %#v", gate.Fence)
			}
			if !test.allowed && gate.CancellationReason == "" {
				t.Fatalf("denied gate has no cancellation reason: %#v", gate)
			}
			for _, input := range client.gets {
				if input.ConsistentRead == nil || !*input.ConsistentRead {
					t.Fatalf("non-consistent gate read = %#v", input)
				}
			}
		})
	}
}

func TestCheckGatesCancelsWhenCurrentTelegramEligibilityDisappears(t *testing.T) {
	record := telegramRecord(t, notifications.NotificationKindOpening)
	complete := telegramGateItems(t, record, "open")
	for _, test := range []struct {
		name  string
		items []map[string]types.AttributeValue
	}{
		{name: "preference missing", items: []map[string]types.AttributeValue{complete[0], nil}},
		{name: "binding missing", items: []map[string]types.AttributeValue{complete[0], complete[1], nil}},
		{name: "destination missing", items: []map[string]types.AttributeValue{complete[0], complete[1], complete[2], nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate, err := New("domain", &fakeClient{items: test.items}).CheckGates(
				context.Background(), record,
			)
			if err != nil || gate.Allowed || gate.CancellationReason == "" {
				t.Fatalf("gate=%#v err=%v", gate, err)
			}
		})
	}
}

func TestBeginAttemptConditionallyFencesTelegramBindingDestinationAndEventStatus(t *testing.T) {
	record := telegramRecord(t, notifications.NotificationKindOpening)
	record.LeaseExpiresAt = time.Date(2026, 8, 13, 12, 1, 0, 0, time.UTC)
	client := &fakeClient{}
	_, err := New("domain", client).BeginAttempt(context.Background(), record, worker.BeginAttemptRequest{
		AttemptID: "att_1", StartedAt: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
		LeaseRequiredUntil: time.Date(2026, 8, 13, 12, 0, 20, 0, time.UTC),
		GateFence: worker.GateFence{
			Channel: notifications.ChannelTelegram, MembershipVersion: 2, PreferenceVersion: 3,
			PreferenceMinimumSeverity: "warning", EventSeverity: "critical", EventStatus: "open",
			TelegramDestinationID: "destination_hash", TelegramChatID: 123,
			TelegramBindingVersion: 4, TelegramDestinationVersion: 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(client.transactions) != 1 || len(client.transactions[0].TransactItems) != 7 {
		t.Fatalf("transactions = %#v", client.transactions)
	}
	serialized := ""
	for _, operation := range client.transactions[0].TransactItems {
		if operation.ConditionCheck != nil {
			serialized += *operation.ConditionCheck.ConditionExpression
			for _, name := range operation.ConditionCheck.ExpressionAttributeNames {
				serialized += name
			}
		}
	}
	for _, required := range []string{"telegram_enabled", "destination_id", "chat_id", "status"} {
		if !strings.Contains(serialized, required) {
			t.Fatalf("Telegram attempt fence missing %q: %s", required, serialized)
		}
	}
}

func telegramGateItems(
	t *testing.T,
	record worker.DeliveryRecord,
	eventStatus string,
) []map[string]types.AttributeValue {
	t.Helper()
	snapshot := record.Delivery
	return []map[string]types.AttributeValue{
		marshal(t, map[string]any{
			"PK": "TENANT#" + snapshot.TenantID, "SK": "MEMBER#" + snapshot.RecipientID,
			"entity_type": "tenant_member", "tenant_id": snapshot.TenantID,
			"cognito_sub": snapshot.RecipientID, "status": "active", "version": int64(2),
		}),
		marshal(t, map[string]any{
			"PK":          "TENANT#" + snapshot.TenantID,
			"SK":          "NOTIFICATION_PREFERENCE#USER#" + snapshot.RecipientID,
			"entity_type": "notification_preference", "tenant_id": snapshot.TenantID,
			"cognito_sub": snapshot.RecipientID, "telegram_enabled": true,
			"minimum_severity": "warning", "version": int64(3),
		}),
		marshal(t, map[string]any{
			"PK":          "TENANT#" + snapshot.TenantID,
			"SK":          "TELEGRAM_BINDING#USER#" + snapshot.RecipientID,
			"entity_type": "telegram_binding", "tenant_id": snapshot.TenantID,
			"recipient_id": snapshot.RecipientID, "destination_id": snapshot.DestinationID,
			"status": "verified", "version": int64(4),
		}),
		marshal(t, map[string]any{
			"PK": "TELEGRAM_DESTINATION#" + snapshot.DestinationID, "SK": "META",
			"entity_type": "telegram_destination", "destination_id": snapshot.DestinationID,
			"recipient_id": snapshot.RecipientID, "chat_id": snapshot.TelegramChatID,
			"status": "active", "version": int64(5),
		}),
		marshal(t, map[string]any{
			"PK": "TENANT#" + snapshot.TenantID, "SK": "ALERT_EVENT#" + snapshot.EventID,
			"entity_type": "alert_event", "tenant_id": snapshot.TenantID,
			"event_id": snapshot.EventID, "rule_id": snapshot.RuleID,
			"severity": "critical", "status": eventStatus,
		}),
	}
}

func telegramRecord(t *testing.T, kind notifications.NotificationKind) worker.DeliveryRecord {
	t.Helper()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	content, err := notifications.NewTelegramRenderedContent(
		notifications.TemplateTelegramAlertOpeningV1, notifications.LocalePTBR, "Alerta simples",
	)
	if kind == notifications.NotificationKindRecovery {
		content, err = notifications.NewTelegramRenderedContent(
			notifications.TemplateTelegramAlertRecoveryV1, notifications.LocalePTBR, "Recuperado",
		)
	}
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := notifications.NewDeliveryID("alert_1", kind, notifications.ChannelTelegram, "user_1")
	if err != nil {
		t.Fatal(err)
	}
	params := notifications.DeliveryParams{
		TenantID: "tnt_1", OutboxID: "outbox_1", DeliveryID: deliveryID,
		EventID: "alert_1", RuleID: "rule_1", Kind: kind, Channel: notifications.ChannelTelegram,
		RecipientID: "user_1", DestinationID: "destination_hash", TelegramChatID: 123,
		MembershipSnapshot: notifications.MembershipSnapshot{Role: "owner", Status: "active", Version: 1},
		TelegramContent:    content, CreatedAt: now, UpdatedAt: now,
	}
	if kind == notifications.NotificationKindRecovery {
		params.DependsOnOutboxID = "opening_outbox"
		params.DependsOnDeliveryID = "opening_delivery"
	}
	delivery, err := notifications.NewPendingDelivery(params)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := delivery.Snapshot()
	snapshot.State = notifications.DeliveryStateProcessing
	return worker.DeliveryRecord{Delivery: snapshot, Revision: 2, LeaseOwner: "worker", LeaseEpoch: 1}
}

func marshal(t *testing.T, item map[string]any) map[string]types.AttributeValue {
	t.Helper()
	encoded, err := attributevalue.MarshalMap(item)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
