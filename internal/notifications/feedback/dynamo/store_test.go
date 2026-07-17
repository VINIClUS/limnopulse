package dynamo

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestReconcileWritesTransportSemanticAttemptAndDeliveryAtomically(t *testing.T) {
	client := newFakeClient(t)
	client.seed(testAttempt("started", "", ""))
	client.seed(testDelivery("processing", "", 7))
	store := Store{Table: "domain", Client: client}

	result, err := store.Reconcile(context.Background(), testEvent(feedback.SemanticSend), testNow())
	if err != nil {
		t.Fatal(err)
	}
	if result != (feedback.ReconcileResult{Disposition: feedback.ReconcileApplied}) {
		t.Fatalf("result = %#v", result)
	}
	if len(client.transactions) != 1 || len(client.transactions[0].TransactItems) != 4 {
		t.Fatalf("transactions = %#v", client.transactions)
	}
	transaction := client.transactions[0]
	transport := decodePut(t, transaction.TransactItems[0].Put)
	semantic := decodePut(t, transaction.TransactItems[1].Put)
	if transport["entity_type"] != "ses_feedback_transport_dedupe" ||
		transport["event_bridge_id_hash"] == "" || numericInt64(transport["expires_at"]) != testNow().Add(DefaultTransportRetention).Unix() {
		t.Fatalf("transport dedupe = %#v", transport)
	}
	if semantic["entity_type"] != "ses_provider_event" || semantic["expires_at"] != nil ||
		semantic["provider_message_id"] != "ses_message_1" || semantic["semantic_type"] != "send" {
		t.Fatalf("semantic event = %#v", semantic)
	}
	text := transactionText(transaction)
	for _, required := range []string{
		"notification_attempt", "notification_delivery", "provider_message_id", "provider_outcome",
		"provider_accepted", "succeeded", "accepted", "delivery_revision",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("transaction missing %q: %s", required, text)
		}
	}
	if strings.Contains(text, "owner@example.com") {
		t.Fatalf("transaction copied immutable email outside its delivery row: %s", text)
	}
	assertLookupOrder(t, client.gets, []notifications.StorageKey{
		mustTransportKey(t), mustSemanticKey(t, feedback.SemanticSend), mustAttemptKey(t), mustDeliveryKey(t),
	})
}

func TestReconcileSuppressesOnlyImmutableDeliveryAddressAndPreservesComplaintPrecedence(t *testing.T) {
	t.Run("hard bounce creates suppression from delivery snapshot", func(t *testing.T) {
		client := newFakeClient(t)
		client.seed(testAttempt("succeeded", "accepted", "ses_message_1"))
		client.seed(testDelivery("succeeded", "accepted", 8))
		store := Store{Table: "domain", Client: client}

		result, err := store.Reconcile(context.Background(), testEvent(feedback.SemanticHardBounce), testNow())
		if err != nil {
			t.Fatal(err)
		}
		if !result.Suppressed || len(client.transactions) != 1 || len(client.transactions[0].TransactItems) != 5 {
			t.Fatalf("result=%#v transactions=%#v", result, client.transactions)
		}
		suppression := client.transactions[0].TransactItems[4].Put
		if suppression == nil {
			t.Fatalf("suppression operation = %#v", client.transactions[0].TransactItems[4])
		}
		item := decodePut(t, suppression)
		wantKey, err := notifications.DeliverabilityStorageKey("owner@example.com")
		if err != nil {
			t.Fatal(err)
		}
		if item["PK"] != wantKey.PartitionKey || item["SK"] != wantKey.SortKey ||
			item["deliverability"] != "suppressed" || item["suppression_reason"] != "hard_bounce" ||
			numericInt64(item["suppression_rank"]) != 2 {
			t.Fatalf("suppression = %#v", item)
		}
		serialized := transactionText(client.transactions[0])
		if strings.Contains(serialized, "untrusted@example.com") || strings.Contains(serialized, "owner@example.com") {
			t.Fatalf("suppression transaction contains address: %s", serialized)
		}
	})

	t.Run("hard bounce after complaint does not downgrade suppression", func(t *testing.T) {
		client := newFakeClient(t)
		client.seed(testAttempt("succeeded", "hard_bounced", "ses_message_1"))
		client.seed(testDelivery("succeeded", "complained", 9))
		client.seed(testSuppression("complaint", 3))

		result, err := (Store{Table: "domain", Client: client}).Reconcile(
			context.Background(), testEvent(feedback.SemanticHardBounce), testNow(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.Disposition != feedback.ReconcileApplied || len(client.transactions) != 1 ||
			len(client.transactions[0].TransactItems) != 4 {
			t.Fatalf("result=%#v transaction=%#v", result, client.transactions)
		}
		if text := transactionText(client.transactions[0]); !strings.Contains(text, "complained") {
			t.Fatalf("provider outcome regressed: %s", text)
		}
	})
}

func TestReconcileReturnsDurableDuplicatesAndUnknownAssociations(t *testing.T) {
	t.Run("transport duplicate", func(t *testing.T) {
		client := newFakeClient(t)
		key := mustTransportKey(t)
		client.seed(map[string]any{"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "ses_feedback_transport_dedupe"})
		result, err := (Store{Table: "domain", Client: client}).Reconcile(context.Background(), testEvent(feedback.SemanticSend), testNow())
		if err != nil || result.Disposition != feedback.ReconcileDuplicate || len(client.transactions) != 0 {
			t.Fatalf("result=%#v err=%v transactions=%d", result, err, len(client.transactions))
		}
	})

	t.Run("semantic duplicate records new transport identity", func(t *testing.T) {
		client := newFakeClient(t)
		key := mustSemanticKey(t, feedback.SemanticSend)
		client.seed(map[string]any{"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "ses_provider_event"})
		result, err := (Store{Table: "domain", Client: client}).Reconcile(context.Background(), testEvent(feedback.SemanticSend), testNow())
		if err != nil || result.Disposition != feedback.ReconcileDuplicate || len(client.transactions) != 1 ||
			len(client.transactions[0].TransactItems) != 1 {
			t.Fatalf("result=%#v err=%v transactions=%#v", result, err, client.transactions)
		}
	})

	t.Run("missing attempt awaits DLQ without writes", func(t *testing.T) {
		client := newFakeClient(t)
		result, err := (Store{Table: "domain", Client: client}).Reconcile(context.Background(), testEvent(feedback.SemanticSend), testNow())
		if err != nil || result.Disposition != feedback.ReconcileAwaitDLQ || len(client.transactions) != 0 {
			t.Fatalf("result=%#v err=%v transactions=%d", result, err, len(client.transactions))
		}
	})

	t.Run("attempt provider identity mismatch awaits DLQ", func(t *testing.T) {
		client := newFakeClient(t)
		client.seed(testAttempt("succeeded", "accepted", "different_message"))
		result, err := (Store{Table: "domain", Client: client}).Reconcile(context.Background(), testEvent(feedback.SemanticDelivery), testNow())
		if err != nil || result.Disposition != feedback.ReconcileAwaitDLQ || len(client.transactions) != 0 {
			t.Fatalf("result=%#v err=%v transactions=%d", result, err, len(client.transactions))
		}
	})
}

func TestReconcileLateAcceptanceNeverResurrectsCancelledRecovery(t *testing.T) {
	client := newFakeClient(t)
	client.seed(testAttempt("ambiguous", "", ""))
	delivery := testDelivery("cancelled", "", 11)
	delivery["kind"] = "recovery"
	client.seed(delivery)

	result, err := (Store{Table: "domain", Client: client}).Reconcile(
		context.Background(), testEvent(feedback.SemanticSend), testNow(),
	)
	if err != nil || result.Disposition != feedback.ReconcileApplied {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	text := transactionText(client.transactions[0])
	if !strings.Contains(text, "cancelled") || strings.Contains(text, "#state = :succeeded") {
		t.Fatalf("cancelled recovery was resurrected: %s", text)
	}
}

type fakeClient struct {
	t              *testing.T
	items          map[string]map[string]types.AttributeValue
	gets           []*awssdk.GetItemInput
	transactions   []*awssdk.TransactWriteItemsInput
	transactionErr error
}

func newFakeClient(t *testing.T) *fakeClient {
	t.Helper()
	return &fakeClient{t: t, items: map[string]map[string]types.AttributeValue{}}
}

func (client *fakeClient) seed(item map[string]any) {
	encoded, err := attributevalue.MarshalMap(item)
	if err != nil {
		client.t.Fatal(err)
	}
	client.items[item["PK"].(string)+"\x00"+item["SK"].(string)] = encoded
}

func (client *fakeClient) GetItem(_ context.Context, input *awssdk.GetItemInput, _ ...func(*awssdk.Options)) (*awssdk.GetItemOutput, error) {
	client.gets = append(client.gets, input)
	var key map[string]string
	if err := attributevalue.UnmarshalMap(input.Key, &key); err != nil {
		client.t.Fatal(err)
	}
	return &awssdk.GetItemOutput{Item: client.items[key["PK"]+"\x00"+key["SK"]]}, nil
}

func (client *fakeClient) TransactWriteItems(_ context.Context, input *awssdk.TransactWriteItemsInput, _ ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error) {
	client.transactions = append(client.transactions, input)
	return &awssdk.TransactWriteItemsOutput{}, client.transactionErr
}

func testEvent(semantic feedback.SemanticType) feedback.Event {
	event := feedback.Event{
		EventBridgeID: "evt_1", ProviderMessageID: "ses_message_1", DeliveryID: "del_1", AttemptID: "att_1",
		SemanticType: semantic,
	}
	switch semantic {
	case feedback.SemanticSend:
		event.ProviderOutcome, event.AcceptedEvidence = notifications.ProviderOutcomeAccepted, true
	case feedback.SemanticDelivery:
		event.ProviderOutcome, event.AcceptedEvidence = notifications.ProviderOutcomeDeliveredToMailServer, true
	case feedback.SemanticHardBounce:
		event.ProviderOutcome, event.AcceptedEvidence = notifications.ProviderOutcomeHardBounced, true
		event.SuppressionReason = feedback.SuppressionHardBounce
	}
	return event
}

func testAttempt(outcome, providerOutcome, providerMessageID string) map[string]any {
	item := map[string]any{
		"PK": "NOTIFICATION_DELIVERY#del_1", "SK": "ATTEMPT#att_1", "entity_type": "notification_attempt",
		"delivery_id": "del_1", "attempt_id": "att_1", "outbox_id": "outbox_1", "outcome": outcome,
	}
	if providerOutcome != "" {
		item["provider_outcome"] = providerOutcome
	}
	if providerMessageID != "" {
		item["provider_message_id"] = providerMessageID
	}
	return item
}

func testDelivery(state, providerOutcome string, revision int64) map[string]any {
	item := map[string]any{
		"PK": "NOTIFICATION_OUTBOX#outbox_1", "SK": "DELIVERY#del_1", "entity_type": "notification_delivery",
		"tenant_id": "tnt_1", "outbox_id": "outbox_1", "delivery_id": "del_1", "event_id": "alert_1",
		"kind": "opening", "channel": "email", "normalized_email": "owner@example.com", "state": state,
		"delivery_revision": revision,
	}
	if providerOutcome != "" {
		item["provider_outcome"] = providerOutcome
	}
	return item
}

func testSuppression(reason string, rank int64) map[string]any {
	key, _ := notifications.DeliverabilityStorageKey("owner@example.com")
	return map[string]any{
		"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "email_deliverability",
		"deliverability": "suppressed", "suppression_reason": reason, "suppression_rank": rank,
	}
}

func testNow() time.Time { return time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC) }

func mustTransportKey(t *testing.T) notifications.StorageKey {
	t.Helper()
	key, err := feedback.TransportDedupeKey("evt_1")
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustSemanticKey(t *testing.T, semantic feedback.SemanticType) notifications.StorageKey {
	t.Helper()
	key, err := feedback.SemanticDedupeKey("ses_message_1", semantic)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustAttemptKey(t *testing.T) notifications.StorageKey {
	t.Helper()
	key, err := notifications.AttemptStorageKey("del_1", "att_1")
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustDeliveryKey(t *testing.T) notifications.StorageKey {
	t.Helper()
	key, err := notifications.DeliveryStorageKey("outbox_1", "del_1")
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func assertLookupOrder(t *testing.T, gets []*awssdk.GetItemInput, want []notifications.StorageKey) {
	t.Helper()
	if len(gets) < len(want) {
		t.Fatalf("GetItem calls = %d, want at least %d", len(gets), len(want))
	}
	for index, key := range want {
		var got map[string]string
		if err := attributevalue.UnmarshalMap(gets[index].Key, &got); err != nil {
			t.Fatal(err)
		}
		if got["PK"] != key.PartitionKey || got["SK"] != key.SortKey || gets[index].ConsistentRead == nil || !*gets[index].ConsistentRead {
			t.Fatalf("lookup %d = %#v consistent=%v, want %#v", index, got, gets[index].ConsistentRead, key)
		}
	}
}

func decodePut(t *testing.T, put *types.Put) map[string]any {
	t.Helper()
	if put == nil {
		t.Fatal("expected Put operation")
	}
	var item map[string]any
	if err := attributevalue.UnmarshalMap(put.Item, &item); err != nil {
		t.Fatal(err)
	}
	return item
}

func transactionText(input *awssdk.TransactWriteItemsInput) string {
	var builder strings.Builder
	for _, operation := range input.TransactItems {
		if operation.Put != nil {
			for name, value := range operation.Put.Item {
				builder.WriteString(name)
				var decoded any
				_ = attributevalue.Unmarshal(value, &decoded)
				builder.WriteString(fmt.Sprint(decoded))
			}
			if operation.Put.ConditionExpression != nil {
				builder.WriteString(*operation.Put.ConditionExpression)
			}
		}
		if operation.Update != nil {
			builder.WriteString(*operation.Update.UpdateExpression)
			builder.WriteString(*operation.Update.ConditionExpression)
			for token, name := range operation.Update.ExpressionAttributeNames {
				builder.WriteString(token + name)
			}
			for token, value := range operation.Update.ExpressionAttributeValues {
				builder.WriteString(token)
				var decoded any
				_ = attributevalue.Unmarshal(value, &decoded)
				builder.WriteString(fmt.Sprint(decoded))
			}
		}
	}
	return builder.String()
}

func numericInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case float64:
		return int64(typed)
	default:
		return 0
	}
}
