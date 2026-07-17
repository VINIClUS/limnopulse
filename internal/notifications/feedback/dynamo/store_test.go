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
	if transaction.ClientRequestToken != nil {
		t.Fatalf("feedback transaction must rely on conditional dedupes, token=%q", *transaction.ClientRequestToken)
	}
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
		"provider_accepted", "completed_at", "succeeded", "accepted", "delivery_revision",
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

func TestReconcileMapsEveryFeedbackOutcomeAndCompletesFastAttempts(t *testing.T) {
	tests := []struct {
		name            string
		event           feedback.Event
		wantOutcome     notifications.ProviderOutcome
		wantState       notifications.DeliveryState
		wantAttempt     notifications.AttemptOutcome
		wantSuppression bool
	}{
		{name: "send", event: testEvent(feedback.SemanticSend), wantOutcome: notifications.ProviderOutcomeAccepted, wantState: notifications.DeliveryStateSucceeded, wantAttempt: notifications.AttemptOutcomeSucceeded},
		{name: "delivery", event: testEvent(feedback.SemanticDelivery), wantOutcome: notifications.ProviderOutcomeDeliveredToMailServer, wantState: notifications.DeliveryStateSucceeded, wantAttempt: notifications.AttemptOutcomeSucceeded},
		{name: "delay", event: testEvent(feedback.SemanticDeliveryDelay), wantOutcome: notifications.ProviderOutcomeDelayed, wantState: notifications.DeliveryStateSucceeded, wantAttempt: notifications.AttemptOutcomeSucceeded},
		{name: "soft bounce", event: testEvent(feedback.SemanticSoftBounce), wantOutcome: notifications.ProviderOutcomeSoftBounced, wantState: notifications.DeliveryStateSucceeded, wantAttempt: notifications.AttemptOutcomeSucceeded},
		{name: "hard bounce", event: testEvent(feedback.SemanticHardBounce), wantOutcome: notifications.ProviderOutcomeHardBounced, wantState: notifications.DeliveryStateSucceeded, wantAttempt: notifications.AttemptOutcomeSucceeded, wantSuppression: true},
		{name: "complaint", event: testEvent(feedback.SemanticComplaint), wantOutcome: notifications.ProviderOutcomeComplained, wantState: notifications.DeliveryStateSucceeded, wantAttempt: notifications.AttemptOutcomeSucceeded, wantSuppression: true},
		{name: "reject", event: testEvent(feedback.SemanticReject), wantOutcome: notifications.ProviderOutcomeRejected, wantState: notifications.DeliveryStatePermanentFailed, wantAttempt: notifications.AttemptOutcomePermanentFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient(t)
			client.seed(testAttempt("started", "", ""))
			client.seed(testDelivery("processing", "", 7))
			result, err := (Store{Table: "domain", Client: client}).Reconcile(context.Background(), test.event, testNow())
			if err != nil || result.Disposition != feedback.ReconcileApplied || result.Suppressed != test.wantSuppression {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			transaction := client.transactions[0]
			attemptUpdate := transaction.TransactItems[2].Update
			deliveryUpdate := transaction.TransactItems[3].Update
			attemptValues := decodeValues(t, attemptUpdate.ExpressionAttributeValues)
			deliveryValues := decodeValues(t, deliveryUpdate.ExpressionAttributeValues)
			if attemptValues[":next_outcome"] != string(test.wantAttempt) ||
				attemptValues[":provider_outcome"] != string(test.wantOutcome) ||
				deliveryValues[":next_state"] != string(test.wantState) ||
				deliveryValues[":provider_outcome"] != string(test.wantOutcome) {
				t.Fatalf("attempt=%#v delivery=%#v", attemptValues, deliveryValues)
			}
			if !strings.Contains(*attemptUpdate.UpdateExpression, "#completed_at") {
				t.Fatalf("fast feedback did not complete Attempt: %s", *attemptUpdate.UpdateExpression)
			}
		})
	}
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
		client.seed(testTransportMarker(t))
		result, err := (Store{Table: "domain", Client: client}).Reconcile(context.Background(), testEvent(feedback.SemanticSend), testNow())
		if err != nil || result.Disposition != feedback.ReconcileDuplicate || len(client.transactions) != 0 {
			t.Fatalf("result=%#v err=%v transactions=%d", result, err, len(client.transactions))
		}
	})

	t.Run("semantic duplicate records new transport identity", func(t *testing.T) {
		client := newFakeClient(t)
		client.seed(testSemanticMarker(t, feedback.SemanticSend))
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
	attempt := testAttempt("ambiguous", "", "")
	attempt["notification_kind"] = "recovery"
	client.seed(attempt)
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

func TestReconcileLateSendPromotesUnknownAndKeepsAcceptanceEvidence(t *testing.T) {
	client := newFakeClient(t)
	client.seed(testAttempt("ambiguous", "", ""))
	client.seed(testDelivery("unknown", "", 11))

	result, err := (Store{Table: "domain", Client: client}).Reconcile(
		context.Background(), testEvent(feedback.SemanticSend), testNow(),
	)
	if err != nil || result.Disposition != feedback.ReconcileApplied {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	deliveryValues := decodeValues(t, client.transactions[0].TransactItems[3].Update.ExpressionAttributeValues)
	if deliveryValues[":next_state"] != "succeeded" || deliveryValues[":provider_accepted"] != true {
		t.Fatalf("late acceptance values = %#v", deliveryValues)
	}
}

func TestReconcileFailsClosedOnAttemptDeliveryMetadataMismatch(t *testing.T) {
	client := newFakeClient(t)
	attempt := testAttempt("started", "", "")
	attempt["tenant_id"] = "other_tenant"
	client.seed(attempt)
	client.seed(testDelivery("processing", "", 7))

	result, err := (Store{Table: "domain", Client: client}).Reconcile(
		context.Background(), testEvent(feedback.SemanticSend), testNow(),
	)
	if err != nil || result.Disposition != feedback.ReconcileAwaitDLQ || len(client.transactions) != 0 {
		t.Fatalf("result=%#v err=%v transactions=%d", result, err, len(client.transactions))
	}
}

func TestReconcileRetriesConcurrentWorkerConflictWithFreshSnapshot(t *testing.T) {
	client := newFakeClient(t)
	client.seed(testAttempt("started", "", ""))
	client.seed(testDelivery("processing", "", 7))
	client.transactionErrors = []error{&types.TransactionCanceledException{}}
	client.onTransaction = func(call int) {
		if call != 1 {
			return
		}
		client.seed(testAttempt("succeeded", "accepted", "ses_message_1"))
		client.seed(testDelivery("succeeded", "accepted", 8))
	}

	result, err := (Store{Table: "domain", Client: client}).Reconcile(
		context.Background(), testEvent(feedback.SemanticDelivery), testNow(),
	)
	if err != nil || result.Disposition != feedback.ReconcileApplied || len(client.transactions) != 2 {
		t.Fatalf("result=%#v err=%v transactions=%d", result, err, len(client.transactions))
	}
	for _, transaction := range client.transactions {
		if transaction.ClientRequestToken != nil {
			t.Fatalf("retry used stale idempotency token %q", *transaction.ClientRequestToken)
		}
	}
	secondValues := decodeValues(t, client.transactions[1].TransactItems[3].Update.ExpressionAttributeValues)
	if numericInt64(secondValues[":current_revision"]) != 8 || secondValues[":provider_outcome"] != "delivered_to_mail_server" {
		t.Fatalf("retry did not use fresh snapshot: %#v", secondValues)
	}
}

func TestReconcileNeverDeletesOnCorruptDedupeAfterTransactionCancellation(t *testing.T) {
	client := newFakeClient(t)
	client.seed(testAttempt("started", "", ""))
	client.seed(testDelivery("processing", "", 7))
	client.transactionErrors = []error{&types.TransactionCanceledException{}}
	client.onTransaction = func(call int) {
		if call == 1 {
			key := mustTransportKey(t)
			client.seed(map[string]any{"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "corrupt"})
		}
	}

	result, err := (Store{Table: "domain", Client: client}).Reconcile(
		context.Background(), testEvent(feedback.SemanticSend), testNow(),
	)
	if err == nil || result.Disposition == feedback.ReconcileDuplicate {
		t.Fatalf("corrupt dedupe was treated as duplicate: result=%#v err=%v", result, err)
	}
}

func TestSemanticDuplicateValidatesTransportMarkerAfterConditionalCancellation(t *testing.T) {
	client := newFakeClient(t)
	client.seed(testSemanticMarker(t, feedback.SemanticSend))
	client.transactionErrors = []error{&types.TransactionCanceledException{}}
	client.onTransaction = func(call int) {
		if call == 1 {
			transport := mustTransportKey(t)
			client.seed(map[string]any{"PK": transport.PartitionKey, "SK": transport.SortKey, "entity_type": "corrupt"})
		}
	}

	result, err := (Store{Table: "domain", Client: client}).Reconcile(
		context.Background(), testEvent(feedback.SemanticSend), testNow(),
	)
	if err == nil || result.Disposition == feedback.ReconcileDuplicate {
		t.Fatalf("corrupt transport marker was accepted: result=%#v err=%v", result, err)
	}
}

type fakeClient struct {
	t                 *testing.T
	items             map[string]map[string]types.AttributeValue
	gets              []*awssdk.GetItemInput
	transactions      []*awssdk.TransactWriteItemsInput
	transactionErr    error
	transactionErrors []error
	onTransaction     func(int)
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
	if client.onTransaction != nil {
		client.onTransaction(len(client.transactions))
	}
	if len(client.transactionErrors) != 0 {
		err := client.transactionErrors[0]
		client.transactionErrors = client.transactionErrors[1:]
		return &awssdk.TransactWriteItemsOutput{}, err
	}
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
	case feedback.SemanticDeliveryDelay:
		event.ProviderOutcome, event.AcceptedEvidence = notifications.ProviderOutcomeDelayed, true
	case feedback.SemanticSoftBounce:
		event.ProviderOutcome, event.AcceptedEvidence = notifications.ProviderOutcomeSoftBounced, true
	case feedback.SemanticHardBounce:
		event.ProviderOutcome, event.AcceptedEvidence = notifications.ProviderOutcomeHardBounced, true
		event.SuppressionReason = feedback.SuppressionHardBounce
	case feedback.SemanticComplaint:
		event.ProviderOutcome, event.AcceptedEvidence = notifications.ProviderOutcomeComplained, true
		event.SuppressionReason = feedback.SuppressionComplaint
	case feedback.SemanticReject:
		event.ProviderOutcome = notifications.ProviderOutcomeRejected
		event.PermanentFailure = true
	}
	return event
}

func testAttempt(outcome, providerOutcome, providerMessageID string) map[string]any {
	item := map[string]any{
		"PK": "NOTIFICATION_DELIVERY#del_1", "SK": "ATTEMPT#att_1", "entity_type": "notification_attempt",
		"tenant_id": "tnt_1", "outbox_id": "outbox_1", "delivery_id": "del_1", "attempt_id": "att_1",
		"event_id": "alert_1", "notification_kind": "opening", "channel": "email", "outcome": outcome,
		"started_at": testNow().Add(-time.Minute).Format(time.RFC3339Nano),
	}
	if outcome != "started" {
		item["completed_at"] = testNow().Add(-30 * time.Second).Format(time.RFC3339Nano)
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

func testTransportMarker(t *testing.T) map[string]any {
	t.Helper()
	key := mustTransportKey(t)
	return map[string]any{
		"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "ses_feedback_transport_dedupe",
		"schema_version": int64(1), "event_bridge_id_hash": strings.TrimPrefix(key.PartitionKey, "SES_FEEDBACK_TRANSPORT#"),
		"created_at": testNow().Format(time.RFC3339Nano), "expires_at": testNow().Add(DefaultTransportRetention).Unix(),
	}
}

func testSemanticMarker(t *testing.T, semantic feedback.SemanticType) map[string]any {
	t.Helper()
	key := mustSemanticKey(t, semantic)
	return map[string]any{
		"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "ses_provider_event", "schema_version": int64(1),
		"provider_message_id": "ses_message_1", "semantic_type": string(semantic), "delivery_id": "del_1", "attempt_id": "att_1",
		"provider_outcome": string(testEvent(semantic).ProviderOutcome), "observed_at": testNow().Format(time.RFC3339Nano),
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

func decodeValues(t *testing.T, values map[string]types.AttributeValue) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := attributevalue.UnmarshalMap(values, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
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
