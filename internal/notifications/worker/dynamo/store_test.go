package dynamo

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestAcquireClaimsQueuedDeliveryWithDurableLeaseAndRevision(t *testing.T) {
	now := testNow()
	queued := testDeliveryItem(t, notifications.DeliveryStateQueued)
	processing := cloneMap(queued)
	processing["state"] = "processing"
	processing["delivery_revision"] = int64(4)
	processing["delivery_lease_owner"] = "worker_1"
	processing["delivery_lease_epoch"] = int64(1)
	processing["delivery_lease_expires_at"] = now.Add(time.Minute).Format(time.RFC3339Nano)
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, queued)}, updateItems: []map[string]types.AttributeValue{marshal(t, processing)}}
	store := Store{Table: "domain", Client: client}

	result, err := store.Acquire(context.Background(), testJob(t), worker.ClaimRequest{
		Owner: "worker_1", Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != worker.AcquireClaimed || result.Record.Revision != 4 ||
		result.Record.LeaseOwner != "worker_1" || result.Record.LeaseEpoch != 1 {
		t.Fatalf("claim = %#v", result)
	}
	if len(client.updates) != 1 {
		t.Fatalf("updates = %d", len(client.updates))
	}
	update := client.updates[0]
	for _, fragment := range []string{"#state = :state", "#revision = :revision", "#lease_owner", "#lease_epoch", "#lease_expires"} {
		if !strings.Contains(*update.ConditionExpression, fragment) && !strings.Contains(*update.UpdateExpression, fragment) {
			t.Errorf("claim mutation missing %q: condition=%s update=%s", fragment, *update.ConditionExpression, *update.UpdateExpression)
		}
	}
	for _, name := range []string{"delivery_lease_owner", "delivery_lease_epoch", "delivery_lease_expires_at"} {
		if !containsName(update.ExpressionAttributeNames, name) {
			t.Errorf("claim mutation missing durable field %q", name)
		}
	}
}

func TestAcquireRepairsPendingPublicationOnlyAfterRelayLeaseExpires(t *testing.T) {
	now := testNow()
	pending := testDeliveryItem(t, notifications.DeliveryStatePending)
	pending["relay_lease_owner"] = "relay_1"
	pending["relay_lease_epoch"] = int64(2)
	pending["relay_lease_expires_at"] = now.Add(time.Second).Format(time.RFC3339Nano)
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, pending)}}
	result, err := (Store{Table: "domain", Client: client}).Acquire(context.Background(), testJob(t), worker.ClaimRequest{
		Owner: "worker_1", SQSMessageID: "sqs_repair_1", Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != worker.AcquireDeferred || result.RetryAfter != time.Second || len(client.updates) != 0 {
		t.Fatalf("active relay lease result=%#v updates=%d", result, len(client.updates))
	}

	pending["relay_lease_expires_at"] = now.Add(-time.Second).Format(time.RFC3339Nano)
	queued := cloneMap(pending)
	queued["state"] = "queued"
	queued["delivery_revision"] = int64(4)
	delete(queued, "relay_lease_owner")
	delete(queued, "relay_lease_epoch")
	delete(queued, "relay_lease_expires_at")
	processing := cloneMap(queued)
	processing["state"] = "processing"
	processing["delivery_revision"] = int64(5)
	processing["delivery_lease_owner"] = "worker_1"
	processing["delivery_lease_epoch"] = int64(1)
	processing["delivery_lease_expires_at"] = now.Add(time.Minute).Format(time.RFC3339Nano)
	client = &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, pending)}, updateItems: []map[string]types.AttributeValue{marshal(t, queued), marshal(t, processing)}}
	result, err = (Store{Table: "domain", Client: client}).Acquire(context.Background(), testJob(t), worker.ClaimRequest{
		Owner: "worker_1", SQSMessageID: "sqs_repair_1", Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != worker.AcquireClaimed || len(client.updates) != 2 {
		t.Fatalf("repair claim=%#v updates=%d", result, len(client.updates))
	}
	if !strings.Contains(*client.updates[0].ConditionExpression, "#relay_lease_expires <= :now") ||
		!strings.Contains(*client.updates[0].UpdateExpression, "#state = :queued") ||
		!strings.Contains(*client.updates[0].UpdateExpression, "#message_id = if_not_exists") {
		t.Fatalf("repair mutation = condition=%s update=%s", *client.updates[0].ConditionExpression, *client.updates[0].UpdateExpression)
	}
}

func TestAcquireExpiredProcessingLoadsStartedAttemptAsAmbiguousFence(t *testing.T) {
	now := testNow()
	processing := testDeliveryItem(t, notifications.DeliveryStateProcessing)
	processing["delivery_lease_owner"] = "dead_worker"
	processing["delivery_lease_epoch"] = int64(3)
	processing["delivery_lease_expires_at"] = now.Add(-time.Second).Format(time.RFC3339Nano)
	processing["attempt_count"] = int64(1)
	processing["last_attempt_id"] = "att_started"
	attempt := map[string]any{
		"PK": processingAttemptPK(t), "SK": "ATTEMPT#att_started", "entity_type": "notification_attempt",
		"delivery_id": testJob(t).DeliveryID, "attempt_id": "att_started", "outcome": "started",
	}
	claimed := cloneMap(processing)
	claimed["delivery_revision"] = int64(4)
	claimed["delivery_lease_owner"] = "worker_1"
	claimed["delivery_lease_epoch"] = int64(4)
	claimed["delivery_lease_expires_at"] = now.Add(time.Minute).Format(time.RFC3339Nano)
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, processing), marshal(t, attempt)}, updateItems: []map[string]types.AttributeValue{marshal(t, claimed)}}
	result, err := (Store{Table: "domain", Client: client}).Acquire(context.Background(), testJob(t), worker.ClaimRequest{
		Owner: "worker_1", Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Record.StartedAttemptID != "att_started" || result.Record.AttemptCount != 1 {
		t.Fatalf("expired started attempt result = %#v", result)
	}
	if len(client.gets) != 2 || !*client.gets[1].ConsistentRead {
		t.Fatalf("attempt lookup = %#v", client.gets)
	}
}

func TestAcquireCrashAfterClaimBeforeBeginReclaimsAfterCompletedPreviousAttempt(t *testing.T) {
	now := testNow()
	processing := testDeliveryItem(t, notifications.DeliveryStateProcessing)
	processing["delivery_lease_owner"] = "dead_worker"
	processing["delivery_lease_epoch"] = int64(4)
	processing["delivery_lease_expires_at"] = now.Add(-time.Second).Format(time.RFC3339Nano)
	processing["attempt_count"] = int64(1)
	processing["last_attempt_id"] = "att_previous"
	attempt := map[string]any{
		"PK": processingAttemptPK(t), "SK": "ATTEMPT#att_previous", "entity_type": "notification_attempt",
		"delivery_id": testJob(t).DeliveryID, "attempt_id": "att_previous", "outcome": "retryable",
	}
	claimed := cloneMap(processing)
	claimed["delivery_revision"] = int64(4)
	claimed["delivery_lease_owner"] = "worker_1"
	claimed["delivery_lease_epoch"] = int64(5)
	claimed["delivery_lease_expires_at"] = now.Add(time.Minute).Format(time.RFC3339Nano)
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, processing), marshal(t, attempt)}, updateItems: []map[string]types.AttributeValue{marshal(t, claimed)}}
	result, err := (Store{Table: "domain", Client: client}).Acquire(context.Background(), testJob(t), worker.ClaimRequest{
		Owner: "worker_1", Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != worker.AcquireClaimed || result.Record.StartedAttemptID != "" {
		t.Fatalf("reclaim result = %#v", result)
	}
}

func TestAcquireDefersAmbiguousExhaustionUntilFinalGrace(t *testing.T) {
	now := testNow()
	item := testDeliveryItem(t, notifications.DeliveryStateRetryableFailed)
	item["attempt_count"] = int64(5)
	item["possibly_accepted"] = true
	item["ambiguous_exhausted"] = true
	item["next_attempt_at"] = now.Add(90 * time.Second).Format(time.RFC3339Nano)
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, item)}}
	result, err := (Store{Table: "domain", Client: client}).Acquire(context.Background(), testJob(t), worker.ClaimRequest{
		Owner: "worker_1", Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != worker.AcquireDeferred || result.RetryAfter != 90*time.Second || len(client.updates) != 0 {
		t.Fatalf("final grace result=%#v updates=%d", result, len(client.updates))
	}
}

func TestAcquireMalformedProviderOutcomeAwaitsDLQWithoutClaim(t *testing.T) {
	now := testNow()
	item := testDeliveryItem(t, notifications.DeliveryStateQueued)
	item["provider_outcome"] = "invented"
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, item)}}
	result, err := (Store{Table: "domain", Client: client}).Acquire(context.Background(), testJob(t), worker.ClaimRequest{
		Owner: "worker_1", Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != worker.AcquireAwaitDLQ || len(client.updates) != 0 {
		t.Fatalf("result=%#v updates=%d", result, len(client.updates))
	}
}

func TestCheckGatesRevalidatesMembershipAndAddressDeliverability(t *testing.T) {
	record := testRecord(t)
	active := map[string]any{"PK": "TENANT#tnt_1", "SK": "MEMBER#user_1", "entity_type": "membership", "tenant_id": "tnt_1", "cognito_sub": "user_1", "status": "active", "role": "owner", "version": int64(2)}
	suppressed := map[string]any{"deliverability": "suppressed"}
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, active), marshal(t, suppressed)}}
	gate, err := (Store{Table: "domain", Client: client}).CheckGates(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if gate.Allowed || gate.CancellationReason != notifications.CancellationReasonEmailSuppressed {
		t.Fatalf("suppressed gate = %#v", gate)
	}
	if len(client.gets) != 2 || !*client.gets[0].ConsistentRead || !*client.gets[1].ConsistentRead {
		t.Fatal("gates were not read consistently")
	}

	client = &fakeClient{getItems: []map[string]types.AttributeValue{nil}}
	gate, err = (Store{Table: "domain", Client: client}).CheckGates(context.Background(), record)
	if err != nil || gate.CancellationReason != notifications.CancellationReasonRecipientMembershipInactive {
		t.Fatalf("missing membership gate=%#v err=%v", gate, err)
	}
}

func TestAttemptTransactionsFenceLeaseAndNeverCopyRenderedContentOrEmail(t *testing.T) {
	record := testRecord(t)
	client := &fakeClient{}
	store := Store{Table: "domain", Client: client}
	started, err := store.BeginAttempt(context.Background(), record, worker.BeginAttemptRequest{
		AttemptID: "att_1", StartedAt: testNow(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.AttemptCount != record.AttemptCount+1 || started.Revision != record.Revision+1 {
		t.Fatalf("started record = %#v", started)
	}
	if len(client.transactions) != 1 || len(client.transactions[0].TransactItems) != 2 {
		t.Fatalf("begin transactions = %#v", client.transactions)
	}
	begin := client.transactions[0]
	serialized := transactionText(begin)
	for _, required := range []string{"delivery_lease_owner", "delivery_lease_epoch", "delivery_revision", "attempt_count", "content_hash"} {
		if !strings.Contains(serialized, required) {
			t.Errorf("begin transaction missing %q: %s", required, serialized)
		}
	}
	for _, forbidden := range []string{"normalized_email", "subject", "html", "text"} {
		if strings.Contains(serialized, forbidden) {
			t.Errorf("attempt transaction copied %q", forbidden)
		}
	}

	err = store.CompleteAttempt(context.Background(), started, worker.AttemptCompletion{
		AttemptID: "att_1", CompletedAt: testNow().Add(time.Second),
		Outcome:           notifications.AttemptOutcomeSucceeded,
		NextState:         notifications.DeliveryStateSucceeded,
		ProviderOutcome:   notifications.ProviderOutcomeAccepted,
		ProviderMessageID: "ses_1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(client.transactions) != 2 {
		t.Fatalf("complete transactions = %d", len(client.transactions))
	}
	complete := transactionText(client.transactions[1])
	for _, required := range []string{"delivery_lease_owner", "delivery_lease_epoch", "last_attempt_id", "started", "succeeded", "accepted"} {
		if !strings.Contains(complete, required) {
			t.Errorf("completion transaction missing %q: %s", required, complete)
		}
	}
}

type fakeClient struct {
	getItems     []map[string]types.AttributeValue
	updateItems  []map[string]types.AttributeValue
	gets         []*awssdk.GetItemInput
	updates      []*awssdk.UpdateItemInput
	transactions []*awssdk.TransactWriteItemsInput
}

func (client *fakeClient) GetItem(_ context.Context, input *awssdk.GetItemInput, _ ...func(*awssdk.Options)) (*awssdk.GetItemOutput, error) {
	client.gets = append(client.gets, input)
	var item map[string]types.AttributeValue
	if len(client.getItems) > 0 {
		item, client.getItems = client.getItems[0], client.getItems[1:]
	}
	return &awssdk.GetItemOutput{Item: item}, nil
}
func (client *fakeClient) UpdateItem(_ context.Context, input *awssdk.UpdateItemInput, _ ...func(*awssdk.Options)) (*awssdk.UpdateItemOutput, error) {
	client.updates = append(client.updates, input)
	var item map[string]types.AttributeValue
	if len(client.updateItems) > 0 {
		item, client.updateItems = client.updateItems[0], client.updateItems[1:]
	}
	return &awssdk.UpdateItemOutput{Attributes: item}, nil
}
func (client *fakeClient) TransactWriteItems(_ context.Context, input *awssdk.TransactWriteItemsInput, _ ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error) {
	client.transactions = append(client.transactions, input)
	return &awssdk.TransactWriteItemsOutput{}, nil
}

func testDeliveryItem(t *testing.T, state notifications.DeliveryState) map[string]any {
	t.Helper()
	snapshot := testSnapshot(t)
	return map[string]any{
		"PK": "NOTIFICATION_OUTBOX#" + snapshot.OutboxID, "SK": "DELIVERY#" + snapshot.DeliveryID,
		"entity_type": "notification_delivery", "relay_schema_version": int64(1),
		"tenant_id": snapshot.TenantID, "outbox_id": snapshot.OutboxID, "delivery_id": snapshot.DeliveryID,
		"event_id": snapshot.EventID, "rule_id": snapshot.RuleID, "kind": string(snapshot.Kind),
		"channel": string(snapshot.Channel), "recipient_id": snapshot.RecipientID,
		"normalized_email":    snapshot.NormalizedEmail,
		"membership_snapshot": map[string]any{"role": "owner", "status": "active", "version": int64(1)},
		"state":               string(state), "delivery_revision": int64(3), "attempt_count": int64(0),
		"content": map[string]any{
			"template_id": string(snapshot.Content.TemplateID), "template_version": snapshot.Content.TemplateVersion,
			"locale": string(snapshot.Content.Locale), "subject": snapshot.Content.Subject,
			"text": snapshot.Content.Text, "html": snapshot.Content.HTML, "content_hash": snapshot.Content.ContentHash,
		},
		"created_at": snapshot.CreatedAt.Format(time.RFC3339Nano), "updated_at": snapshot.UpdatedAt.Format(time.RFC3339Nano),
	}
}

func testRecord(t *testing.T) worker.DeliveryRecord {
	t.Helper()
	snapshot := testSnapshot(t)
	snapshot.State = notifications.DeliveryStateProcessing
	return worker.DeliveryRecord{Delivery: snapshot, Revision: 3, AttemptCount: 0,
		LeaseOwner: "worker_1", LeaseEpoch: 1, LeaseExpiresAt: testNow().Add(time.Minute)}
}

func testSnapshot(t *testing.T) notifications.DeliverySnapshot {
	t.Helper()
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		t.Fatal(err)
	}
	now := testNow().Add(-time.Hour)
	content, err := renderer.Render(notifications.TemplateAlertOpeningV1, notifications.LocalePTBR,
		notifications.EmailTemplateData{RuleName: "Rule", Severity: "critical", TenantID: "tnt_1",
			PondID: "pond_1", DeviceID: "dev_1", Metric: "temperature", Unit: "°C", Operator: ">",
			Threshold: 30, EvaluationWindow: time.Minute, WindowStart: now.Add(-time.Minute), WindowEnd: now,
			EvaluatedAt: now, EventID: "alert_1"})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := notifications.NewDeliveryID("alert_1", notifications.NotificationKindOpening,
		notifications.ChannelEmail, "user_1")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := notifications.NewPendingDelivery(notifications.DeliveryParams{
		TenantID: "tnt_1", OutboxID: "outbox_1", DeliveryID: deliveryID, EventID: "alert_1", RuleID: "rule_1",
		Kind: notifications.NotificationKindOpening, Channel: notifications.ChannelEmail,
		RecipientID: "user_1", NormalizedEmail: "owner@example.com",
		MembershipSnapshot: notifications.MembershipSnapshot{Role: "owner", Status: "active", Version: 1},
		Content:            content, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return delivery.Snapshot()
}

func testJob(t *testing.T) notifications.JobEnvelope {
	t.Helper()
	snapshot := testSnapshot(t)
	job, err := notifications.NewDeliveryJob(snapshot.TenantID, snapshot.OutboxID, snapshot.DeliveryID,
		snapshot.EventID, snapshot.Kind, snapshot.Channel)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func processingAttemptPK(t *testing.T) string {
	t.Helper()
	return "NOTIFICATION_DELIVERY#" + testJob(t).DeliveryID
}

func marshal(t *testing.T, value map[string]any) map[string]types.AttributeValue {
	t.Helper()
	encoded, err := attributevalue.MarshalMap(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func containsName(names map[string]string, value string) bool {
	for _, name := range names {
		if name == value {
			return true
		}
	}
	return false
}

func transactionText(input *awssdk.TransactWriteItemsInput) string {
	var builder strings.Builder
	for _, item := range input.TransactItems {
		if item.Update != nil {
			if item.Update.UpdateExpression != nil {
				builder.WriteString(*item.Update.UpdateExpression)
			}
			if item.Update.ConditionExpression != nil {
				builder.WriteString(*item.Update.ConditionExpression)
			}
			for token, name := range item.Update.ExpressionAttributeNames {
				builder.WriteString(token + name)
			}
			for token, value := range item.Update.ExpressionAttributeValues {
				builder.WriteString(token)
				var decoded any
				_ = attributevalue.Unmarshal(value, &decoded)
				builder.WriteString(strings.TrimSpace(toString(decoded)))
			}
		}
		if item.Put != nil {
			if item.Put.ConditionExpression != nil {
				builder.WriteString(*item.Put.ConditionExpression)
			}
			for name, value := range item.Put.Item {
				builder.WriteString(name)
				var decoded any
				_ = attributevalue.Unmarshal(value, &decoded)
				builder.WriteString(strings.TrimSpace(toString(decoded)))
			}
		}
	}
	return builder.String()
}

func toString(value any) string {
	return strings.TrimSpace(fmt.Sprint(value))
}

func testNow() time.Time { return time.Date(2026, 7, 16, 15, 0, 0, 0, time.UTC) }
