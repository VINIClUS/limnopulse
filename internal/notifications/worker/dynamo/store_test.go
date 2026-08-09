package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	feedbackdynamo "github.com/VINIClUS/limnopulse/internal/notifications/feedback/dynamo"
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

func TestAcquireTreatsInterventionTimestampAsRepairHold(t *testing.T) {
	now := testNow()
	t.Run("future hold remains deferred", func(t *testing.T) {
		item := testDeliveryItem(t, notifications.DeliveryStateRetryableFailed)
		item["awaiting_intervention"] = true
		item["next_attempt_at"] = now.Add(15 * time.Minute).Format(time.RFC3339Nano)
		client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, item)}}

		result, err := (Store{Table: "domain", Client: client}).Acquire(
			context.Background(), testJob(t), worker.ClaimRequest{
				Owner: "worker_1", Now: now, ExpiresAt: now.Add(time.Minute),
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.Disposition != worker.AcquireDeferred || result.RetryAfter != 15*time.Minute ||
			len(client.updates) != 0 {
			t.Fatalf("future intervention hold result=%#v updates=%d", result, len(client.updates))
		}
	})

	t.Run("due hold is claimed and cleared atomically", func(t *testing.T) {
		item := testDeliveryItem(t, notifications.DeliveryStateRetryableFailed)
		item["awaiting_intervention"] = true
		item["next_attempt_at"] = now.Add(-time.Second).Format(time.RFC3339Nano)
		claimed := cloneMap(item)
		claimed["state"] = string(notifications.DeliveryStateProcessing)
		claimed["delivery_revision"] = int64(4)
		claimed["delivery_lease_owner"] = "worker_1"
		claimed["delivery_lease_epoch"] = int64(1)
		claimed["delivery_lease_expires_at"] = now.Add(time.Minute).Format(time.RFC3339Nano)
		delete(claimed, "awaiting_intervention")
		delete(claimed, "next_attempt_at")
		client := &fakeClient{
			getItems:    []map[string]types.AttributeValue{marshal(t, item)},
			updateItems: []map[string]types.AttributeValue{marshal(t, claimed)},
		}

		result, err := (Store{Table: "domain", Client: client}).Acquire(
			context.Background(), testJob(t), worker.ClaimRequest{
				Owner: "worker_1", Now: now, ExpiresAt: now.Add(time.Minute),
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.Disposition != worker.AcquireClaimed || result.Record.AwaitingIntervention ||
			!result.Record.NextAttemptAt.IsZero() || len(client.updates) != 1 {
			t.Fatalf("due intervention hold result=%#v updates=%d", result, len(client.updates))
		}
		update := client.updates[0]
		if !strings.Contains(*update.ConditionExpression, "#revision = :revision") ||
			!strings.Contains(*update.UpdateExpression, "REMOVE #next_attempt_at, #awaiting_intervention") {
			t.Fatalf("intervention repair claim condition=%s update=%s", *update.ConditionExpression, *update.UpdateExpression)
		}
	})
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
	active := map[string]any{"PK": "TENANT#tnt_1", "SK": "MEMBER#user_1", "entity_type": "tenant_member", "tenant_id": "tnt_1", "cognito_sub": "user_1", "status": "active", "role": "owner", "version": int64(2)}
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

func TestCompletePreflightFailureFencesClaimWithoutCreatingAttemptAndTreatsTerminalRaceAsSafe(t *testing.T) {
	t.Run("permanent recipient", func(t *testing.T) {
		record := testRecord(t)
		client := &fakeClient{}
		err := (Store{Table: "domain", Client: client}).CompletePreflightFailure(
			context.Background(), record, worker.PreflightFailureCompletion{
				CompletedAt: testNow(), ErrorCategory: string(worker.ErrorPermanentRecipient),
				NextState: notifications.DeliveryStatePermanentFailed,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(client.updates) != 0 || len(client.transactions) != 1 ||
			len(client.transactions[0].TransactItems) != 2 {
			t.Fatalf("updates=%d transactions=%d", len(client.updates), len(client.transactions))
		}
		update := client.transactions[0].TransactItems[0].Update
		if update == nil || client.transactions[0].TransactItems[1].ConditionCheck == nil {
			t.Fatalf("preflight terminal transaction = %#v", client.transactions[0].TransactItems)
		}
		for _, required := range []string{"#state = :next_state", "#revision = :next_revision", "#last_error_category = :error_category", "#lease_owner = :owner", "#lease_epoch = :epoch"} {
			if !strings.Contains(*update.UpdateExpression+*update.ConditionExpression, required) {
				t.Errorf("preflight mutation missing %q: update=%s condition=%s", required, *update.UpdateExpression, *update.ConditionExpression)
			}
		}
		if text := *update.UpdateExpression; strings.Contains(text, "attempt_count") || strings.Contains(text, "last_attempt_id") {
			t.Fatalf("preflight mutation touched Attempt accounting: %s", text)
		}
	})

	t.Run("concurrent terminal", func(t *testing.T) {
		record := testRecord(t)
		terminal := testDeliveryItem(t, notifications.DeliveryStateSucceeded)
		terminal["delivery_revision"] = record.Revision + 1
		client := &fakeClient{
			getItems:          []map[string]types.AttributeValue{nil, marshal(t, terminal)},
			transactionErrors: []error{&types.TransactionCanceledException{}},
		}
		err := (Store{Table: "domain", Client: client}).CompletePreflightFailure(
			context.Background(), record, worker.PreflightFailureCompletion{
				CompletedAt: testNow(), ErrorCategory: string(worker.ErrorPermanentRecipient),
				NextState: notifications.DeliveryStatePermanentFailed,
			},
		)
		if !errors.Is(err, worker.ErrConcurrentTerminal) || len(client.updates) != 0 ||
			len(client.transactions) != 1 || len(client.gets) != 2 {
			t.Fatalf("error=%v updates=%d gets=%d", err, len(client.updates), len(client.gets))
		}
	})
}

func TestAttemptTransactionsFenceLeaseAndNeverCopyRenderedContentOrEmail(t *testing.T) {
	record := testRecord(t)
	client := &fakeClient{}
	store := Store{Table: "domain", Client: client}
	started, err := store.BeginAttempt(context.Background(), record, worker.BeginAttemptRequest{
		AttemptID: "att_1", StartedAt: testNow(), LeaseRequiredUntil: testNow().Add(20 * time.Second),
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
	if condition := *begin.TransactItems[0].Update.ConditionExpression; !strings.Contains(condition, "#lease_expires >= :lease_required_until") {
		t.Fatalf("begin condition does not fence an expired claim: %s", condition)
	}
	values := decodeExpressionValues(t, begin.TransactItems[0].Update.ExpressionAttributeValues)
	if values[":lease_required_until"] != fixedTime(testNow().Add(20*time.Second)) {
		t.Fatalf("lease headroom value = %#v", values[":lease_required_until"])
	}
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
	for _, required := range []string{"delivery_lease_owner", "delivery_lease_epoch", "last_attempt_id", "provider_attempt_id", "started", "succeeded", "accepted"} {
		if !strings.Contains(complete, required) {
			t.Errorf("completion transaction missing %q: %s", required, complete)
		}
	}
}

func TestCompleteAttemptReportsConcurrentTerminalFeedbackAsSafeNoOp(t *testing.T) {
	record := testRecord(t)
	record.AttemptCount = 1
	record.LastAttemptID = "att_1"
	terminal := testDeliveryItem(t, notifications.DeliveryStateSucceeded)
	terminal["delivery_revision"] = record.Revision + 1
	terminal["attempt_count"] = int64(1)
	terminal["last_attempt_id"] = "att_1"
	terminal["provider_outcome"] = "accepted"
	terminal["provider_message_id"] = "ses_message_1"
	completedAttempt := testAttemptItem(record, "att_1", notifications.AttemptOutcomeSucceeded)
	completedAttempt["provider_outcome"] = "accepted"
	completedAttempt["provider_message_id"] = "ses_message_1"
	completedAttempt["completed_at"] = testNow().Format(time.RFC3339Nano)
	client := &fakeClient{
		getItems:          []map[string]types.AttributeValue{nil, marshal(t, terminal), marshal(t, completedAttempt)},
		transactionErrors: []error{&types.TransactionCanceledException{}},
	}

	err := (Store{Table: "domain", Client: client}).CompleteAttempt(
		context.Background(), record, worker.AttemptCompletion{
			AttemptID: "att_1", CompletedAt: testNow(),
			Outcome: notifications.AttemptOutcomeSucceeded, NextState: notifications.DeliveryStateSucceeded,
			ProviderOutcome: notifications.ProviderOutcomeAccepted, ProviderMessageID: "ses_message_1",
		},
	)
	if !errors.Is(err, worker.ErrConcurrentTerminal) {
		t.Fatalf("CompleteAttempt() error = %v", err)
	}
	if len(client.gets) != 3 || len(client.transactions) != 1 || len(client.updates) != 0 ||
		client.gets[0].ConsistentRead == nil || !*client.gets[0].ConsistentRead ||
		client.gets[1].ConsistentRead == nil || !*client.gets[1].ConsistentRead ||
		client.gets[2].ConsistentRead == nil || !*client.gets[2].ConsistentRead {
		t.Fatalf("terminal reconciliation reads = %#v", client.gets)
	}
}

func TestCompleteAttemptPromotesWaitingRecoveryForPreviouslyNonterminalOpening(t *testing.T) {
	record := testRecord(t)
	record.AttemptCount = 1
	record.LastAttemptID = "att_1"
	recoveryOutboxID := notifications.OutboxID(
		record.Delivery.EventID, record.Delivery.Channel, notifications.NotificationKindRecovery,
	)
	recoveryDeliveryID, err := notifications.NewDeliveryID(
		record.Delivery.EventID, notifications.NotificationKindRecovery,
		record.Delivery.Channel, record.Delivery.RecipientID,
	)
	if err != nil {
		t.Fatal(err)
	}
	recovery := testDeliveryItem(t, notifications.DeliveryStateWaitingDependency)
	recovery["PK"] = "NOTIFICATION_OUTBOX#" + recoveryOutboxID
	recovery["SK"] = "DELIVERY#" + recoveryDeliveryID
	recovery["outbox_id"] = recoveryOutboxID
	recovery["delivery_id"] = recoveryDeliveryID
	recovery["kind"] = "recovery"
	recovery["depends_on_outbox_id"] = record.Delivery.OutboxID
	recovery["depends_on_delivery_id"] = record.Delivery.DeliveryID
	recovery["delivery_revision"] = int64(1)
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, recovery)}}

	err = (Store{Table: "domain", Client: client}).CompleteAttempt(
		context.Background(), record, worker.AttemptCompletion{
			AttemptID: "att_1", CompletedAt: testNow(),
			Outcome: notifications.AttemptOutcomeSucceeded, NextState: notifications.DeliveryStateSucceeded,
			ProviderOutcome: notifications.ProviderOutcomeAccepted, ProviderMessageID: "ses_message_1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.transactions) != 1 || len(client.transactions[0].TransactItems) != 3 {
		t.Fatalf("completion transaction = %#v", client.transactions)
	}
	update := client.transactions[0].TransactItems[2].Update
	if update == nil {
		t.Fatalf("waiting recovery was not promoted: %#v", client.transactions[0].TransactItems)
	}
	text := transactionText(client.transactions[0])
	for _, required := range []string{"waiting_dependency", "pending", "relay_gsi_pk"} {
		if !strings.Contains(text, required) {
			t.Fatalf("recovery promotion is missing %q: %s", required, text)
		}
	}
}

func TestCancelCancelsWaitingRecoveryForPreviouslyNonterminalOpening(t *testing.T) {
	record := testRecord(t)
	recovery := waitingRecoveryFor(t, record)
	client := &fakeClient{getItems: []map[string]types.AttributeValue{marshal(t, recovery)}}

	err := (Store{Table: "domain", Client: client}).Cancel(
		context.Background(), record, notifications.CancellationReasonEmailSuppressed, testNow(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.transactions) != 1 || len(client.transactions[0].TransactItems) != 2 {
		t.Fatalf("cancellation transaction = %#v", client.transactions)
	}
	update := client.transactions[0].TransactItems[1].Update
	if update == nil {
		t.Fatalf("waiting recovery was not cancelled: %#v", client.transactions[0].TransactItems)
	}
	text := transactionText(client.transactions[0])
	for _, required := range []string{"waiting_dependency", "cancelled", "opening_delivery_not_succeeded"} {
		if !strings.Contains(text, required) {
			t.Fatalf("recovery cancellation is missing %q: %s", required, text)
		}
	}
}

func TestStaleFeedbackCommitDoesNotRaceSuccessfulCurrentAttemptCompletion(t *testing.T) {
	tests := []struct {
		name            string
		event           feedback.Event
		wantSuppression bool
	}{
		{
			name: "Reject",
			event: feedback.Event{
				EventBridgeID: "evt_stale_reject", ProviderMessageID: "ses_attempt_a",
				SemanticType: feedback.SemanticReject, ProviderOutcome: notifications.ProviderOutcomeRejected,
				PermanentFailure: true,
			},
		},
		{
			name: "hard bounce suppression",
			event: feedback.Event{
				EventBridgeID: "evt_stale_bounce", ProviderMessageID: "ses_attempt_a",
				SemanticType: feedback.SemanticHardBounce, ProviderOutcome: notifications.ProviderOutcomeHardBounced,
				SuppressionReason: feedback.SuppressionHardBounce, AcceptedEvidence: true,
			},
			wantSuppression: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := testRecord(t)
			record.AttemptCount = 2
			record.LastAttemptID = "att_b"
			test.event.DeliveryID = record.Delivery.DeliveryID
			test.event.AttemptID = "att_a"

			attemptA := map[string]any{
				"PK": "NOTIFICATION_DELIVERY#" + record.Delivery.DeliveryID, "SK": "ATTEMPT#att_a",
				"entity_type": "notification_attempt", "tenant_id": record.Delivery.TenantID,
				"outbox_id": record.Delivery.OutboxID, "delivery_id": record.Delivery.DeliveryID,
				"attempt_id": "att_a", "event_id": record.Delivery.EventID,
				"notification_kind": string(record.Delivery.Kind), "channel": string(record.Delivery.Channel),
				"outcome": "ambiguous", "started_at": testNow().Add(-time.Minute).Format(time.RFC3339Nano),
				"completed_at": testNow().Add(-30 * time.Second).Format(time.RFC3339Nano),
			}
			delivery := testDeliveryItem(t, notifications.DeliveryStateProcessing)
			delivery["delivery_revision"] = record.Revision
			delivery["attempt_count"] = int64(record.AttemptCount)
			delivery["last_attempt_id"] = record.LastAttemptID
			delivery["delivery_lease_owner"] = record.LeaseOwner
			delivery["delivery_lease_epoch"] = record.LeaseEpoch
			delivery["delivery_lease_expires_at"] = record.LeaseExpiresAt.Format(time.RFC3339Nano)
			delivery["provider_attempt_id"] = record.LastAttemptID
			delivery["provider_message_id"] = "ses_attempt_b_feedback"
			delivery["provider_outcome"] = "delayed"
			delivery["provider_accepted"] = true
			gets := []map[string]types.AttributeValue{nil, nil, marshal(t, attemptA), marshal(t, delivery)}
			if test.wantSuppression {
				gets = append(gets, nil)
			}
			client := &fakeClient{getItems: gets}

			// Attempt B has returned successfully from SES but has not yet committed
			// CompleteAttempt. Stale feedback for A commits first.
			result, err := (feedbackdynamo.Store{Table: "domain", Client: client}).Reconcile(
				context.Background(), test.event, testNow(),
			)
			if err != nil || result.Disposition != feedback.ReconcileApplied || result.Suppressed != test.wantSuppression {
				t.Fatalf("stale feedback result=%#v err=%v", result, err)
			}
			feedbackTransaction := client.transactions[0]
			wantItems := 4
			if test.wantSuppression {
				wantItems = 5
			}
			if len(feedbackTransaction.TransactItems) != wantItems ||
				feedbackTransaction.TransactItems[3].ConditionCheck == nil ||
				feedbackTransaction.TransactItems[3].Update != nil {
				t.Fatalf("non-owner Delivery operation = %#v", feedbackTransaction.TransactItems)
			}
			if test.wantSuppression && feedbackTransaction.TransactItems[4].Put == nil {
				t.Fatalf("stale suppression missing from feedback transaction: %#v", feedbackTransaction.TransactItems)
			}

			// Because A only fenced the observed Delivery, B still owns the original
			// processing revision and can durably commit its successful SES response.
			err = (Store{Table: "domain", Client: client}).CompleteAttempt(
				context.Background(), record, worker.AttemptCompletion{
					AttemptID: "att_b", CompletedAt: testNow().Add(time.Second),
					Outcome: notifications.AttemptOutcomeSucceeded, NextState: notifications.DeliveryStateSucceeded,
					ProviderOutcome: notifications.ProviderOutcomeAccepted, ProviderMessageID: "ses_attempt_b",
				},
			)
			if err != nil || len(client.transactions) != 2 {
				t.Fatalf("Attempt B completion error=%v transactions=%d", err, len(client.transactions))
			}
			completionValues := decodeExpressionValues(
				t, client.transactions[1].TransactItems[1].Update.ExpressionAttributeValues,
			)
			if fmt.Sprint(completionValues[":revision"]) != fmt.Sprint(record.Revision) || completionValues[":attempt_id"] != "att_b" {
				t.Fatalf("Attempt B completion fence = %#v", completionValues)
			}
		})
	}
}

func TestCurrentAttemptDeliveryDelayDoesNotLoseConfirmedSendCompletion(t *testing.T) {
	record := testRecord(t)
	record.AttemptCount = 1
	record.LastAttemptID = "att_current"

	attempt := map[string]any{
		"PK": "NOTIFICATION_DELIVERY#" + record.Delivery.DeliveryID, "SK": "ATTEMPT#att_current",
		"entity_type": "notification_attempt", "tenant_id": record.Delivery.TenantID,
		"outbox_id": record.Delivery.OutboxID, "delivery_id": record.Delivery.DeliveryID,
		"attempt_id": "att_current", "event_id": record.Delivery.EventID,
		"notification_kind": string(record.Delivery.Kind), "channel": string(record.Delivery.Channel),
		"outcome": "started", "started_at": testNow().Add(-time.Second).Format(time.RFC3339Nano),
	}
	delivery := testDeliveryItem(t, notifications.DeliveryStateProcessing)
	delivery["delivery_revision"] = record.Revision
	delivery["attempt_count"] = int64(record.AttemptCount)
	delivery["last_attempt_id"] = record.LastAttemptID
	delivery["delivery_lease_owner"] = record.LeaseOwner
	delivery["delivery_lease_epoch"] = record.LeaseEpoch
	delivery["delivery_lease_expires_at"] = record.LeaseExpiresAt.Format(time.RFC3339Nano)

	refreshed := cloneMap(delivery)
	refreshed["delivery_revision"] = record.Revision + 1
	refreshed["provider_attempt_id"] = record.LastAttemptID
	refreshed["provider_message_id"] = "ses_current"
	refreshed["provider_outcome"] = "delayed"
	refreshed["possibly_accepted"] = true

	client := &fakeClient{
		getItems: []map[string]types.AttributeValue{
			nil, nil, marshal(t, attempt), marshal(t, delivery), nil, marshal(t, refreshed),
		},
		transactionErrors: []error{nil, &types.TransactionCanceledException{}, nil},
	}
	event := feedback.Event{
		EventBridgeID: "evt_delay_current", ProviderMessageID: "ses_current",
		DeliveryID: record.Delivery.DeliveryID, AttemptID: record.LastAttemptID,
		SemanticType:    feedback.SemanticDeliveryDelay,
		ProviderOutcome: notifications.ProviderOutcomeDelayed, AcceptedEvidence: true,
	}
	result, err := (feedbackdynamo.Store{Table: "domain", Client: client}).Reconcile(
		context.Background(), event, testNow(),
	)
	if err != nil || result.Disposition != feedback.ReconcileApplied {
		t.Fatalf("DeliveryDelay result=%#v err=%v", result, err)
	}

	err = (Store{Table: "domain", Client: client}).CompleteAttempt(
		context.Background(), record, worker.AttemptCompletion{
			AttemptID: record.LastAttemptID, CompletedAt: testNow(),
			Outcome: notifications.AttemptOutcomeSucceeded, NextState: notifications.DeliveryStateSucceeded,
			ProviderOutcome: notifications.ProviderOutcomeAccepted, ProviderMessageID: "ses_current",
		},
	)
	if err != nil {
		t.Fatalf("confirmed send completion lost after DeliveryDelay: %v", err)
	}
	if len(client.transactions) != 3 {
		t.Fatalf("transactions = %d, want feedback + failed completion + merged completion", len(client.transactions))
	}
	mergedValues := decodeExpressionValues(
		t, client.transactions[2].TransactItems[1].Update.ExpressionAttributeValues,
	)
	if fmt.Sprint(mergedValues[":revision"]) != fmt.Sprint(record.Revision+1) ||
		mergedValues[":provider_outcome"] != "accepted" || mergedValues[":provider_message_id"] != "ses_current" {
		t.Fatalf("merged completion values = %#v", mergedValues)
	}
}

func TestCompleteAttemptFinalizesCurrentStartedAttemptWhenOlderFeedbackTerminalizesDelivery(t *testing.T) {
	for _, terminalState := range []notifications.DeliveryState{
		notifications.DeliveryStateSucceeded,
		notifications.DeliveryStateCancelled,
	} {
		t.Run(string(terminalState), func(t *testing.T) {
			record := testRecord(t)
			record.AttemptCount = 2
			record.LastAttemptID = "att_current"
			terminal := testDeliveryItem(t, terminalState)
			terminal["delivery_revision"] = record.Revision + 1
			terminal["attempt_count"] = int64(2)
			terminal["last_attempt_id"] = "att_current"
			terminal["provider_outcome"] = "accepted"
			terminal["provider_message_id"] = "ses_previous"
			if terminalState == notifications.DeliveryStateCancelled {
				terminal["cancellation_reason"] = "email_suppressed"
			}
			startedAttempt := testAttemptItem(record, "att_current", notifications.AttemptOutcomeStarted)
			client := &fakeClient{
				getItems:          []map[string]types.AttributeValue{marshal(t, terminal), marshal(t, startedAttempt)},
				transactionErrors: []error{&types.TransactionCanceledException{}},
			}

			err := (Store{Table: "domain", Client: client}).CompleteAttempt(
				context.Background(), record, worker.AttemptCompletion{
					AttemptID: "att_current", CompletedAt: testNow(), Outcome: notifications.AttemptOutcomeRetryable,
					ErrorCategory: "retryable_service_unavailable", NextState: notifications.DeliveryStateRetryableFailed,
					NextAttemptAt: testNow().Add(time.Minute),
				},
			)
			if !errors.Is(err, worker.ErrConcurrentTerminal) || len(client.transactions) != 2 {
				t.Fatalf("CompleteAttempt() error=%v transactions=%d", err, len(client.transactions))
			}
			cleanup := client.transactions[1]
			if len(cleanup.TransactItems) != 2 || cleanup.TransactItems[0].ConditionCheck == nil || cleanup.TransactItems[1].Update == nil {
				t.Fatalf("terminal Attempt cleanup transaction = %#v", cleanup.TransactItems)
			}
			if condition := *cleanup.TransactItems[0].ConditionCheck.ConditionExpression; !strings.Contains(condition, "#state = :terminal_state") || !strings.Contains(condition, "#revision = :terminal_revision") {
				t.Fatalf("terminal delivery fence = %s", condition)
			}
			update := cleanup.TransactItems[1].Update
			var key map[string]string
			if err := attributevalue.UnmarshalMap(update.Key, &key); err != nil {
				t.Fatal(err)
			}
			if key["PK"] != "NOTIFICATION_DELIVERY#"+record.Delivery.DeliveryID || key["SK"] != "ATTEMPT#att_current" ||
				!strings.Contains(*update.ConditionExpression, "#outcome = :started") ||
				strings.Contains(*update.UpdateExpression, "delivery_revision") || strings.Contains(*update.UpdateExpression, "#state") {
				t.Fatalf("current Attempt cleanup = key:%#v condition:%s update:%s", key, *update.ConditionExpression, *update.UpdateExpression)
			}
			values := decodeExpressionValues(t, update.ExpressionAttributeValues)
			if values[":outcome"] != "retryable" || values[":completed_at"] == "" {
				t.Fatalf("current Attempt cleanup values = %#v", values)
			}
		})
	}
}

type fakeClient struct {
	getItems          []map[string]types.AttributeValue
	updateItems       []map[string]types.AttributeValue
	updateErrors      []error
	gets              []*awssdk.GetItemInput
	updates           []*awssdk.UpdateItemInput
	transactions      []*awssdk.TransactWriteItemsInput
	transactionErrors []error
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
	if len(client.updateErrors) != 0 {
		err := client.updateErrors[0]
		client.updateErrors = client.updateErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	var item map[string]types.AttributeValue
	if len(client.updateItems) > 0 {
		item, client.updateItems = client.updateItems[0], client.updateItems[1:]
	}
	return &awssdk.UpdateItemOutput{Attributes: item}, nil
}
func (client *fakeClient) TransactWriteItems(_ context.Context, input *awssdk.TransactWriteItemsInput, _ ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error) {
	client.transactions = append(client.transactions, input)
	if len(client.transactionErrors) != 0 {
		err := client.transactionErrors[0]
		client.transactionErrors = client.transactionErrors[1:]
		return &awssdk.TransactWriteItemsOutput{}, err
	}
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

func testAttemptItem(record worker.DeliveryRecord, attemptID string, outcome notifications.AttemptOutcome) map[string]any {
	return map[string]any{
		"PK": "NOTIFICATION_DELIVERY#" + record.Delivery.DeliveryID, "SK": "ATTEMPT#" + attemptID,
		"entity_type": "notification_attempt", "delivery_id": record.Delivery.DeliveryID,
		"attempt_id": attemptID, "outcome": string(outcome),
	}
}

func waitingRecoveryFor(t *testing.T, record worker.DeliveryRecord) map[string]any {
	t.Helper()
	recoveryOutboxID := notifications.OutboxID(
		record.Delivery.EventID, record.Delivery.Channel, notifications.NotificationKindRecovery,
	)
	recoveryDeliveryID, err := notifications.NewDeliveryID(
		record.Delivery.EventID, notifications.NotificationKindRecovery,
		record.Delivery.Channel, record.Delivery.RecipientID,
	)
	if err != nil {
		t.Fatal(err)
	}
	recovery := testDeliveryItem(t, notifications.DeliveryStateWaitingDependency)
	recovery["PK"] = "NOTIFICATION_OUTBOX#" + recoveryOutboxID
	recovery["SK"] = "DELIVERY#" + recoveryDeliveryID
	recovery["outbox_id"] = recoveryOutboxID
	recovery["delivery_id"] = recoveryDeliveryID
	recovery["kind"] = "recovery"
	recovery["depends_on_outbox_id"] = record.Delivery.OutboxID
	recovery["depends_on_delivery_id"] = record.Delivery.DeliveryID
	recovery["delivery_revision"] = int64(1)
	return recovery
}

func decodeExpressionValues(t *testing.T, values map[string]types.AttributeValue) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := attributevalue.UnmarshalMap(values, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
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
