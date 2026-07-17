package dynamo

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

func TestMarkQueuedFencesPendingDeliveryAndRemovesRelayLease(t *testing.T) {
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	work := deliveryWork(t, now)
	client := &fakeClient{}
	store := Store{Table: "domain", Client: client}

	if err := store.MarkQueued(context.Background(), work, relay.QueuedResult{
		QueuedAt: now, MessageID: "message_1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(client.updateInputs) != 1 {
		t.Fatalf("UpdateItem calls = %d", len(client.updateInputs))
	}
	input := client.updateInputs[0]
	condition := aws.ToString(input.ConditionExpression)
	for _, fragment := range []string{
		"#state = :pending", "#revision = :revision", "#lease_owner = :lease_owner",
		"#lease_epoch = :lease_epoch", "#relay_pk = :relay_pk", "#relay_sk = :relay_sk",
	} {
		if !strings.Contains(condition, fragment) {
			t.Fatalf("queued condition missing %q: %s", fragment, condition)
		}
	}
	update := aws.ToString(input.UpdateExpression)
	for _, fragment := range []string{
		"#state = :queued", "#queued_at = :queued_at", "#message_id = :message_id",
		"#revision = :next_revision", "REMOVE", "#relay_pk", "#relay_sk", "#available_at",
		"#lease_owner", "#lease_epoch", "#lease_expires",
	} {
		if !strings.Contains(update, fragment) {
			t.Fatalf("queued update missing %q: %s", fragment, update)
		}
	}
}

func TestRescheduleReindexesPendingDeliveryWithoutChangingDeliveryStateOrContent(t *testing.T) {
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	next := now.Add(time.Minute)
	work := deliveryWork(t, now)
	client := &fakeClient{}
	store := Store{Table: "domain", Client: client}

	if err := store.Reschedule(context.Background(), work, next); err != nil {
		t.Fatal(err)
	}
	if len(client.updateInputs) != 1 {
		t.Fatalf("UpdateItem calls = %d", len(client.updateInputs))
	}
	input := client.updateInputs[0]
	condition := aws.ToString(input.ConditionExpression)
	for _, fragment := range []string{
		"#state = :state", "#revision = :revision", "#lease_owner = :lease_owner",
		"#lease_epoch = :lease_epoch", "#relay_pk = :relay_pk", "#relay_sk = :relay_sk",
	} {
		if !strings.Contains(condition, fragment) {
			t.Fatalf("reschedule condition missing %q: %s", fragment, condition)
		}
	}
	update := aws.ToString(input.UpdateExpression)
	for _, forbidden := range []string{"#state =", "#content =", "#normalized_email ="} {
		if strings.Contains(update, forbidden) {
			t.Fatalf("reschedule mutates delivery via %q: %s", forbidden, update)
		}
	}
	var values map[string]any
	if err := attributevalue.UnmarshalMap(input.ExpressionAttributeValues, &values); err != nil {
		t.Fatal(err)
	}
	wantIndex, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, work.TenantID, work.DeliveryID, next,
	)
	if err != nil {
		t.Fatal(err)
	}
	if values[":next_available_at"] != next.Format(fixedUTCLayout) ||
		values[":next_relay_pk"] != wantIndex.PartitionKey || values[":next_relay_sk"] != wantIndex.SortKey {
		t.Fatalf("reschedule values = %#v", values)
	}
}

func TestRescheduleInitialOutboxConditionsAbsentRevisionAsZero(t *testing.T) {
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	work := openingWork(t, now)
	client := &fakeClient{}

	if err := (Store{Table: "domain", Client: client}).Reschedule(
		context.Background(), work, now.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	condition := aws.ToString(client.updateInputs[0].ConditionExpression)
	if !strings.Contains(condition, "attribute_not_exists(#revision)") ||
		!strings.Contains(condition, "#revision = :revision") {
		t.Fatalf("initial revision condition = %s", condition)
	}
}

func deliveryWork(t *testing.T, now time.Time) relay.Work {
	t.Helper()
	deliveryID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	available := now.Add(-time.Minute)
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, "tnt_1", deliveryID, available,
	)
	if err != nil {
		t.Fatal(err)
	}
	return relay.Work{
		Candidate: relay.Candidate{
			PK: "NOTIFICATION_OUTBOX#outbox_1", SK: "DELIVERY#" + deliveryID,
			RelayPK: index.PartitionKey, RelaySK: index.SortKey,
			Kind: notifications.WorkKindDelivery, AvailableAt: available,
		},
		TenantID: "tnt_1", ItemID: deliveryID, OutboxID: "outbox_1", DeliveryID: deliveryID,
		EventID: "event_1", RuleID: "rule_1", NotificationKind: notifications.NotificationKindOpening,
		Channel: notifications.ChannelEmail, State: "pending", Revision: 1,
		LeaseOwner: "run_1", LeaseEpoch: 3,
	}
}
