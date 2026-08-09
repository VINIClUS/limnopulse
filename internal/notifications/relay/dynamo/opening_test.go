package dynamo

import (
	"context"
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

func TestExpandIntentCreatesRenderedPendingDeliveryAndCompletesPageAtomically(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	work := openingWork(t, relayTime)
	membership := marshalMap(t, map[string]any{
		"PK": "TENANT#tnt_1", "SK": "MEMBER#sub_1", "entity_type": "tenant_member",
		"tenant_id": "tnt_1", "cognito_sub": "sub_1", "role": "owner", "status": "active",
		"version": int64(7), "created_at": relayTime.Add(-time.Hour).Format(time.RFC3339Nano),
	})
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{membership}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: marshalMap(t, openingEvent(relayTime))},
			{Item: marshalMap(t, map[string]any{
				"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_PREFERENCE#USER#sub_1",
				"entity_type": "notification_preference", "tenant_id": "tnt_1", "cognito_sub": "sub_1",
				"version": int64(3), "email_enabled": true, "email_address": "owner@example.com",
				"email_verified": true, "minimum_severity": "warning",
			})},
			{},
		},
	}
	store := Store{Table: "domain", Client: client}

	result, err := store.ExpandIntent(context.Background(), work, relay.ExpandRequest{
		RelayTime: relayTime, PageSize: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCreated != 1 || result.DeliveriesCancelled != 0 ||
		result.RecipientsExamined != 1 || result.RecipientsFiltered != 0 {
		t.Fatalf("result = %#v", result)
	}
	if len(client.queryInputs) != 1 {
		t.Fatalf("membership Query calls = %d", len(client.queryInputs))
	}
	if len(client.getInputs) != 3 {
		t.Fatalf("GetItem calls = %d", len(client.getInputs))
	}
	wantDeliverabilityKey, err := notifications.DeliverabilityStorageKey("owner@example.com")
	if err != nil {
		t.Fatal(err)
	}
	var deliverabilityKey map[string]string
	if err := attributevalue.UnmarshalMap(client.getInputs[2].Key, &deliverabilityKey); err != nil {
		t.Fatal(err)
	}
	if deliverabilityKey["PK"] != wantDeliverabilityKey.PartitionKey ||
		deliverabilityKey["SK"] != wantDeliverabilityKey.SortKey {
		t.Fatalf("deliverability key = %#v, want %#v", deliverabilityKey, wantDeliverabilityKey)
	}
	query := client.queryInputs[0]
	if query.IndexName != nil || !aws.ToBool(query.ConsistentRead) || aws.ToInt32(query.Limit) != 20 ||
		aws.ToString(query.KeyConditionExpression) != "#pk = :pk AND begins_with(#sk, :member_prefix)" {
		t.Fatalf("membership query = %#v", query)
	}
	var queryValues map[string]string
	if err := attributevalue.UnmarshalMap(query.ExpressionAttributeValues, &queryValues); err != nil {
		t.Fatal(err)
	}
	if queryValues[":pk"] != "TENANT#tnt_1" || queryValues[":member_prefix"] != "MEMBER#" {
		t.Fatalf("membership query values = %#v", queryValues)
	}
	if len(client.transactInputs) != 1 {
		t.Fatalf("TransactWriteItems calls = %d", len(client.transactInputs))
	}
	transaction := client.transactInputs[0]
	if transaction.ClientRequestToken == nil || len(aws.ToString(transaction.ClientRequestToken)) > 36 {
		t.Fatalf("client request token = %#v", transaction.ClientRequestToken)
	}
	if len(transaction.TransactItems) != 2 || transaction.TransactItems[0].Update == nil ||
		transaction.TransactItems[1].Update == nil {
		t.Fatalf("transaction shape = %#v", transaction.TransactItems)
	}
	deliveryUpdate := transaction.TransactItems[0].Update
	var deliveryKey map[string]string
	if err := attributevalue.UnmarshalMap(deliveryUpdate.Key, &deliveryKey); err != nil {
		t.Fatal(err)
	}
	deliveryID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if deliveryKey["PK"] != "NOTIFICATION_OUTBOX#outbox_1" || deliveryKey["SK"] != "DELIVERY#"+deliveryID {
		t.Fatalf("delivery key = %#v", deliveryKey)
	}
	if !strings.Contains(aws.ToString(deliveryUpdate.UpdateExpression), "if_not_exists") {
		t.Fatalf("delivery replay is not non-regressive: %s", aws.ToString(deliveryUpdate.UpdateExpression))
	}
	deliveryCondition := aws.ToString(deliveryUpdate.ConditionExpression)
	for _, fragment := range []string{
		"#state = :state", "#delivery_revision = :revision", "#normalized_email = :email",
		"#membership_snapshot = :membership", "#content = :content",
	} {
		if !strings.Contains(deliveryCondition, fragment) {
			t.Fatalf("delivery replay condition missing %q: %s", fragment, deliveryCondition)
		}
	}
	var deliveryValues map[string]any
	if err := attributevalue.UnmarshalMap(deliveryUpdate.ExpressionAttributeValues, &deliveryValues); err != nil {
		t.Fatal(err)
	}
	if deliveryValues[":state"] != "pending" || deliveryValues[":email"] != "owner@example.com" ||
		deliveryValues[":relay_work_kind"] != string(notifications.WorkKindDelivery) ||
		deliveryValues[":relay_pk"] == "" || deliveryValues[":relay_sk"] == "" {
		t.Fatalf("delivery values = %#v", deliveryValues)
	}
	content, ok := deliveryValues[":content"].(map[string]any)
	if !ok || content["subject"] == "" || content["text"] == "" || content["html"] == "" ||
		content["locale"] != "pt-BR" {
		t.Fatalf("rendered content = %#v", deliveryValues[":content"])
	}
	outboxUpdate := transaction.TransactItems[1].Update
	if !strings.Contains(aws.ToString(outboxUpdate.ConditionExpression), "#lease_epoch = :lease_epoch") ||
		!strings.Contains(aws.ToString(outboxUpdate.ConditionExpression), "#revision = :revision") ||
		!strings.Contains(aws.ToString(outboxUpdate.ConditionExpression), "#relay_work_kind = :relay_work_kind") ||
		!strings.Contains(aws.ToString(outboxUpdate.ConditionExpression), "attribute_not_exists(#cursor)") {
		t.Fatalf("outbox condition = %s", aws.ToString(outboxUpdate.ConditionExpression))
	}
	updateExpression := aws.ToString(outboxUpdate.UpdateExpression)
	for _, fragment := range []string{"#status = :expanded", "#expanded_at = :relay_time", "#relay_work_kind = :relay_work_kind", "REMOVE", "#relay_pk", "#lease_owner"} {
		if !strings.Contains(updateExpression, fragment) {
			t.Fatalf("outbox update missing %q: %s", fragment, updateExpression)
		}
	}
	var outboxValues map[string]any
	if err := attributevalue.UnmarshalMap(outboxUpdate.ExpressionAttributeValues, &outboxValues); err != nil {
		t.Fatal(err)
	}
	if outboxValues[":relay_work_kind"] != string(notifications.WorkKindIntent) {
		t.Fatalf("outbox values = %#v", outboxValues)
	}
}

func TestExpandIntentFiltersBelowMinimumSeverityWithoutDelivery(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	work := openingWork(t, relayTime)
	event := openingEvent(relayTime)
	event["severity"] = "warning"
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{activeMember(t, relayTime, "sub_1")}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: marshalMap(t, event)},
			{Item: preferenceItem(t, "sub_1", "critical", "owner@example.com")},
		},
	}

	result, err := (Store{Table: "domain", Client: client}).ExpandIntent(
		context.Background(), work, relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.RecipientsFiltered != 1 || result.FilteredBySeverity != 1 ||
		result.DeliveriesCreated != 0 || result.DeliveriesCancelled != 0 {
		t.Fatalf("result = %#v", result)
	}
	if len(client.getInputs) != 2 {
		t.Fatalf("GetItem calls = %d, deliverability should not be read", len(client.getInputs))
	}
	if len(client.transactInputs) != 1 || len(client.transactInputs[0].TransactItems) != 1 {
		t.Fatalf("filtered page transaction = %#v", client.transactInputs)
	}
}

func TestExpandIntentCreatesTerminalSuppressedDeliveryWithoutContentOrRelayIndex(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	work := openingWork(t, relayTime)
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{activeMember(t, relayTime, "sub_1")}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: marshalMap(t, openingEvent(relayTime))},
			{Item: preferenceItem(t, "sub_1", "warning", "owner@example.com")},
			{Item: marshalMap(t, map[string]any{"deliverability": "suppressed", "suppression_reason": "bounce"})},
		},
	}

	result, err := (Store{Table: "domain", Client: client}).ExpandIntent(
		context.Background(), work, relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCreated != 0 || result.DeliveriesCancelled != 1 || result.RecipientsFiltered != 0 {
		t.Fatalf("result = %#v", result)
	}
	update := client.transactInputs[0].TransactItems[0].Update
	var values map[string]any
	if err := attributevalue.UnmarshalMap(update.ExpressionAttributeValues, &values); err != nil {
		t.Fatal(err)
	}
	if values[":state"] != "cancelled" || values[":cancellation_reason"] != "email_suppressed" {
		t.Fatalf("cancelled delivery values = %#v", values)
	}
	condition := aws.ToString(update.ConditionExpression)
	for _, fragment := range []string{
		"#state = :state", "#delivery_revision = :revision",
		"#cancellation_reason = :cancellation_reason",
	} {
		if !strings.Contains(condition, fragment) {
			t.Fatalf("cancelled replay condition missing %q: %s", fragment, condition)
		}
	}
	for _, forbidden := range []string{":content", ":relay_pk", ":relay_sk", ":available_at"} {
		if _, exists := values[forbidden]; exists {
			t.Fatalf("suppressed delivery contains %s: %#v", forbidden, values)
		}
	}
}

func TestExpandIntentRejectsMalformedDeliverabilityRecordInsteadOfFailingOpen(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{activeMember(t, relayTime, "sub_1")}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: marshalMap(t, openingEvent(relayTime))},
			{Item: preferenceItem(t, "sub_1", "warning", "owner@example.com")},
			{Item: marshalMap(t, map[string]any{"suppression_reason": "bounce"})},
		},
	}

	_, err := (Store{Table: "domain", Client: client}).ExpandIntent(
		context.Background(), openingWork(t, relayTime),
		relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	)
	if err == nil || !strings.Contains(err.Error(), "email deliverability is invalid") {
		t.Fatalf("error = %v", err)
	}
	if len(client.transactInputs) != 0 {
		t.Fatalf("malformed deliverability failed open: %#v", client.transactInputs)
	}
}

func TestLegacyOpeningSnapshotDoesNotMixLaterEvaluationWithOriginalWindow(t *testing.T) {
	windowEnd := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	later := windowEnd.Add(3 * time.Minute)
	value := 8.1
	data, err := (eventSnapshot{
		TenantID: "tnt_1", EventID: "event_1", RuleID: "rule_1", RuleName: "Oxigênio baixo",
		Severity: "critical", PondID: "pond_1", DeviceID: "device_1", Metric: "do_mg_l",
		Operator: "<", Threshold: 4.5,
		WindowStart: windowEnd.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		WindowEnd:   windowEnd.Format(time.RFC3339Nano), LastEvaluatedAt: later.Format(time.RFC3339Nano),
		LastEvaluationValue: &value,
	}).templateData()
	if err != nil {
		t.Fatal(err)
	}
	if data.ObservedValue != nil || !data.WindowStart.Equal(windowEnd.Add(-5*time.Minute)) ||
		!data.WindowEnd.Equal(windowEnd) || !data.EvaluatedAt.Equal(windowEnd) {
		t.Fatalf("legacy delayed opening data = %#v", data)
	}
}

func TestLegacyRuleNameControlsAreNormalizedBeforeRendering(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	data, err := (eventSnapshot{
		TenantID: "tnt_1", EventID: "event_1", RuleID: "rule_1",
		RuleName: "Oxigênio\r\nBcc: victim@example.test", Severity: "critical",
		PondID: "pond_1", DeviceID: "device_1", Metric: "do_mg_l", Operator: "<", Threshold: 4.5,
		WindowStart: now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		WindowEnd:   now.Format(time.RFC3339Nano), LastEvaluatedAt: now.Format(time.RFC3339Nano),
	}).templateData()
	if err != nil {
		t.Fatal(err)
	}
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		t.Fatal(err)
	}
	content, err := renderer.Render(notifications.TemplateAlertOpeningV1, notifications.LocalePTBR, data)
	if err != nil {
		t.Fatalf("legacy relay render must converge, got %v", err)
	}
	if strings.ContainsAny(data.RuleName, "\r\n\t") || strings.ContainsAny(content.Subject(), "\r\n") ||
		data.RuleName != "Oxigênio Bcc: victim@example.test" {
		t.Fatalf("legacy rule name was not normalized: data=%q subject=%q", data.RuleName, content.Subject())
	}
}

func TestOutboxEvaluationSnapshotWinsOverLaterMutableEventMetadata(t *testing.T) {
	openingEnd := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	laterValue := 8.1
	openingValue := 4.2
	event := eventSnapshot{
		TenantID: "tnt_1", EventID: "event_1", RuleID: "rule_1", RuleName: "Oxigênio baixo",
		Severity: "critical", PondID: "pond_1", DeviceID: "device_1", Metric: "do_mg_l",
		Operator: "<", Threshold: 4.5,
		WindowStart:         openingEnd.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		WindowEnd:           openingEnd.Format(time.RFC3339Nano),
		LastEvaluatedAt:     openingEnd.Add(4 * time.Minute).Format(time.RFC3339Nano),
		LastEvaluationValue: &laterValue,
	}
	snapshot := relay.EvaluationSnapshot{
		WindowStart: openingEnd.Add(-5 * time.Minute), WindowEnd: openingEnd,
		EvaluatedAt: openingEnd, Value: &openingValue,
	}
	data, err := event.templateDataFor(notifications.NotificationKindOpening, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if data.ObservedValue == nil || *data.ObservedValue != openingValue ||
		!data.WindowStart.Equal(snapshot.WindowStart) || !data.WindowEnd.Equal(snapshot.WindowEnd) ||
		!data.EvaluatedAt.Equal(snapshot.EvaluatedAt) {
		t.Fatalf("delayed opening data = %#v", data)
	}
}

func TestRecoveryEvaluationSnapshotAndLegacyFallbackUseMatchingWindow(t *testing.T) {
	openingEnd := time.Date(2026, 7, 16, 12, 30, 0, 0, time.UTC)
	recoveryEnd := openingEnd.Add(4 * time.Minute)
	recoveryValue := 8.1
	event := eventSnapshot{
		TenantID: "tnt_1", EventID: "event_1", RuleID: "rule_1", RuleName: "Oxigênio baixo",
		Severity: "critical", PondID: "pond_1", DeviceID: "device_1", Metric: "do_mg_l",
		Operator: "<", Threshold: 4.5,
		WindowStart: openingEnd.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		WindowEnd:   openingEnd.Format(time.RFC3339Nano), LastEvaluatedAt: recoveryEnd.Format(time.RFC3339Nano),
		LastEvaluationValue: &recoveryValue,
	}

	for name, snapshot := range map[string]relay.EvaluationSnapshot{
		"durable outbox": {
			WindowStart: recoveryEnd.Add(-5 * time.Minute), WindowEnd: recoveryEnd,
			EvaluatedAt: recoveryEnd, Value: &recoveryValue,
		},
		"legacy backfill": {},
	} {
		t.Run(name, func(t *testing.T) {
			data, err := event.templateDataFor(notifications.NotificationKindRecovery, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if data.ObservedValue == nil || *data.ObservedValue != recoveryValue ||
				!data.WindowStart.Equal(recoveryEnd.Add(-5*time.Minute)) ||
				!data.WindowEnd.Equal(recoveryEnd) || !data.EvaluatedAt.Equal(recoveryEnd) {
				t.Fatalf("recovery data = %#v", data)
			}
		})
	}
}

func TestExpandIntentNextPageRequiresExactCursorAndPreservesSnapshotCounters(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 12, 31, 0, 0, time.UTC)
	startedAt := relayTime.Add(-time.Minute)
	work := openingWork(t, relayTime)
	cursorKey := marshalMap(t, map[string]any{"PK": "TENANT#tnt_1", "SK": "MEMBER#sub_previous"})
	cursor, err := encodeBaseCursor(cursorKey)
	if err != nil {
		t.Fatal(err)
	}
	work.Cursor = cursor
	work.ExpansionStartedAt = startedAt
	work.Revision = 4
	work.RecipientsExamined = 20
	work.DeliveriesCreated = 8
	work.DeliveriesCancelled = 2
	work.RecipientsFiltered = 10
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{}},
		getOutputs:   []*awssdk.GetItemOutput{{Item: marshalMap(t, openingEvent(relayTime))}},
	}

	if _, err := (Store{Table: "domain", Client: client}).ExpandIntent(
		context.Background(), work, relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	); err != nil {
		t.Fatal(err)
	}
	if len(client.queryInputs) != 1 || client.queryInputs[0].ExclusiveStartKey == nil {
		t.Fatalf("membership cursor was not used: %#v", client.queryInputs)
	}
	outbox := client.transactInputs[0].TransactItems[0].Update
	condition := aws.ToString(outbox.ConditionExpression)
	if !strings.Contains(condition, "#cursor = :cursor") || strings.Contains(condition, "attribute_not_exists(#cursor)") {
		t.Fatalf("next-page cursor is not exact: %s", condition)
	}
	var values map[string]any
	if err := attributevalue.UnmarshalMap(outbox.ExpressionAttributeValues, &values); err != nil {
		t.Fatal(err)
	}
	if values[":started_at"] != startedAt.Format(fixedUTCLayout) ||
		fmt.Sprint(values[":recipients_examined"]) != "20" ||
		fmt.Sprint(values[":deliveries_created"]) != "8" ||
		fmt.Sprint(values[":deliveries_cancelled"]) != "2" ||
		fmt.Sprint(values[":recipients_filtered"]) != "10" {
		t.Fatalf("persisted fanout counters = %#v", values)
	}
}

func openingWork(t *testing.T, relayTime time.Time) relay.Work {
	t.Helper()
	available := relayTime.Add(-time.Minute)
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindIntent, "tnt_1", "outbox_1", available,
	)
	if err != nil {
		t.Fatal(err)
	}
	return relay.Work{
		Candidate: relay.Candidate{
			PK: "TENANT#tnt_1", SK: "NOTIFICATION_OUTBOX#outbox_1",
			RelayPK: index.PartitionKey, RelaySK: index.SortKey,
			Kind: notifications.WorkKindIntent, AvailableAt: available,
		},
		TenantID: "tnt_1", ItemID: "outbox_1", OutboxID: "outbox_1",
		EventID: "event_1", RuleID: "rule_1", NotificationKind: notifications.NotificationKindOpening,
		Channel: notifications.ChannelEmail, State: "pending", Revision: 0,
		LeaseOwner: "run_1", LeaseEpoch: 1,
	}
}

func openingEvent(relayTime time.Time) map[string]any {
	return map[string]any{
		"PK": "TENANT#tnt_1", "SK": "ALERT_EVENT#event_1", "entity_type": "alert_event",
		"tenant_id": "tnt_1", "event_id": "event_1", "rule_id": "rule_1",
		"rule_name": "Oxigênio baixo", "severity": "critical", "pond_id": "pond_1",
		"device_id": "device_1", "metric": "do_mg_l", "operator": "<", "threshold": 4.5,
		"window_start":      relayTime.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		"window_end":        relayTime.Format(time.RFC3339Nano),
		"last_evaluated_at": relayTime.Format(time.RFC3339Nano), "last_evaluation_value": 4.2,
	}
}

func activeMember(t *testing.T, relayTime time.Time, recipientID string) map[string]types.AttributeValue {
	t.Helper()
	return marshalMap(t, map[string]any{
		"PK": "TENANT#tnt_1", "SK": "MEMBER#" + recipientID, "entity_type": "tenant_member",
		"tenant_id": "tnt_1", "cognito_sub": recipientID, "role": "owner", "status": "active",
		"version": int64(7), "created_at": relayTime.Add(-time.Hour).Format(time.RFC3339Nano),
	})
}

func preferenceItem(t *testing.T, recipientID, minimumSeverity, address string) map[string]types.AttributeValue {
	t.Helper()
	return marshalMap(t, map[string]any{
		"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_PREFERENCE#USER#" + recipientID,
		"entity_type": "notification_preference", "tenant_id": "tnt_1", "cognito_sub": recipientID,
		"version": int64(3), "email_enabled": true, "email_address": address,
		"email_verified": true, "minimum_severity": minimumSeverity,
	})
}
