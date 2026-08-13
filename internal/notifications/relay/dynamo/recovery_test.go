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

func TestExpandDependencyChainsSucceededOpeningWithoutRecheckingPreference(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	work := recoveryWork(t, relayTime)
	openingDelivery := succeededOpeningDelivery(t, relayTime)
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{openingDelivery}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: marshalMap(t, map[string]any{
				"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_OUTBOX#opening_outbox",
				"entity_type": "notification_outbox", "tenant_id": "tnt_1",
				"outbox_id": "opening_outbox", "event_id": "event_1", "kind": "opening",
				"expansion_status": "expanded",
			})},
			{Item: marshalMap(t, openingEvent(relayTime))},
			{Item: activeMember(t, relayTime, "sub_1")},
			{},
		},
	}

	result, err := (Store{Table: "domain", Client: client}).ExpandDependency(
		context.Background(), work, relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCreated != 1 || result.DeliveriesCancelled != 0 || result.RecipientsExamined != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(client.queryInputs) != 1 || client.queryInputs[0].IndexName != nil {
		t.Fatalf("opening delivery query = %#v", client.queryInputs)
	}
	for _, input := range client.getInputs {
		var key map[string]string
		if err := attributevalue.UnmarshalMap(input.Key, &key); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(key["SK"], "NOTIFICATION_PREFERENCE") {
			t.Fatalf("recovery rechecked preference: %#v", key)
		}
	}
	transaction := client.transactInputs[0]
	if len(transaction.TransactItems) != 3 || transaction.TransactItems[0].Update == nil ||
		transaction.TransactItems[1].ConditionCheck == nil {
		t.Fatalf("recovery transaction = %#v", transaction.TransactItems)
	}
	var values map[string]any
	if err := attributevalue.UnmarshalMap(
		transaction.TransactItems[0].Update.ExpressionAttributeValues, &values,
	); err != nil {
		t.Fatal(err)
	}
	openingID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if values[":state"] != "pending" || values[":kind"] != "recovery" ||
		values[":relay_work_kind"] != string(notifications.WorkKindDelivery) ||
		values[":email"] != "opening-snapshot@example.com" ||
		values[":depends_on_outbox_id"] != "opening_outbox" ||
		values[":depends_on_delivery_id"] != openingID {
		t.Fatalf("recovery values = %#v", values)
	}
	openingFence := transaction.TransactItems[1].ConditionCheck
	if !strings.Contains(aws.ToString(openingFence.ConditionExpression), "#state = :state") ||
		!strings.Contains(aws.ToString(openingFence.ConditionExpression), "#revision = :revision") {
		t.Fatalf("succeeded opening fence = %s", aws.ToString(openingFence.ConditionExpression))
	}
	var fenceValues map[string]any
	if err := attributevalue.UnmarshalMap(openingFence.ExpressionAttributeValues, &fenceValues); err != nil {
		t.Fatal(err)
	}
	if fenceValues[":state"] != "succeeded" || fmt.Sprint(fenceValues[":revision"]) != "1" {
		t.Fatalf("succeeded opening fence values = %#v", fenceValues)
	}
	content, ok := values[":content"].(map[string]any)
	if !ok || content["locale"] != "pt-BR" || content["template_id"] != "alert-recovery/v1" {
		t.Fatalf("recovery content = %#v", values[":content"])
	}
}

func TestExpandTelegramDependencyUsesOpeningDestinationAndTelegramLocale(t *testing.T) {
	relayTime := time.Date(2026, 8, 13, 13, 0, 0, 0, time.UTC)
	work := recoveryWork(t, relayTime)
	work.Channel = notifications.ChannelTelegram
	work.RelaySchemaVersion = notifications.TelegramRelaySchemaVersion
	destinationID := notificationsHashChatForTest("123")
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
			telegramSucceededOpeningDelivery(t, relayTime, destinationID),
		}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: expandedOpeningOutbox(t)},
			{Item: marshalMap(t, openingEvent(relayTime))},
			{Item: activeMember(t, relayTime, "sub_1")},
			{Item: marshalMap(t, map[string]any{
				"entity_type": "telegram_binding", "tenant_id": "tnt_1", "recipient_id": "sub_1",
				"destination_id": destinationID, "status": "verified", "version": int64(2),
			})},
			{Item: marshalMap(t, map[string]any{
				"entity_type": "telegram_destination", "destination_id": destinationID,
				"recipient_id": "sub_1", "chat_id": int64(123), "status": "active", "version": int64(3),
			})},
		},
	}
	result, err := (Store{
		Table: "domain", Client: client, WebURL: "http://localhost:3000", AllowInsecureWebURL: true,
	}).ExpandDependency(context.Background(), work, relay.ExpandRequest{RelayTime: relayTime, PageSize: 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCreated != 1 || result.DeliveriesCancelled != 0 {
		t.Fatalf("result = %#v", result)
	}
	var values map[string]any
	if err := attributevalue.UnmarshalMap(
		client.transactInputs[0].TransactItems[0].Update.ExpressionAttributeValues, &values,
	); err != nil {
		t.Fatal(err)
	}
	content, ok := values[":content"].(map[string]any)
	if !ok || content["template_id"] != "telegram-alert-recovery/v1" ||
		content["locale"] != "pt-BR" || values[":destination_id"] != destinationID ||
		fmt.Sprint(values[":telegram_chat_id"]) != "123" {
		t.Fatalf("Telegram recovery values = %#v", values)
	}
}

func TestExpandTelegramDependencyFencesNonterminalOpeningBeforeCancellingRecovery(t *testing.T) {
	relayTime := time.Date(2026, 8, 13, 13, 0, 0, 0, time.UTC)
	work := recoveryWork(t, relayTime)
	work.Channel = notifications.ChannelTelegram
	work.RelaySchemaVersion = notifications.TelegramRelaySchemaVersion
	destinationID := notificationsHashChatForTest("123")
	var opening map[string]any
	if err := attributevalue.UnmarshalMap(
		telegramSucceededOpeningDelivery(t, relayTime, destinationID),
		&opening,
	); err != nil {
		t.Fatal(err)
	}
	opening["state"] = string(notifications.DeliveryStateProcessing)
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
			marshalMap(t, opening),
		}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: expandedOpeningOutbox(t)},
			{Item: marshalMap(t, openingEvent(relayTime))},
		},
	}

	result, err := (Store{Table: "domain", Client: client}).ExpandDependency(
		context.Background(), work, relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCreated != 0 || result.DeliveriesCancelled != 1 {
		t.Fatalf("result = %#v", result)
	}
	var openingFence *types.ConditionCheck
	for _, item := range client.transactInputs[0].TransactItems {
		if item.ConditionCheck != nil {
			openingFence = item.ConditionCheck
			break
		}
	}
	if openingFence == nil {
		t.Fatalf("nonterminal Telegram recovery cancellation lacks opening fence: %#v", client.transactInputs[0])
	}
	var values map[string]any
	if err := attributevalue.UnmarshalMap(openingFence.ExpressionAttributeValues, &values); err != nil {
		t.Fatal(err)
	}
	if values[":state"] != string(notifications.DeliveryStateProcessing) ||
		fmt.Sprint(values[":revision"]) != "1" {
		t.Fatalf("nonterminal opening fence values = %#v", values)
	}
}

func TestExpandDependencyCancelsInactiveMembershipWithoutRelayIndex(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	inactive := activeMember(t, relayTime, "sub_1")
	var inactiveValues map[string]any
	if err := attributevalue.UnmarshalMap(inactive, &inactiveValues); err != nil {
		t.Fatal(err)
	}
	inactiveValues["status"] = "inactive"
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{succeededOpeningDelivery(t, relayTime)}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: expandedOpeningOutbox(t)},
			{Item: marshalMap(t, openingEvent(relayTime))},
			{Item: marshalMap(t, inactiveValues)},
		},
	}

	result, err := (Store{Table: "domain", Client: client}).ExpandDependency(
		context.Background(), recoveryWork(t, relayTime),
		relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCancelled != 1 || result.DeliveriesCreated != 0 || len(client.getInputs) != 3 {
		t.Fatalf("result = %#v, GetItem calls = %d", result, len(client.getInputs))
	}
	var values map[string]any
	if err := attributevalue.UnmarshalMap(
		client.transactInputs[0].TransactItems[0].Update.ExpressionAttributeValues, &values,
	); err != nil {
		t.Fatal(err)
	}
	if values[":state"] != "cancelled" ||
		values[":cancellation_reason"] != "recipient_membership_inactive" {
		t.Fatalf("inactive recovery = %#v", values)
	}
	for _, forbidden := range []string{":relay_pk", ":relay_sk", ":available_at", ":content"} {
		if _, exists := values[forbidden]; exists {
			t.Fatalf("inactive recovery contains %s: %#v", forbidden, values)
		}
	}
}

func TestExpandDependencyPreservesNonterminalOpeningWithoutBlockingSucceededRecipient(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	nonterminal := succeededOpeningDelivery(t, relayTime)
	var values map[string]any
	if err := attributevalue.UnmarshalMap(nonterminal, &values); err != nil {
		t.Fatal(err)
	}
	values["state"] = "retryable_failed"
	values["available_at"] = relayTime.Add(2 * time.Minute).Format(fixedUTCLayout)
	values["delivery_revision"] = int64(5)
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{
			marshalMap(t, values), succeededOpeningDelivery(t, relayTime),
		}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: expandedOpeningOutbox(t)}, {Item: marshalMap(t, openingEvent(relayTime))},
			{Item: activeMember(t, relayTime, "sub_1")}, {},
		},
	}

	result, err := (Store{Table: "domain", Client: client}).ExpandDependency(
		context.Background(), recoveryWork(t, relayTime),
		relay.ExpandRequest{RelayTime: relayTime, PageSize: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCreated != 2 || result.RecipientsExamined != 2 {
		t.Fatalf("result = %#v", result)
	}
	if len(client.transactInputs) != 1 || len(client.transactInputs[0].TransactItems) != 5 {
		t.Fatalf("nonterminal opening did not preserve its recovery beside the successful recipient: %#v", client.transactInputs)
	}
	var mutationText strings.Builder
	for _, item := range client.transactInputs[0].TransactItems {
		if item.Update != nil {
			mutationText.WriteString(aws.ToString(item.Update.UpdateExpression))
			mutationText.WriteString(aws.ToString(item.Update.ConditionExpression))
			var values map[string]any
			if err := attributevalue.UnmarshalMap(item.Update.ExpressionAttributeValues, &values); err != nil {
				t.Fatal(err)
			}
			mutationText.WriteString(fmt.Sprint(values))
		}
		if item.ConditionCheck != nil {
			mutationText.WriteString(aws.ToString(item.ConditionCheck.ConditionExpression))
			var values map[string]any
			if err := attributevalue.UnmarshalMap(item.ConditionCheck.ExpressionAttributeValues, &values); err != nil {
				t.Fatal(err)
			}
			mutationText.WriteString(fmt.Sprint(values))
		}
	}
	for _, required := range []string{"waiting_dependency", "pending"} {
		if !strings.Contains(mutationText.String(), required) {
			t.Fatalf("recovery transaction is missing %q: %s", required, mutationText.String())
		}
	}
}

func TestExpandDependencyCreatesSucceededAndWaitingRecoveriesWithoutHeadOfLineBlocking(t *testing.T) {
	relayTime := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	succeeded := succeededOpeningDelivery(t, relayTime)
	unknown := succeededOpeningDelivery(t, relayTime)
	var values map[string]any
	if err := attributevalue.UnmarshalMap(unknown, &values); err != nil {
		t.Fatal(err)
	}
	unknownOpeningID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_2",
	)
	if err != nil {
		t.Fatal(err)
	}
	values["SK"] = "DELIVERY#" + unknownOpeningID
	values["delivery_id"] = unknownOpeningID
	values["recipient_id"] = "sub_2"
	values["normalized_email"] = "second@example.com"
	values["state"] = "unknown"
	values["delivery_revision"] = int64(11)
	client := &fakeClient{
		queryOutputs: []*awssdk.QueryOutput{{Items: []map[string]types.AttributeValue{succeeded, marshalMap(t, values)}}},
		getOutputs: []*awssdk.GetItemOutput{
			{Item: expandedOpeningOutbox(t)}, {Item: marshalMap(t, openingEvent(relayTime))},
			{Item: activeMember(t, relayTime, "sub_1")}, {},
		},
	}

	result, err := (Store{Table: "domain", Client: client}).ExpandDependency(
		context.Background(), recoveryWork(t, relayTime),
		relay.ExpandRequest{RelayTime: relayTime, PageSize: 99},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeliveriesCreated != 2 || result.DeliveriesCancelled != 0 ||
		result.RecipientsExamined != 2 || len(client.transactInputs) != 1 {
		t.Fatalf("result=%#v transactions=%d", result, len(client.transactInputs))
	}
	if client.queryInputs[0].Limit == nil || *client.queryInputs[0].Limit != 49 {
		t.Fatalf("dependency query limit = %v, want 49 for worst-case transaction capacity", client.queryInputs[0].Limit)
	}
	transaction := client.transactInputs[0]
	if len(transaction.TransactItems) != 5 || transaction.TransactItems[0].Update == nil ||
		transaction.TransactItems[1].Update == nil || transaction.TransactItems[2].ConditionCheck == nil ||
		transaction.TransactItems[3].ConditionCheck == nil || transaction.TransactItems[4].Update == nil {
		t.Fatalf("mixed recovery transaction = %#v", transaction.TransactItems)
	}
	var text strings.Builder
	for _, item := range transaction.TransactItems {
		if item.Update != nil {
			text.WriteString(aws.ToString(item.Update.UpdateExpression))
			text.WriteString(aws.ToString(item.Update.ConditionExpression))
			for _, name := range item.Update.ExpressionAttributeNames {
				text.WriteString(name)
			}
			var decoded map[string]any
			if err := attributevalue.UnmarshalMap(item.Update.ExpressionAttributeValues, &decoded); err != nil {
				t.Fatal(err)
			}
			text.WriteString(fmt.Sprint(decoded))
		}
		if item.ConditionCheck != nil {
			text.WriteString(aws.ToString(item.ConditionCheck.ConditionExpression))
			for _, name := range item.ConditionCheck.ExpressionAttributeNames {
				text.WriteString(name)
			}
			var decoded map[string]any
			if err := attributevalue.UnmarshalMap(item.ConditionCheck.ExpressionAttributeValues, &decoded); err != nil {
				t.Fatal(err)
			}
			text.WriteString(fmt.Sprint(decoded))
		}
	}
	for _, required := range []string{"unknown", "delivery_revision", "waiting_dependency", "pending", "relay_gsi_pk"} {
		if !strings.Contains(text.String(), required) {
			t.Errorf("mixed recovery transaction missing %q: %s", required, text.String())
		}
	}
	if strings.Contains(text.String(), "opening_delivery_not_succeeded") {
		t.Fatalf("unknown opening permanently cancelled recovery: %s", text.String())
	}
}

func recoveryWork(t *testing.T, relayTime time.Time) relay.Work {
	t.Helper()
	available := relayTime.Add(-time.Minute)
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDependency, "tnt_1", "recovery_outbox", available,
	)
	if err != nil {
		t.Fatal(err)
	}
	return relay.Work{
		Candidate: relay.Candidate{
			PK: "TENANT#tnt_1", SK: "NOTIFICATION_OUTBOX#recovery_outbox",
			RelayPK: index.PartitionKey, RelaySK: index.SortKey,
			Kind: notifications.WorkKindDependency, AvailableAt: available,
		},
		TenantID: "tnt_1", ItemID: "recovery_outbox", OutboxID: "recovery_outbox",
		EventID: "event_1", RuleID: "rule_1", DependsOnOutboxID: "opening_outbox",
		NotificationKind: notifications.NotificationKindRecovery, Channel: notifications.ChannelEmail,
		State: "pending", LeaseOwner: "run_1", LeaseEpoch: 2,
	}
}

func succeededOpeningDelivery(t *testing.T, relayTime time.Time) map[string]types.AttributeValue {
	t.Helper()
	deliveryID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		t.Fatal(err)
	}
	data, err := (eventSnapshot{
		TenantID: "tnt_1", EventID: "event_1", RuleID: "rule_1", RuleName: "Oxigênio baixo",
		Severity: "critical", PondID: "pond_1", DeviceID: "device_1", Metric: "do_mg_l",
		Operator: "<", Threshold: 4.5, WindowStart: relayTime.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		WindowEnd: relayTime.Format(time.RFC3339Nano), LastEvaluatedAt: relayTime.Format(time.RFC3339Nano),
	}).templateData()
	if err != nil {
		t.Fatal(err)
	}
	content, err := renderer.Render(notifications.TemplateAlertOpeningV1, notifications.LocalePTBR, data)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := content.Snapshot()
	return marshalMap(t, map[string]any{
		"PK": "NOTIFICATION_OUTBOX#opening_outbox", "SK": "DELIVERY#" + deliveryID,
		"entity_type": "notification_delivery", "tenant_id": "tnt_1", "outbox_id": "opening_outbox",
		"delivery_id": deliveryID, "event_id": "event_1", "rule_id": "rule_1",
		"kind": "opening", "channel": "email", "recipient_id": "sub_1",
		"normalized_email": "opening-snapshot@example.com", "state": "succeeded",
		"delivery_revision":   int64(1),
		"membership_snapshot": map[string]any{"role": "owner", "status": "active", "version": int64(7)},
		"content": map[string]any{
			"template_id": string(snapshot.TemplateID), "template_version": snapshot.TemplateVersion,
			"locale": string(snapshot.Locale), "subject": snapshot.Subject, "text": snapshot.Text,
			"html": snapshot.HTML, "content_hash": snapshot.ContentHash,
		},
	})
}

func telegramSucceededOpeningDelivery(
	t *testing.T,
	relayTime time.Time,
	destinationID string,
) map[string]types.AttributeValue {
	t.Helper()
	deliveryID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelTelegram, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	content, err := notifications.NewTelegramRenderedContent(
		notifications.TemplateTelegramAlertOpeningV1, notifications.LocalePTBR, "Alerta simples",
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := content.Snapshot()
	return marshalMap(t, map[string]any{
		"PK": "NOTIFICATION_OUTBOX#opening_outbox", "SK": "DELIVERY#" + deliveryID,
		"entity_type": "notification_delivery", "tenant_id": "tnt_1", "outbox_id": "opening_outbox",
		"delivery_id": deliveryID, "event_id": "event_1", "rule_id": "rule_1",
		"kind": "opening", "channel": "telegram", "recipient_id": "sub_1",
		"destination_id": destinationID, "telegram_chat_id": int64(123), "state": "succeeded",
		"delivery_revision":   int64(1),
		"membership_snapshot": map[string]any{"role": "owner", "status": "active", "version": int64(7)},
		"content": map[string]any{
			"template_id": string(snapshot.TemplateID), "template_version": snapshot.TemplateVersion,
			"locale": string(snapshot.Locale), "body_text": snapshot.BodyText,
			"content_hash": snapshot.ContentHash,
		},
	})
}

func expandedOpeningOutbox(t *testing.T) map[string]types.AttributeValue {
	t.Helper()
	return marshalMap(t, map[string]any{
		"PK": "TENANT#tnt_1", "SK": "NOTIFICATION_OUTBOX#opening_outbox",
		"entity_type": "notification_outbox", "tenant_id": "tnt_1",
		"outbox_id": "opening_outbox", "event_id": "event_1", "kind": "opening",
		"expansion_status": "expanded",
	})
}
