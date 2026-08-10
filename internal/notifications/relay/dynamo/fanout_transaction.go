package dynamo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (store Store) commitFanoutPage(
	ctx context.Context,
	work relay.Work,
	request relay.ExpandRequest,
	deliveries []notifications.Delivery,
	dependencyChecks []types.TransactWriteItem,
	result relay.WorkResult,
	nextCursor string,
) error {
	items := make([]types.TransactWriteItem, 0, len(deliveries)+len(dependencyChecks)+1)
	for _, delivery := range deliveries {
		mutation, err := store.deliveryMutation(delivery, request.RelayTime)
		if err != nil {
			return err
		}
		items = append(items, types.TransactWriteItem{Update: mutation})
	}
	items = append(items, dependencyChecks...)
	if len(items) >= 100 {
		return fmt.Errorf("relay fanout page exceeds DynamoDB transaction capacity")
	}
	outboxMutation, err := store.outboxMutation(work, request, result, nextCursor)
	if err != nil {
		return err
	}
	items = append(items, types.TransactWriteItem{Update: outboxMutation})
	token := fanoutToken(work, deliveries, nextCursor)
	_, err = store.Client.TransactWriteItems(ctx, &awssdk.TransactWriteItemsInput{
		TransactItems: items, ClientRequestToken: aws.String(token),
	})
	if err != nil {
		return fmt.Errorf("commit relay fanout page: %w", err)
	}
	return nil
}

func (store Store) deliveryMutation(
	delivery notifications.Delivery,
	availableAt time.Time,
) (*types.Update, error) {
	snapshot := delivery.Snapshot()
	key, err := notifications.DeliveryStorageKey(snapshot.OutboxID, snapshot.DeliveryID)
	if err != nil {
		return nil, err
	}
	encodedKey, err := attributevalue.MarshalMap(map[string]string{"PK": key.PartitionKey, "SK": key.SortKey})
	if err != nil {
		return nil, err
	}
	values := map[string]any{
		":entity_type": "notification_delivery", ":relay_schema": notifications.RelaySchemaVersion,
		":tenant_id": snapshot.TenantID, ":outbox_id": snapshot.OutboxID,
		":delivery_id": snapshot.DeliveryID, ":event_id": snapshot.EventID,
		":rule_id": snapshot.RuleID, ":kind": string(snapshot.Kind), ":channel": string(snapshot.Channel),
		":recipient_id": snapshot.RecipientID, ":email": snapshot.NormalizedEmail,
		":membership": map[string]any{
			"role": snapshot.MembershipSnapshot.Role, "status": snapshot.MembershipSnapshot.Status,
			"version": snapshot.MembershipSnapshot.Version,
		},
		":state": string(snapshot.State), ":revision": int64(1),
		":created_at": snapshot.CreatedAt.UTC().Format(fixedUTCLayout),
		":updated_at": snapshot.UpdatedAt.UTC().Format(fixedUTCLayout),
	}
	fields := []struct {
		name  string
		value string
	}{
		{"entity_type", ":entity_type"}, {"relay_schema_version", ":relay_schema"},
		{"tenant_id", ":tenant_id"}, {"outbox_id", ":outbox_id"}, {"delivery_id", ":delivery_id"},
		{"event_id", ":event_id"}, {"rule_id", ":rule_id"}, {"kind", ":kind"},
		{"channel", ":channel"}, {"recipient_id", ":recipient_id"}, {"normalized_email", ":email"},
		{"membership_snapshot", ":membership"}, {"state", ":state"},
		{"delivery_revision", ":revision"}, {"created_at", ":created_at"}, {"updated_at", ":updated_at"},
	}
	dependencyConditions := []string{}
	if snapshot.DependsOnOutboxID != "" || snapshot.DependsOnDeliveryID != "" {
		values[":depends_on_outbox_id"] = snapshot.DependsOnOutboxID
		values[":depends_on_delivery_id"] = snapshot.DependsOnDeliveryID
		fields = append(fields,
			struct{ name, value string }{"depends_on_outbox_id", ":depends_on_outbox_id"},
			struct{ name, value string }{"depends_on_delivery_id", ":depends_on_delivery_id"},
		)
		dependencyConditions = append(dependencyConditions,
			"#depends_on_outbox_id = :depends_on_outbox_id",
			"#depends_on_delivery_id = :depends_on_delivery_id",
		)
	}
	payloadCondition := "#cancellation_reason = :cancellation_reason"
	if snapshot.State == notifications.DeliveryStatePending ||
		snapshot.State == notifications.DeliveryStateWaitingDependency {
		content := snapshot.Content
		values[":content"] = map[string]any{
			"template_id": string(content.TemplateID), "template_version": content.TemplateVersion,
			"locale": string(content.Locale), "subject": content.Subject, "text": content.Text,
			"html": content.HTML, "content_hash": content.ContentHash,
		}
		fields = append(fields, struct{ name, value string }{"content", ":content"})
		if snapshot.State == notifications.DeliveryStatePending {
			index, indexErr := notifications.BuildRelayIndexKey(
				notifications.WorkKindDelivery, snapshot.TenantID, snapshot.DeliveryID, availableAt,
			)
			if indexErr != nil {
				return nil, indexErr
			}
			values[":available_at"] = availableAt.UTC().Format(fixedUTCLayout)
			values[":relay_work_kind"] = string(notifications.WorkKindDelivery)
			values[":relay_pk"] = index.PartitionKey
			values[":relay_sk"] = index.SortKey
			fields = append(fields,
				struct{ name, value string }{"available_at", ":available_at"},
				struct{ name, value string }{"relay_work_kind", ":relay_work_kind"},
				struct{ name, value string }{"relay_gsi_pk", ":relay_pk"},
				struct{ name, value string }{"relay_gsi_sk", ":relay_sk"},
			)
		}
		payloadCondition = "#content = :content"
	} else {
		values[":cancellation_reason"] = string(snapshot.CancellationReason)
		fields = append(fields, struct{ name, value string }{"cancellation_reason", ":cancellation_reason"})
	}
	names := make(map[string]string, len(fields))
	set := make([]string, 0, len(fields))
	for _, field := range fields {
		token := "#" + field.name
		names[token] = field.name
		set = append(set, token+" = if_not_exists("+token+", "+field.value+")")
	}
	encodedValues, err := attributevalue.MarshalMap(values)
	if err != nil {
		return nil, err
	}
	existingConditions := []string{
		"#entity_type = :entity_type", "#relay_schema_version = :relay_schema",
		"#tenant_id = :tenant_id", "#outbox_id = :outbox_id", "#delivery_id = :delivery_id",
		"#event_id = :event_id", "#rule_id = :rule_id", "#kind = :kind", "#channel = :channel",
		"#recipient_id = :recipient_id", "#normalized_email = :email",
		"#membership_snapshot = :membership", "#state = :state",
		"#delivery_revision = :revision", "#created_at = :created_at", "#updated_at = :updated_at",
	}
	if snapshot.State == notifications.DeliveryStatePending {
		existingConditions = append(existingConditions, "#relay_work_kind = :relay_work_kind")
	}
	existingConditions = append(existingConditions, dependencyConditions...)
	existingConditions = append(existingConditions, payloadCondition)
	condition := "attribute_not_exists(#delivery_id) OR (" +
		strings.Join(existingConditions, " AND ") + ")"
	return &types.Update{
		TableName: aws.String(store.Table), Key: encodedKey,
		UpdateExpression:    aws.String("SET " + strings.Join(set, ", ")),
		ConditionExpression: aws.String(condition), ExpressionAttributeNames: names,
		ExpressionAttributeValues: encodedValues,
	}, nil
}

func (store Store) outboxMutation(
	work relay.Work,
	request relay.ExpandRequest,
	page relay.WorkResult,
	nextCursor string,
) (*types.Update, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"PK": work.PK, "SK": work.SK})
	if err != nil {
		return nil, err
	}
	startedAt := work.ExpansionStartedAt
	if startedAt.IsZero() {
		startedAt = request.RelayTime
	}
	values := map[string]any{
		":state": work.State, ":revision": work.Revision, ":cursor": work.Cursor,
		":lease_owner": work.LeaseOwner, ":lease_epoch": work.LeaseEpoch,
		":relay_work_kind": string(work.Kind), ":relay_pk": work.RelayPK, ":relay_sk": work.RelaySK,
		":started_at":           startedAt.UTC().Format(fixedUTCLayout),
		":next_revision":        work.Revision + 1,
		":recipients_examined":  work.RecipientsExamined + page.RecipientsExamined,
		":deliveries_created":   work.DeliveriesCreated + page.DeliveriesCreated,
		":deliveries_cancelled": work.DeliveriesCancelled + page.DeliveriesCancelled,
		":recipients_filtered":  work.RecipientsFiltered + page.RecipientsFiltered,
		":relay_time":           request.RelayTime.UTC().Format(fixedUTCLayout),
	}
	names := map[string]string{
		"#status": "expansion_status", "#revision": "expansion_revision",
		"#cursor": "expansion_cursor", "#lease_owner": "relay_lease_owner",
		"#lease_epoch": "relay_lease_epoch", "#lease_expires": "relay_lease_expires_at",
		"#relay_work_kind": "relay_work_kind", "#relay_pk": "relay_gsi_pk",
		"#relay_sk": "relay_gsi_sk", "#available_at": "available_at",
		"#started_at": "expansion_started_at", "#expanded_at": "expanded_at",
		"#recipients_examined": "recipients_examined", "#deliveries_created": "deliveries_created",
		"#deliveries_cancelled": "deliveries_cancelled", "#recipients_filtered": "recipients_filtered",
	}
	condition := "#status = :state "
	if work.Revision == 0 {
		condition += "AND (attribute_not_exists(#revision) OR #revision = :revision) "
	} else {
		condition += "AND #revision = :revision "
	}
	if work.Cursor == "" {
		condition += "AND (attribute_not_exists(#cursor) OR #cursor = :cursor) "
	} else {
		condition += "AND #cursor = :cursor "
	}
	condition += "AND #lease_owner = :lease_owner AND #lease_epoch = :lease_epoch " +
		"AND #relay_work_kind = :relay_work_kind AND #relay_pk = :relay_pk AND #relay_sk = :relay_sk"
	set := []string{
		"#relay_work_kind = :relay_work_kind",
		"#started_at = if_not_exists(#started_at, :started_at)", "#revision = :next_revision",
		"#recipients_examined = :recipients_examined", "#deliveries_created = :deliveries_created",
		"#deliveries_cancelled = :deliveries_cancelled", "#recipients_filtered = :recipients_filtered",
	}
	remove := []string{"#lease_owner", "#lease_epoch", "#lease_expires"}
	if nextCursor == "" {
		values[":expanded"] = "expanded"
		set = append(set, "#status = :expanded", "#expanded_at = :relay_time")
		remove = append(remove, "#cursor", "#relay_pk", "#relay_sk", "#available_at")
	} else {
		index, indexErr := notifications.BuildRelayIndexKey(
			work.Kind, work.TenantID, work.ItemID, request.RelayTime,
		)
		if indexErr != nil {
			return nil, indexErr
		}
		values[":next_cursor"] = nextCursor
		values[":next_relay_pk"] = index.PartitionKey
		values[":next_relay_sk"] = index.SortKey
		set = append(set, "#cursor = :next_cursor", "#available_at = :relay_time",
			"#relay_pk = :next_relay_pk", "#relay_sk = :next_relay_sk")
	}
	encodedValues, err := attributevalue.MarshalMap(values)
	if err != nil {
		return nil, err
	}
	return &types.Update{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression:    aws.String("SET " + strings.Join(set, ", ") + " REMOVE " + strings.Join(remove, ", ")),
		ConditionExpression: aws.String(condition), ExpressionAttributeNames: names,
		ExpressionAttributeValues: encodedValues,
	}, nil
}

func fanoutToken(work relay.Work, deliveries []notifications.Delivery, nextCursor string) string {
	canonical := strings.Join([]string{
		work.PK, work.SK, strconv.FormatInt(work.LeaseEpoch, 10),
		strconv.FormatInt(work.Revision, 10), work.Cursor, nextCursor,
	}, "\x00")
	for _, delivery := range deliveries {
		snapshot := delivery.Snapshot()
		canonical += "\x00" + snapshot.DeliveryID + "\x00" + string(snapshot.State) + "\x00" + snapshot.Content.ContentHash
	}
	digest := sha256.Sum256([]byte(canonical))
	return "nrel-" + hex.EncodeToString(digest[:])[:31]
}
