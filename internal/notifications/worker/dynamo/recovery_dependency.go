package dynamo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// recoveryDependencyOperation resolves a recovery that fanout parked while its
// opening delivery was still in flight. The absence check fences a concurrent
// fanout create: whichever transaction loses is retried against the durable
// waiting row instead of leaving it unindexed.
func (store Store) recoveryDependencyOperation(
	ctx context.Context,
	record worker.DeliveryRecord,
	nextOpeningState notifications.DeliveryState,
	now time.Time,
) (*types.TransactWriteItem, error) {
	if record.Delivery.Kind != notifications.NotificationKindOpening {
		return nil, nil
	}
	nextState := notifications.DeliveryState("")
	reason := notifications.CancellationReason("")
	switch nextOpeningState {
	case notifications.DeliveryStateSucceeded:
		nextState = notifications.DeliveryStatePending
	case notifications.DeliveryStatePermanentFailed, notifications.DeliveryStateCancelled:
		nextState = notifications.DeliveryStateCancelled
		reason = notifications.CancellationReasonOpeningNotSucceeded
	default:
		return nil, nil
	}

	recoveryOutboxID := notifications.OutboxID(
		record.Delivery.EventID, record.Delivery.Channel, notifications.NotificationKindRecovery,
	)
	recoveryDeliveryID, err := notifications.NewDeliveryID(
		record.Delivery.EventID, notifications.NotificationKindRecovery,
		record.Delivery.Channel, record.Delivery.RecipientID,
	)
	if err != nil {
		return nil, err
	}
	key, err := notifications.DeliveryStorageKey(recoveryOutboxID, recoveryDeliveryID)
	if err != nil {
		return nil, err
	}
	item, err := store.getConsistent(ctx, key.PartitionKey, key.SortKey)
	if err != nil {
		return nil, err
	}
	if len(item) == 0 {
		encodedKey, marshalErr := attributevalue.MarshalMap(map[string]string{
			"PK": key.PartitionKey, "SK": key.SortKey,
		})
		if marshalErr != nil {
			return nil, marshalErr
		}
		return &types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName: aws.String(store.Table), Key: encodedKey,
			ConditionExpression: aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)"),
		}}, nil
	}
	var recovery deliveryItem
	if err := attributevalue.UnmarshalMap(item, &recovery); err != nil {
		return nil, fmt.Errorf("decode waiting recovery delivery: %w", err)
	}
	if recovery.State != notifications.DeliveryStateWaitingDependency {
		return nil, nil
	}
	if recovery.EntityType != "notification_delivery" ||
		recovery.RelaySchemaVersion != notifications.RelaySchemaVersion ||
		recovery.PK != key.PartitionKey || recovery.SK != key.SortKey ||
		recovery.TenantID != record.Delivery.TenantID || recovery.OutboxID != recoveryOutboxID ||
		recovery.DeliveryID != recoveryDeliveryID || recovery.EventID != record.Delivery.EventID ||
		recovery.Kind != notifications.NotificationKindRecovery || recovery.Channel != record.Delivery.Channel ||
		recovery.RecipientID != record.Delivery.RecipientID ||
		recovery.DependsOnOutboxID != record.Delivery.OutboxID ||
		recovery.DependsOnDeliveryID != record.Delivery.DeliveryID || recovery.DeliveryRevision < 1 {
		return nil, fmt.Errorf("waiting recovery delivery identity is invalid")
	}

	valuesMap := map[string]any{
		":entity": "notification_delivery", ":tenant_id": recovery.TenantID,
		":outbox_id": recovery.OutboxID, ":delivery_id": recovery.DeliveryID,
		":event_id": recovery.EventID, ":recipient_id": recovery.RecipientID,
		":opening_outbox_id":   recovery.DependsOnOutboxID,
		":opening_delivery_id": recovery.DependsOnDeliveryID,
		":waiting":             string(notifications.DeliveryStateWaitingDependency),
		":revision":            recovery.DeliveryRevision, ":next_revision": recovery.DeliveryRevision + 1,
		":next_state": string(nextState), ":now": fixedTime(now),
	}
	sets := []string{"#state = :next_state", "#revision = :next_revision", "#updated_at = :now"}
	removes := []string{"#lease_owner", "#lease_expires"}
	if nextState == notifications.DeliveryStatePending {
		index, indexErr := notifications.BuildRelayIndexKey(
			notifications.WorkKindDelivery, recovery.TenantID, recovery.DeliveryID, now,
		)
		if indexErr != nil {
			return nil, indexErr
		}
		valuesMap[":available_at"] = fixedTime(now)
		valuesMap[":relay_work_kind"] = string(notifications.WorkKindDelivery)
		valuesMap[":relay_pk"] = index.PartitionKey
		valuesMap[":relay_sk"] = index.SortKey
		sets = append(sets,
			"#available_at = :available_at", "#relay_work_kind = :relay_work_kind",
			"#relay_pk = :relay_pk", "#relay_sk = :relay_sk",
		)
		removes = append(removes, "#cancellation_reason")
	} else {
		valuesMap[":cancellation_reason"] = string(reason)
		sets = append(sets, "#cancellation_reason = :cancellation_reason")
		removes = append(removes, "#available_at", "#relay_work_kind", "#relay_pk", "#relay_sk")
	}
	condition := "#entity = :entity AND #tenant_id = :tenant_id AND #outbox_id = :outbox_id AND #delivery_id = :delivery_id AND #event_id = :event_id AND #recipient_id = :recipient_id AND #depends_on_outbox_id = :opening_outbox_id AND #depends_on_delivery_id = :opening_delivery_id AND #state = :waiting AND #revision = :revision"
	update := "SET " + strings.Join(sets, ", ") + " REMOVE " + strings.Join(removes, ", ")
	names := map[string]string{
		"#entity": "entity_type", "#tenant_id": "tenant_id", "#outbox_id": "outbox_id",
		"#delivery_id": "delivery_id", "#event_id": "event_id", "#recipient_id": "recipient_id",
		"#depends_on_outbox_id": "depends_on_outbox_id", "#depends_on_delivery_id": "depends_on_delivery_id",
		"#state": "state", "#revision": "delivery_revision", "#updated_at": "updated_at",
		"#available_at": "available_at", "#relay_work_kind": "relay_work_kind",
		"#relay_pk": "relay_gsi_pk", "#relay_sk": "relay_gsi_sk",
		"#cancellation_reason": "cancellation_reason", "#lease_owner": "delivery_lease_owner",
		"#lease_expires": "delivery_lease_expires_at",
	}
	values, err := attributevalue.MarshalMap(valuesMap)
	if err != nil {
		return nil, err
	}
	encodedKey, err := attributevalue.MarshalMap(map[string]string{
		"PK": key.PartitionKey, "SK": key.SortKey,
	})
	if err != nil {
		return nil, err
	}
	return &types.TransactWriteItem{Update: &types.Update{
		TableName: aws.String(store.Table), Key: encodedKey,
		UpdateExpression: aws.String(update), ConditionExpression: aws.String(condition),
		ExpressionAttributeNames: usedNames(names, update+condition), ExpressionAttributeValues: values,
	}}, nil
}
