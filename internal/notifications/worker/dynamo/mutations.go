package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (store Store) Cancel(
	ctx context.Context,
	record worker.DeliveryRecord,
	reason notifications.CancellationReason,
	now time.Time,
) error {
	if reason.Validate() != nil || now.IsZero() || validateTransition(record.Delivery, notifications.DeliveryStateCancelled) != nil {
		return fmt.Errorf("invalid delivery cancellation")
	}
	return store.transitionProcessing(ctx, record,
		"SET #state = :cancelled, #cancellation_reason = :reason, #updated_at = :now, #revision = :next_revision REMOVE #lease_owner, #lease_expires, #next_attempt_at, #awaiting_intervention",
		map[string]string{"#cancellation_reason": "cancellation_reason", "#awaiting_intervention": "awaiting_intervention"},
		map[string]any{":cancelled": string(notifications.DeliveryStateCancelled), ":reason": string(reason), ":now": fixedTime(now)},
	)
}

func (store Store) Defer(ctx context.Context, record worker.DeliveryRecord, request worker.DeferRequest) error {
	if request.Now.IsZero() || request.NextAttemptAt.Before(request.Now) || request.ErrorCategory == "" ||
		validateTransition(record.Delivery, notifications.DeliveryStateRetryableFailed) != nil {
		return fmt.Errorf("invalid delivery defer mutation")
	}
	return store.transitionProcessing(ctx, record,
		"SET #state = :retryable, #next_attempt_at = :next_attempt_at, #last_error_category = :category, #updated_at = :now, #revision = :next_revision REMOVE #lease_owner, #lease_expires",
		map[string]string{"#next_attempt_at": "next_attempt_at", "#last_error_category": "last_error_category"},
		map[string]any{":retryable": string(notifications.DeliveryStateRetryableFailed),
			":next_attempt_at": fixedTime(request.NextAttemptAt), ":category": request.ErrorCategory, ":now": fixedTime(request.Now)},
	)
}

func (store Store) FinalizeUnknown(ctx context.Context, record worker.DeliveryRecord, now time.Time) error {
	if now.IsZero() || !record.AmbiguousExhausted || !record.PossiblyAccepted ||
		validateTransition(record.Delivery, notifications.DeliveryStateUnknown) != nil {
		return fmt.Errorf("invalid unknown delivery mutation")
	}
	return store.transitionProcessing(ctx, record,
		"SET #state = :unknown, #possibly_accepted = :true, #updated_at = :now, #revision = :next_revision REMOVE #lease_owner, #lease_expires, #next_attempt_at, #ambiguous_exhausted",
		map[string]string{"#possibly_accepted": "possibly_accepted", "#ambiguous_exhausted": "ambiguous_exhausted", "#next_attempt_at": "next_attempt_at"},
		map[string]any{":unknown": string(notifications.DeliveryStateUnknown), ":true": true, ":now": fixedTime(now)},
	)
}

func (store Store) Renew(ctx context.Context, record worker.DeliveryRecord, expiresAt time.Time) error {
	if expiresAt.IsZero() || record.Delivery.State != notifications.DeliveryStateProcessing {
		return fmt.Errorf("invalid delivery lease renewal")
	}
	key, err := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	if err != nil {
		return err
	}
	encodedKey, _ := attributevalue.MarshalMap(map[string]string{"PK": key.PartitionKey, "SK": key.SortKey})
	values, _ := attributevalue.MarshalMap(map[string]any{
		":processing": string(notifications.DeliveryStateProcessing), ":revision": record.Revision,
		":owner": record.LeaseOwner, ":epoch": record.LeaseEpoch, ":expires": fixedTime(expiresAt),
	})
	_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: aws.String(store.Table), Key: encodedKey,
		UpdateExpression:    aws.String("SET #lease_expires = :expires"),
		ConditionExpression: aws.String("#state = :processing AND #revision = :revision AND #lease_owner = :owner AND #lease_epoch = :epoch AND #lease_expires < :expires"),
		ExpressionAttributeNames: map[string]string{"#state": "state", "#revision": "delivery_revision",
			"#lease_owner": "delivery_lease_owner", "#lease_epoch": "delivery_lease_epoch", "#lease_expires": "delivery_lease_expires_at"},
		ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("renew delivery lease: %w", err)
	}
	return nil
}

func (store Store) Refresh(ctx context.Context, record worker.DeliveryRecord) (worker.DeliveryRecord, bool, error) {
	key, err := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	if err != nil {
		return worker.DeliveryRecord{}, false, err
	}
	item, err := store.getConsistent(ctx, key.PartitionKey, key.SortKey)
	if err != nil {
		return worker.DeliveryRecord{}, false, err
	}
	if len(item) == 0 {
		return worker.DeliveryRecord{}, false, fmt.Errorf("claimed delivery disappeared")
	}
	_, refreshed, err := decodeDelivery(item)
	if err != nil {
		return worker.DeliveryRecord{}, false, err
	}
	switch refreshed.Delivery.State {
	case notifications.DeliveryStateSucceeded, notifications.DeliveryStatePermanentFailed,
		notifications.DeliveryStateCancelled, notifications.DeliveryStateUnknown:
		return refreshed, true, nil
	}
	if refreshed.Delivery.State != notifications.DeliveryStateProcessing || refreshed.Revision != record.Revision ||
		refreshed.LeaseOwner != record.LeaseOwner || refreshed.LeaseEpoch != record.LeaseEpoch {
		return worker.DeliveryRecord{}, false, fmt.Errorf("delivery claim changed during reconciliation")
	}
	return refreshed, false, nil
}

func (store Store) transitionProcessing(
	ctx context.Context,
	record worker.DeliveryRecord,
	update string,
	extraNames map[string]string,
	extraValues map[string]any,
) error {
	key, err := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	if err != nil {
		return err
	}
	encodedKey, _ := attributevalue.MarshalMap(map[string]string{"PK": key.PartitionKey, "SK": key.SortKey})
	valuesMap := map[string]any{
		":processing": string(notifications.DeliveryStateProcessing), ":revision": record.Revision,
		":next_revision": record.Revision + 1, ":owner": record.LeaseOwner, ":epoch": record.LeaseEpoch,
	}
	for token, value := range extraValues {
		valuesMap[token] = value
	}
	values, _ := attributevalue.MarshalMap(valuesMap)
	names := map[string]string{
		"#state": "state", "#revision": "delivery_revision", "#updated_at": "updated_at",
		"#lease_owner": "delivery_lease_owner", "#lease_epoch": "delivery_lease_epoch",
		"#lease_expires": "delivery_lease_expires_at", "#next_attempt_at": "next_attempt_at",
	}
	for token, name := range extraNames {
		names[token] = name
	}
	_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: aws.String(store.Table), Key: encodedKey, UpdateExpression: aws.String(update),
		ConditionExpression:      aws.String("#state = :processing AND #revision = :revision AND #lease_owner = :owner AND #lease_epoch = :epoch"),
		ExpressionAttributeNames: names, ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("transition notification delivery: %w", err)
	}
	return nil
}
