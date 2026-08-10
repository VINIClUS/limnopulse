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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	return store.transitionTerminalOpening(ctx, record, notifications.DeliveryStateCancelled, now,
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

func (store Store) CompletePreflightFailure(
	ctx context.Context,
	record worker.DeliveryRecord,
	completion worker.PreflightFailureCompletion,
) error {
	if completion.CompletedAt.IsZero() || completion.ErrorCategory == "" ||
		validateTransition(record.Delivery, completion.NextState) != nil ||
		(record.PossiblyAccepted && !completion.PossiblyAccepted) {
		return fmt.Errorf("invalid preflight failure completion")
	}
	switch completion.NextState {
	case notifications.DeliveryStatePermanentFailed:
		if !completion.NextAttemptAt.IsZero() || completion.PossiblyAccepted ||
			completion.AmbiguousExhausted || completion.AwaitingIntervention {
			return fmt.Errorf("invalid permanent preflight failure completion")
		}
	case notifications.DeliveryStateRetryableFailed:
		if completion.NextAttemptAt.Before(completion.CompletedAt) ||
			(!completion.AmbiguousExhausted && !completion.AwaitingIntervention) {
			return fmt.Errorf("invalid retryable preflight failure completion")
		}
	default:
		return fmt.Errorf("invalid preflight failure state")
	}
	update := "SET #state = :next_state, #last_error_category = :error_category, #possibly_accepted = :possibly_accepted, #ambiguous_exhausted = :ambiguous_exhausted, #awaiting_intervention = :awaiting_intervention, #updated_at = :now, #revision = :next_revision"
	if completion.NextAttemptAt.IsZero() {
		update += " REMOVE #lease_owner, #lease_expires, #next_attempt_at"
	} else {
		update += ", #next_attempt_at = :next_attempt_at REMOVE #lease_owner, #lease_expires"
	}
	values := map[string]any{
		":next_state": string(completion.NextState), ":error_category": completion.ErrorCategory,
		":possibly_accepted": completion.PossiblyAccepted, ":ambiguous_exhausted": completion.AmbiguousExhausted,
		":awaiting_intervention": completion.AwaitingIntervention, ":now": fixedTime(completion.CompletedAt),
	}
	if !completion.NextAttemptAt.IsZero() {
		values[":next_attempt_at"] = fixedTime(completion.NextAttemptAt)
	}
	transition := store.transitionProcessing
	if completion.NextState == notifications.DeliveryStatePermanentFailed {
		transition = func(
			ctx context.Context,
			record worker.DeliveryRecord,
			update string,
			extraNames map[string]string,
			extraValues map[string]any,
		) error {
			return store.transitionTerminalOpening(
				ctx, record, completion.NextState, completion.CompletedAt, update, extraNames, extraValues,
			)
		}
	}
	err := transition(ctx, record, update,
		map[string]string{
			"#last_error_category": "last_error_category", "#possibly_accepted": "possibly_accepted",
			"#ambiguous_exhausted": "ambiguous_exhausted", "#awaiting_intervention": "awaiting_intervention",
		},
		values,
	)
	if err == nil {
		return nil
	}
	_, terminal, refreshErr := store.Refresh(ctx, record)
	if refreshErr == nil && terminal {
		return worker.ErrConcurrentTerminal
	}
	if refreshErr != nil {
		return fmt.Errorf("persist preflight failure after terminal race: %w", refreshErr)
	}
	return fmt.Errorf("persist preflight failure: %w", err)
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
	mutation, err := store.processingTransitionMutation(record, update, extraNames, extraValues)
	if err != nil {
		return err
	}
	_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: mutation.TableName, Key: mutation.Key, UpdateExpression: mutation.UpdateExpression,
		ConditionExpression: mutation.ConditionExpression, ExpressionAttributeNames: mutation.ExpressionAttributeNames,
		ExpressionAttributeValues: mutation.ExpressionAttributeValues,
	})
	if err != nil {
		return fmt.Errorf("transition notification delivery: %w", err)
	}
	return nil
}

func (store Store) transitionTerminalOpening(
	ctx context.Context,
	record worker.DeliveryRecord,
	nextState notifications.DeliveryState,
	now time.Time,
	update string,
	extraNames map[string]string,
	extraValues map[string]any,
) error {
	mutation, err := store.processingTransitionMutation(record, update, extraNames, extraValues)
	if err != nil {
		return err
	}
	recovery, err := store.recoveryDependencyOperation(ctx, record, nextState, now)
	if err != nil {
		return err
	}
	if recovery == nil {
		_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
			TableName: mutation.TableName, Key: mutation.Key, UpdateExpression: mutation.UpdateExpression,
			ConditionExpression: mutation.ConditionExpression, ExpressionAttributeNames: mutation.ExpressionAttributeNames,
			ExpressionAttributeValues: mutation.ExpressionAttributeValues,
		})
		if err != nil {
			return fmt.Errorf("transition notification delivery: %w", err)
		}
		return nil
	}
	_, err = store.Client.TransactWriteItems(ctx, &awssdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Update: mutation},
			*recovery,
		},
	})
	if err != nil {
		return fmt.Errorf("transition notification delivery: %w", err)
	}
	return nil
}

func (store Store) processingTransitionMutation(
	record worker.DeliveryRecord,
	update string,
	extraNames map[string]string,
	extraValues map[string]any,
) (*types.Update, error) {
	key, err := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	if err != nil {
		return nil, err
	}
	encodedKey, err := attributevalue.MarshalMap(map[string]string{"PK": key.PartitionKey, "SK": key.SortKey})
	if err != nil {
		return nil, err
	}
	valuesMap := map[string]any{
		":processing": string(notifications.DeliveryStateProcessing), ":revision": record.Revision,
		":next_revision": record.Revision + 1, ":owner": record.LeaseOwner, ":epoch": record.LeaseEpoch,
	}
	for token, value := range extraValues {
		valuesMap[token] = value
	}
	values, err := attributevalue.MarshalMap(valuesMap)
	if err != nil {
		return nil, err
	}
	names := map[string]string{
		"#state": "state", "#revision": "delivery_revision", "#updated_at": "updated_at",
		"#lease_owner": "delivery_lease_owner", "#lease_epoch": "delivery_lease_epoch",
		"#lease_expires": "delivery_lease_expires_at", "#next_attempt_at": "next_attempt_at",
	}
	for token, name := range extraNames {
		names[token] = name
	}
	return &types.Update{
		TableName: aws.String(store.Table), Key: encodedKey, UpdateExpression: aws.String(update),
		ConditionExpression:      aws.String("#state = :processing AND #revision = :revision AND #lease_owner = :owner AND #lease_epoch = :epoch"),
		ExpressionAttributeNames: names, ExpressionAttributeValues: values,
	}, nil
}
