package dynamo

import (
	"context"
	"fmt"
	"strings"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (store Store) BeginAttempt(
	ctx context.Context,
	record worker.DeliveryRecord,
	request worker.BeginAttemptRequest,
) (worker.DeliveryRecord, error) {
	if record.Delivery.State != notifications.DeliveryStateProcessing || request.AttemptID == "" ||
		request.StartedAt.IsZero() || !request.LeaseRequiredUntil.After(request.StartedAt) ||
		record.AttemptCount < 0 || record.AttemptCount >= worker.MaxProviderCalls {
		return worker.DeliveryRecord{}, fmt.Errorf("invalid attempt start")
	}
	deliveryKey, err := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	if err != nil {
		return worker.DeliveryRecord{}, err
	}
	attemptKey, err := notifications.AttemptStorageKey(record.Delivery.DeliveryID, request.AttemptID)
	if err != nil {
		return worker.DeliveryRecord{}, err
	}
	encodedDeliveryKey, _ := attributevalue.MarshalMap(map[string]string{"PK": deliveryKey.PartitionKey, "SK": deliveryKey.SortKey})
	encodedAttempt, err := attributevalue.MarshalMap(map[string]any{
		"PK": attemptKey.PartitionKey, "SK": attemptKey.SortKey, "entity_type": "notification_attempt",
		"delivery_id": record.Delivery.DeliveryID, "attempt_id": request.AttemptID,
		"tenant_id": record.Delivery.TenantID, "outbox_id": record.Delivery.OutboxID,
		"event_id": record.Delivery.EventID, "notification_kind": string(record.Delivery.Kind),
		"channel": string(record.Delivery.Channel), "content_hash": record.Delivery.Content.ContentHash,
		"attempt_number": record.AttemptCount + 1, "started_at": fixedTime(request.StartedAt),
		"outcome": string(notifications.AttemptOutcomeStarted), "possibly_accepted": false,
	})
	if err != nil {
		return worker.DeliveryRecord{}, err
	}
	values, _ := attributevalue.MarshalMap(map[string]any{
		":processing": string(notifications.DeliveryStateProcessing), ":revision": record.Revision,
		":next_revision": record.Revision + 1, ":owner": record.LeaseOwner, ":epoch": record.LeaseEpoch,
		":attempt_count": record.AttemptCount, ":next_attempt_count": record.AttemptCount + 1,
		":attempt_id": request.AttemptID, ":started_at": fixedTime(request.StartedAt),
		":lease_required_until": fixedTime(request.LeaseRequiredUntil),
	})
	_, err = store.Client.TransactWriteItems(ctx, &awssdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{
			TableName: aws.String(store.Table), Key: encodedDeliveryKey,
			UpdateExpression:    aws.String("SET #attempt_count = :next_attempt_count, #last_attempt_id = :attempt_id, #last_attempt_started_at = :started_at, #updated_at = :started_at, #revision = :next_revision"),
			ConditionExpression: aws.String("#state = :processing AND #revision = :revision AND #lease_owner = :owner AND #lease_epoch = :epoch AND #lease_expires >= :lease_required_until AND (attribute_not_exists(#attempt_count) OR #attempt_count = :attempt_count)"),
			ExpressionAttributeNames: map[string]string{
				"#state": "state", "#revision": "delivery_revision", "#lease_owner": "delivery_lease_owner",
				"#lease_epoch": "delivery_lease_epoch", "#lease_expires": "delivery_lease_expires_at",
				"#attempt_count":   "attempt_count",
				"#last_attempt_id": "last_attempt_id", "#last_attempt_started_at": "last_attempt_started_at",
				"#updated_at": "updated_at",
			}, ExpressionAttributeValues: values,
		}},
		{Put: &types.Put{TableName: aws.String(store.Table), Item: encodedAttempt,
			ConditionExpression: aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)")}},
	}})
	if err != nil {
		return worker.DeliveryRecord{}, fmt.Errorf("persist notification attempt start: %w", err)
	}
	record.Revision++
	record.AttemptCount++
	record.LastAttemptID = request.AttemptID
	record.Delivery.UpdatedAt = request.StartedAt.UTC()
	return record, nil
}

func (store Store) CompleteAttempt(
	ctx context.Context,
	record worker.DeliveryRecord,
	completion worker.AttemptCompletion,
) error {
	return store.completeAttempt(ctx, record, completion, true)
}

func (store Store) completeAttempt(
	ctx context.Context,
	record worker.DeliveryRecord,
	completion worker.AttemptCompletion,
	allowDelayedFeedbackMerge bool,
) error {
	if completion.AttemptID == "" || completion.AttemptID != record.LastAttemptID ||
		completion.CompletedAt.IsZero() || completion.Outcome.Validate() != nil || completion.NextState.Validate() != nil ||
		validateTransition(record.Delivery, completion.NextState) != nil {
		return fmt.Errorf("invalid attempt completion")
	}
	if err := validateCompletion(completion); err != nil {
		return err
	}
	deliveryKey, _ := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	attemptKey, _ := notifications.AttemptStorageKey(record.Delivery.DeliveryID, completion.AttemptID)
	encodedDeliveryKey, _ := attributevalue.MarshalMap(map[string]string{"PK": deliveryKey.PartitionKey, "SK": deliveryKey.SortKey})
	encodedAttemptKey, _ := attributevalue.MarshalMap(map[string]string{"PK": attemptKey.PartitionKey, "SK": attemptKey.SortKey})

	valuesMap := map[string]any{
		":processing": string(notifications.DeliveryStateProcessing), ":started": string(notifications.AttemptOutcomeStarted),
		":revision": record.Revision, ":next_revision": record.Revision + 1,
		":owner": record.LeaseOwner, ":epoch": record.LeaseEpoch,
		":attempt_count": record.AttemptCount, ":attempt_id": completion.AttemptID,
		":outcome": string(completion.Outcome), ":next_state": string(completion.NextState),
		":completed_at": fixedTime(completion.CompletedAt), ":possibly_accepted": completion.PossiblyAccepted || record.PossiblyAccepted,
		":ambiguous_exhausted": completion.AmbiguousExhausted, ":awaiting_intervention": completion.AwaitingIntervention,
	}
	attemptSet := []string{"#outcome = :outcome", "#completed_at = :completed_at", "#possibly_accepted = :possibly_accepted"}
	deliverySet := []string{
		"#state = :next_state", "#revision = :next_revision", "#updated_at = :completed_at",
		"#possibly_accepted = :possibly_accepted", "#ambiguous_exhausted = :ambiguous_exhausted",
		"#awaiting_intervention = :awaiting_intervention",
	}
	if completion.ErrorCategory != "" {
		valuesMap[":error_category"] = completion.ErrorCategory
		attemptSet = append(attemptSet, "#error_category = :error_category")
		deliverySet = append(deliverySet, "#last_error_category = :error_category")
	}
	if completion.ProviderMessageID != "" {
		valuesMap[":provider_message_id"] = completion.ProviderMessageID
		valuesMap[":provider_attempt_id"] = completion.AttemptID
		attemptSet = append(attemptSet, "#provider_message_id = :provider_message_id")
		deliverySet = append(deliverySet,
			"#provider_message_id = :provider_message_id",
			"#provider_attempt_id = :provider_attempt_id",
		)
	}
	if completion.ProviderOutcome != "" {
		valuesMap[":provider_outcome"] = string(completion.ProviderOutcome)
		attemptSet = append(attemptSet, "#provider_outcome = :provider_outcome")
		deliverySet = append(deliverySet, "#provider_outcome = :provider_outcome")
	}
	if !completion.NextAttemptAt.IsZero() {
		valuesMap[":next_attempt_at"] = fixedTime(completion.NextAttemptAt)
		deliverySet = append(deliverySet, "#next_attempt_at = :next_attempt_at")
	}
	attemptValueMap := map[string]any{
		":started": string(notifications.AttemptOutcomeStarted), ":outcome": string(completion.Outcome),
		":completed_at":      fixedTime(completion.CompletedAt),
		":possibly_accepted": completion.PossiblyAccepted || record.PossiblyAccepted,
	}
	attemptNames := map[string]string{
		"#outcome": "outcome", "#completed_at": "completed_at",
		"#possibly_accepted": "possibly_accepted", "#error_category": "error_category",
		"#provider_message_id": "provider_message_id", "#provider_outcome": "provider_outcome",
	}
	for _, token := range []string{":error_category", ":provider_message_id", ":provider_outcome"} {
		if value, ok := valuesMap[token]; ok {
			attemptValueMap[token] = value
		}
	}
	deliveryNames := map[string]string{
		"#state": "state", "#revision": "delivery_revision", "#updated_at": "updated_at",
		"#lease_owner": "delivery_lease_owner", "#lease_epoch": "delivery_lease_epoch",
		"#lease_expires": "delivery_lease_expires_at", "#attempt_count": "attempt_count",
		"#last_attempt_id": "last_attempt_id", "#outcome": "outcome", "#completed_at": "completed_at",
		"#possibly_accepted": "possibly_accepted", "#ambiguous_exhausted": "ambiguous_exhausted",
		"#awaiting_intervention": "awaiting_intervention", "#error_category": "error_category",
		"#last_error_category": "last_error_category", "#provider_message_id": "provider_message_id",
		"#provider_attempt_id": "provider_attempt_id", "#provider_outcome": "provider_outcome",
		"#next_attempt_at": "next_attempt_at",
	}
	deliveryUpdate := "SET " + strings.Join(deliverySet, ", ") + " REMOVE #lease_owner, #lease_expires"
	if completion.NextAttemptAt.IsZero() {
		deliveryUpdate += ", #next_attempt_at"
	}
	attemptUpdate := "SET " + strings.Join(attemptSet, ", ")
	attemptCondition := "#outcome = :started"
	deliveryCondition := "#state = :processing AND #revision = :revision AND #lease_owner = :owner AND #lease_epoch = :epoch AND #attempt_count = :attempt_count AND #last_attempt_id = :attempt_id"
	attemptExpression := attemptUpdate + " " + attemptCondition
	deliveryExpression := deliveryUpdate + " " + deliveryCondition
	attemptValues, err := marshalUsedValues(attemptValueMap, attemptExpression)
	if err != nil {
		return err
	}
	deliveryValues, err := marshalUsedValues(valuesMap, deliveryExpression)
	if err != nil {
		return err
	}
	_, err = store.Client.TransactWriteItems(ctx, &awssdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{
			TableName: aws.String(store.Table), Key: encodedAttemptKey,
			UpdateExpression:          aws.String(attemptUpdate),
			ConditionExpression:       aws.String(attemptCondition),
			ExpressionAttributeNames:  usedNames(attemptNames, attemptExpression),
			ExpressionAttributeValues: attemptValues,
		}},
		{Update: &types.Update{
			TableName: aws.String(store.Table), Key: encodedDeliveryKey,
			UpdateExpression:          aws.String(deliveryUpdate),
			ConditionExpression:       aws.String(deliveryCondition),
			ExpressionAttributeNames:  usedNames(deliveryNames, deliveryExpression),
			ExpressionAttributeValues: deliveryValues,
		}},
	}})
	if err != nil {
		if allowDelayedFeedbackMerge {
			refreshed, mergeable, terminal, refreshErr := store.refreshDelayedFeedbackCompletion(ctx, record, completion)
			if refreshErr != nil {
				return fmt.Errorf("reconcile delayed feedback before notification attempt completion: %w", refreshErr)
			}
			if terminal {
				resolved, reconcileErr := store.completeAttemptBesideKnownTerminal(ctx, record, completion, refreshed)
				if reconcileErr == nil && resolved {
					return worker.ErrConcurrentTerminal
				}
				if reconcileErr != nil {
					return fmt.Errorf("persist notification attempt completion after terminal race: %w", reconcileErr)
				}
			}
			if mergeable {
				return store.completeAttempt(ctx, refreshed, completion, false)
			}
		}
		terminal, reconcileErr := store.completeAttemptBesideTerminal(ctx, record, completion)
		if reconcileErr == nil && terminal {
			return worker.ErrConcurrentTerminal
		}
		if reconcileErr != nil {
			return fmt.Errorf("persist notification attempt completion after terminal race: %w", reconcileErr)
		}
		return fmt.Errorf("persist notification attempt completion: %w", err)
	}
	return nil
}

func (store Store) refreshDelayedFeedbackCompletion(
	ctx context.Context,
	record worker.DeliveryRecord,
	completion worker.AttemptCompletion,
) (worker.DeliveryRecord, bool, bool, error) {
	if completion.Outcome != notifications.AttemptOutcomeSucceeded ||
		completion.NextState != notifications.DeliveryStateSucceeded ||
		completion.ProviderOutcome != notifications.ProviderOutcomeAccepted || completion.ProviderMessageID == "" {
		return worker.DeliveryRecord{}, false, false, nil
	}
	key, err := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	if err != nil {
		return worker.DeliveryRecord{}, false, false, err
	}
	item, err := store.getConsistent(ctx, key.PartitionKey, key.SortKey)
	if err != nil {
		return worker.DeliveryRecord{}, false, false, err
	}
	if len(item) == 0 {
		return worker.DeliveryRecord{}, false, false, fmt.Errorf("claimed delivery disappeared")
	}
	_, refreshed, err := decodeDelivery(item)
	if err != nil {
		return worker.DeliveryRecord{}, false, false, err
	}
	switch refreshed.Delivery.State {
	case notifications.DeliveryStateSucceeded, notifications.DeliveryStatePermanentFailed,
		notifications.DeliveryStateCancelled, notifications.DeliveryStateUnknown:
		return refreshed, false, true, nil
	}
	mergeable := refreshed.Delivery.State == notifications.DeliveryStateProcessing &&
		refreshed.Revision > record.Revision && refreshed.LeaseOwner == record.LeaseOwner &&
		refreshed.LeaseEpoch == record.LeaseEpoch && refreshed.AttemptCount == record.AttemptCount &&
		refreshed.LastAttemptID == completion.AttemptID &&
		refreshed.ProviderOutcome == notifications.ProviderOutcomeDelayed &&
		refreshed.ProviderAttemptID == completion.AttemptID &&
		refreshed.ProviderMessageID == completion.ProviderMessageID
	return refreshed, mergeable, false, nil
}

func (store Store) completeAttemptBesideTerminal(
	ctx context.Context,
	record worker.DeliveryRecord,
	completion worker.AttemptCompletion,
) (bool, error) {
	refreshed, terminal, err := store.Refresh(ctx, record)
	if err != nil || !terminal {
		return false, err
	}
	return store.completeAttemptBesideKnownTerminal(ctx, record, completion, refreshed)
}

func (store Store) completeAttemptBesideKnownTerminal(
	ctx context.Context,
	record worker.DeliveryRecord,
	completion worker.AttemptCompletion,
	refreshed worker.DeliveryRecord,
) (bool, error) {
	attemptKey, err := notifications.AttemptStorageKey(record.Delivery.DeliveryID, completion.AttemptID)
	if err != nil {
		return false, err
	}
	attempt, err := store.loadAttemptForTerminalCompletion(ctx, attemptKey, record.Delivery.DeliveryID, completion)
	if err != nil {
		return false, err
	}
	if attempt.Outcome != notifications.AttemptOutcomeStarted {
		return true, nil
	}

	deliveryKey, err := notifications.DeliveryStorageKey(record.Delivery.OutboxID, record.Delivery.DeliveryID)
	if err != nil {
		return false, err
	}
	encodedDeliveryKey, _ := attributevalue.MarshalMap(map[string]string{
		"PK": deliveryKey.PartitionKey, "SK": deliveryKey.SortKey,
	})
	encodedAttemptKey, _ := attributevalue.MarshalMap(map[string]string{
		"PK": attemptKey.PartitionKey, "SK": attemptKey.SortKey,
	})
	deliveryValues, _ := attributevalue.MarshalMap(map[string]any{
		":entity": "notification_delivery", ":delivery_id": record.Delivery.DeliveryID,
		":terminal_state": string(refreshed.Delivery.State), ":terminal_revision": refreshed.Revision,
	})
	attemptSet := []string{
		"#outcome = :outcome", "#completed_at = :completed_at", "#possibly_accepted = :possibly_accepted",
	}
	attemptValuesMap := map[string]any{
		":entity": "notification_attempt", ":delivery_id": record.Delivery.DeliveryID,
		":attempt_id": completion.AttemptID, ":started": string(notifications.AttemptOutcomeStarted),
		":outcome": string(completion.Outcome), ":completed_at": fixedTime(completion.CompletedAt),
		":possibly_accepted": completion.PossiblyAccepted || record.PossiblyAccepted,
	}
	if completion.ErrorCategory != "" {
		attemptValuesMap[":error_category"] = completion.ErrorCategory
		attemptSet = append(attemptSet, "#error_category = :error_category")
	}
	if completion.ProviderMessageID != "" {
		attemptValuesMap[":provider_message_id"] = completion.ProviderMessageID
		attemptSet = append(attemptSet, "#provider_message_id = :provider_message_id")
	}
	if completion.ProviderOutcome != "" {
		attemptValuesMap[":provider_outcome"] = string(completion.ProviderOutcome)
		attemptSet = append(attemptSet, "#provider_outcome = :provider_outcome")
	}
	attemptValues, err := attributevalue.MarshalMap(attemptValuesMap)
	if err != nil {
		return false, err
	}
	attemptUpdate := "SET " + strings.Join(attemptSet, ", ")
	attemptCondition := "#entity = :entity AND #delivery_id = :delivery_id AND #attempt_id = :attempt_id AND #outcome = :started"
	attemptNames := map[string]string{
		"#entity": "entity_type", "#delivery_id": "delivery_id", "#attempt_id": "attempt_id",
		"#outcome": "outcome", "#completed_at": "completed_at", "#possibly_accepted": "possibly_accepted",
		"#error_category": "error_category", "#provider_message_id": "provider_message_id",
		"#provider_outcome": "provider_outcome",
	}
	_, err = store.Client.TransactWriteItems(ctx, &awssdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{ConditionCheck: &types.ConditionCheck{
				TableName: aws.String(store.Table), Key: encodedDeliveryKey,
				ConditionExpression: aws.String("#entity = :entity AND #delivery_id = :delivery_id AND #state = :terminal_state AND #revision = :terminal_revision"),
				ExpressionAttributeNames: map[string]string{
					"#entity": "entity_type", "#delivery_id": "delivery_id", "#state": "state", "#revision": "delivery_revision",
				},
				ExpressionAttributeValues: deliveryValues,
			}},
			{Update: &types.Update{
				TableName: aws.String(store.Table), Key: encodedAttemptKey,
				UpdateExpression: aws.String(attemptUpdate), ConditionExpression: aws.String(attemptCondition),
				ExpressionAttributeNames:  usedNames(attemptNames, attemptUpdate+" "+attemptCondition),
				ExpressionAttributeValues: attemptValues,
			}},
		},
	})
	if err == nil {
		return true, nil
	}
	current, loadErr := store.loadAttemptForTerminalCompletion(ctx, attemptKey, record.Delivery.DeliveryID, completion)
	if loadErr == nil && current.Outcome != notifications.AttemptOutcomeStarted {
		return true, nil
	}
	if loadErr != nil {
		return false, loadErr
	}
	return false, fmt.Errorf("finalize current attempt beside terminal delivery: %w", err)
}

func (store Store) loadAttemptForTerminalCompletion(
	ctx context.Context,
	key notifications.StorageKey,
	deliveryID string,
	completion worker.AttemptCompletion,
) (attemptItem, error) {
	item, err := store.getConsistent(ctx, key.PartitionKey, key.SortKey)
	if err != nil {
		return attemptItem{}, err
	}
	if len(item) == 0 {
		return attemptItem{}, fmt.Errorf("current notification attempt is missing")
	}
	var attempt attemptItem
	if err := attributevalue.UnmarshalMap(item, &attempt); err != nil {
		return attemptItem{}, fmt.Errorf("decode current notification attempt: %w", err)
	}
	if attempt.PK != key.PartitionKey || attempt.SK != key.SortKey || attempt.EntityType != "notification_attempt" ||
		attempt.DeliveryID != deliveryID || attempt.AttemptID != completion.AttemptID || attempt.Outcome.Validate() != nil {
		return attemptItem{}, fmt.Errorf("current notification attempt identity is invalid")
	}
	if attempt.ProviderOutcome != "" && attempt.ProviderOutcome.Validate() != nil {
		return attemptItem{}, fmt.Errorf("current notification attempt provider outcome is invalid")
	}
	if completion.ProviderMessageID != "" && attempt.ProviderMessageID != "" &&
		completion.ProviderMessageID != attempt.ProviderMessageID {
		return attemptItem{}, fmt.Errorf("current notification attempt provider association changed")
	}
	if attempt.Outcome == notifications.AttemptOutcomeStarted {
		if attempt.CompletedAt != "" {
			return attemptItem{}, fmt.Errorf("started notification attempt has completion time")
		}
		return attempt, nil
	}
	if _, err := parseRequiredTime("current notification attempt completed_at", attempt.CompletedAt); err != nil {
		return attemptItem{}, err
	}
	return attempt, nil
}

func validateCompletion(completion worker.AttemptCompletion) error {
	switch completion.Outcome {
	case notifications.AttemptOutcomeSucceeded:
		if completion.NextState != notifications.DeliveryStateSucceeded || completion.ProviderMessageID == "" ||
			completion.ProviderOutcome != notifications.ProviderOutcomeAccepted || completion.ErrorCategory != "" {
			return fmt.Errorf("successful attempt completion is invalid")
		}
	case notifications.AttemptOutcomePermanentFailed:
		validHistoricalAmbiguity := completion.NextState == notifications.DeliveryStateRetryableFailed &&
			completion.PossiblyAccepted && completion.AmbiguousExhausted && !completion.NextAttemptAt.IsZero()
		if (completion.NextState != notifications.DeliveryStatePermanentFailed && !validHistoricalAmbiguity) || completion.ErrorCategory == "" {
			return fmt.Errorf("permanent attempt completion is invalid")
		}
	case notifications.AttemptOutcomeRetryable:
		if completion.NextState != notifications.DeliveryStateRetryableFailed &&
			completion.NextState != notifications.DeliveryStatePermanentFailed {
			return fmt.Errorf("retryable attempt completion is invalid")
		}
		if completion.ErrorCategory == "" {
			return fmt.Errorf("retryable attempt error category is required")
		}
	case notifications.AttemptOutcomeAmbiguous:
		if completion.NextState != notifications.DeliveryStateRetryableFailed || !completion.PossiblyAccepted || completion.NextAttemptAt.IsZero() {
			return fmt.Errorf("ambiguous attempt completion is invalid")
		}
	default:
		return fmt.Errorf("attempt cannot complete with outcome %q", completion.Outcome)
	}
	if completion.NextState == notifications.DeliveryStateRetryableFailed && completion.NextAttemptAt.IsZero() {
		return fmt.Errorf("retryable delivery needs next attempt time")
	}
	if completion.ProviderOutcome != "" && completion.ProviderOutcome.Validate() != nil {
		return fmt.Errorf("provider outcome is invalid")
	}
	return nil
}

func usedNames(names map[string]string, expression string) map[string]string {
	used := make(map[string]string)
	for token, name := range names {
		if strings.Contains(expression, token) {
			used[token] = name
		}
	}
	return used
}

func marshalUsedValues(values map[string]any, expression string) (map[string]types.AttributeValue, error) {
	used := make(map[string]any)
	for token, value := range values {
		if strings.Contains(expression, token) {
			used[token] = value
		}
	}
	return attributevalue.MarshalMap(used)
}
