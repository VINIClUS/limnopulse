package dynamo

import (
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type providerMetadata struct {
	Outcome   notifications.ProviderOutcome
	MessageID string
	AttemptID string
	Accepted  bool
}

type deliveryProviderDecision struct {
	Metadata                 providerMetadata
	OwnsAggregate            bool
	AcceptedByAnotherAttempt bool
}

func (store Store) reconcileTransaction(
	event feedback.Event,
	now time.Time,
	transportKey notifications.StorageKey,
	semanticKey notifications.StorageKey,
	attemptKey notifications.StorageKey,
	deliveryKey notifications.StorageKey,
	attempt attemptRecord,
	delivery deliveryRecord,
	decision deliveryProviderDecision,
	recoveryOperation *types.TransactWriteItem,
	suppression suppressionRecord,
	writeSuppression bool,
) (*awssdk.TransactWriteItemsInput, error) {
	transport, err := store.transportPut(transportKey, event, now)
	if err != nil {
		return nil, err
	}
	semantic, err := store.semanticPut(semanticKey, event, now)
	if err != nil {
		return nil, err
	}
	attemptUpdate, err := store.attemptUpdate(attemptKey, attempt, event, now)
	if err != nil {
		return nil, err
	}
	var deliveryOperation types.TransactWriteItem
	if decision.OwnsAggregate {
		deliveryUpdate, updateErr := store.deliveryUpdate(deliveryKey, delivery, event, now, decision)
		if updateErr != nil {
			return nil, updateErr
		}
		deliveryOperation.Update = deliveryUpdate
	} else {
		deliveryCheck, checkErr := store.deliveryConditionCheck(deliveryKey, delivery)
		if checkErr != nil {
			return nil, checkErr
		}
		deliveryOperation.ConditionCheck = deliveryCheck
	}
	items := []types.TransactWriteItem{
		{Put: transport}, {Put: semantic}, {Update: attemptUpdate}, deliveryOperation,
	}
	if recoveryOperation != nil {
		items = append(items, *recoveryOperation)
	}
	if writeSuppression {
		put, putErr := store.suppressionPut(event, delivery, suppression, now)
		if putErr != nil {
			return nil, putErr
		}
		items = append(items, types.TransactWriteItem{Put: put})
	}
	return &awssdk.TransactWriteItemsInput{
		TransactItems: items,
	}, nil
}

func (store Store) transportPut(
	key notifications.StorageKey,
	event feedback.Event,
	now time.Time,
) (*types.Put, error) {
	retention := store.TransportRetention
	if retention <= 0 {
		retention = DefaultTransportRetention
	}
	hash := strings.TrimPrefix(key.PartitionKey, "SES_FEEDBACK_TRANSPORT#")
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "ses_feedback_transport_dedupe",
		"schema_version": int64(1), "event_bridge_id_hash": hash,
		"created_at": fixedTime(now), "expires_at": now.Add(retention).Unix(),
	})
	if err != nil {
		return nil, err
	}
	return &types.Put{
		TableName: aws.String(store.Table), Item: item,
		ConditionExpression: aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)"),
	}, nil
}

func (store Store) semanticPut(
	key notifications.StorageKey,
	event feedback.Event,
	now time.Time,
) (*types.Put, error) {
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "ses_provider_event",
		"schema_version": int64(1), "provider_message_id": event.ProviderMessageID,
		"semantic_type": string(event.SemanticType), "delivery_id": event.DeliveryID,
		"attempt_id": event.AttemptID, "provider_outcome": string(event.ProviderOutcome),
		"observed_at": fixedTime(now),
	})
	if err != nil {
		return nil, err
	}
	return &types.Put{
		TableName: aws.String(store.Table), Item: item,
		ConditionExpression: aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)"),
	}, nil
}

func (store Store) attemptUpdate(
	key notifications.StorageKey,
	current attemptRecord,
	event feedback.Event,
	now time.Time,
) (*types.Update, error) {
	provider, err := reconcileProviderMetadata(
		current.ProviderOutcome,
		current.ProviderMessageID,
		current.ProviderAccepted,
		event,
	)
	if err != nil {
		return nil, err
	}
	nextOutcome := nextAttemptOutcome(current.Outcome, event)
	valuesMap := map[string]any{
		":entity": "notification_attempt", ":delivery_id": current.DeliveryID, ":attempt_id": current.AttemptID,
		":current_outcome": string(current.Outcome), ":next_outcome": string(nextOutcome),
		":provider_outcome": string(provider.Outcome), ":provider_message_id": provider.MessageID,
		":provider_accepted": provider.Accepted, ":now": fixedTime(now),
	}
	condition := "#entity = :entity AND #delivery_id = :delivery_id AND #attempt_id = :attempt_id AND #outcome = :current_outcome"
	condition = addCurrentCondition(condition, valuesMap, "#current_provider_outcome", ":current_provider_outcome", string(current.ProviderOutcome))
	condition = addCurrentCondition(condition, valuesMap, "#current_provider_message_id", ":current_provider_message_id", current.ProviderMessageID)
	update := "SET #outcome = :next_outcome, #provider_outcome = :provider_outcome, #provider_message_id = :provider_message_id, #provider_accepted = :provider_accepted, #feedback_updated_at = :now"
	if nextOutcome != current.Outcome {
		update += ", #completed_at = if_not_exists(#completed_at, :now)"
	}
	if event.AcceptedEvidence {
		update += ", #provider_accepted_at = if_not_exists(#provider_accepted_at, :now)"
	}
	if nextOutcome == notifications.AttemptOutcomePermanentFailed {
		valuesMap[":feedback_error"] = "provider_rejected"
		update += ", #error_category = if_not_exists(#error_category, :feedback_error)"
	}
	names := map[string]string{
		"#entity": "entity_type", "#delivery_id": "delivery_id", "#attempt_id": "attempt_id", "#outcome": "outcome",
		"#provider_outcome": "provider_outcome", "#provider_message_id": "provider_message_id",
		"#provider_accepted": "provider_accepted", "#feedback_updated_at": "feedback_updated_at",
		"#provider_accepted_at": "provider_accepted_at", "#current_provider_outcome": "provider_outcome",
		"#current_provider_message_id": "provider_message_id", "#completed_at": "completed_at",
		"#error_category": "error_category",
	}
	values, err := attributevalue.MarshalMap(valuesMap)
	if err != nil {
		return nil, err
	}
	return &types.Update{
		TableName: aws.String(store.Table), Key: marshalKey(key), UpdateExpression: aws.String(update),
		ConditionExpression: aws.String(condition), ExpressionAttributeNames: usedNames(names, update+condition),
		ExpressionAttributeValues: values,
	}, nil
}

func (store Store) deliveryUpdate(
	key notifications.StorageKey,
	current deliveryRecord,
	event feedback.Event,
	now time.Time,
	decision deliveryProviderDecision,
) (*types.Update, error) {
	nextState, err := reconciledDeliveryState(current, event, decision)
	if err != nil {
		return nil, err
	}
	valuesMap := map[string]any{
		":entity": "notification_delivery", ":outbox_id": current.OutboxID, ":delivery_id": current.DeliveryID,
		":current_state": string(current.State), ":next_state": string(nextState),
		":current_revision": current.Revision, ":next_revision": current.Revision + 1,
		":provider_accepted": decision.Metadata.Accepted, ":now": fixedTime(now),
	}
	condition := "#entity = :entity AND #outbox_id = :outbox_id AND #delivery_id = :delivery_id AND #state = :current_state AND #revision = :current_revision"
	condition = addOptionalStringCondition(condition, valuesMap, "#current_provider_outcome", ":current_provider_outcome", string(current.ProviderOutcome))
	condition = addOptionalStringCondition(condition, valuesMap, "#current_provider_message_id", ":current_provider_message_id", current.ProviderMessageID)
	sets := []string{
		"#state = :next_state", "#revision = :next_revision", "#provider_accepted = :provider_accepted",
		"#feedback_updated_at = :now", "#updated_at = :now",
	}
	removes := make([]string, 0, 8)
	for _, optional := range []struct {
		name  string
		token string
		value string
	}{
		{"#provider_outcome", ":provider_outcome", string(decision.Metadata.Outcome)},
		{"#provider_message_id", ":provider_message_id", decision.Metadata.MessageID},
		{"#provider_attempt_id", ":provider_attempt_id", decision.Metadata.AttemptID},
	} {
		if optional.value == "" {
			removes = append(removes, optional.name)
			continue
		}
		valuesMap[optional.token] = optional.value
		sets = append(sets, optional.name+" = "+optional.token)
	}
	if decision.OwnsAggregate && event.AcceptedEvidence {
		sets = append(sets, "#provider_accepted_at = if_not_exists(#provider_accepted_at, :now)")
	}
	if nextState != notifications.DeliveryStateProcessing && nextState != notifications.DeliveryStateRetryableFailed {
		removes = append(removes,
			"#lease_owner", "#lease_expires", "#next_attempt_at", "#ambiguous_exhausted", "#awaiting_intervention",
		)
	}
	update := "SET " + strings.Join(sets, ", ")
	if len(removes) != 0 {
		update += " REMOVE " + strings.Join(removes, ", ")
	}
	names := map[string]string{
		"#entity": "entity_type", "#outbox_id": "outbox_id", "#delivery_id": "delivery_id", "#state": "state",
		"#revision": "delivery_revision", "#provider_outcome": "provider_outcome",
		"#provider_message_id": "provider_message_id", "#provider_accepted": "provider_accepted",
		"#provider_attempt_id": "provider_attempt_id",
		"#feedback_updated_at": "feedback_updated_at", "#updated_at": "updated_at",
		"#provider_accepted_at": "provider_accepted_at", "#lease_owner": "delivery_lease_owner",
		"#lease_expires": "delivery_lease_expires_at", "#next_attempt_at": "next_attempt_at",
		"#ambiguous_exhausted": "ambiguous_exhausted", "#awaiting_intervention": "awaiting_intervention",
		"#current_provider_outcome": "provider_outcome", "#current_provider_message_id": "provider_message_id",
	}
	condition = addOptionalStringCondition(condition, valuesMap, "#current_provider_attempt_id", ":current_provider_attempt_id", current.ProviderAttemptID)
	names["#current_provider_attempt_id"] = "provider_attempt_id"
	values, err := attributevalue.MarshalMap(valuesMap)
	if err != nil {
		return nil, err
	}
	return &types.Update{
		TableName: aws.String(store.Table), Key: marshalKey(key), UpdateExpression: aws.String(update),
		ConditionExpression: aws.String(condition), ExpressionAttributeNames: usedNames(names, update+condition),
		ExpressionAttributeValues: values,
	}, nil
}

func reconciledDeliveryState(
	current deliveryRecord,
	event feedback.Event,
	decision deliveryProviderDecision,
) (notifications.DeliveryState, error) {
	incomingStateOutcome := event.ProviderOutcome
	if event.PermanentFailure && decision.AcceptedByAnotherAttempt {
		incomingStateOutcome = notifications.ProviderOutcomeAccepted
	}
	return notifications.ReconcileDeliveryStateFromProvider(current.State, incomingStateOutcome)
}

func (store Store) deliveryConditionCheck(
	key notifications.StorageKey,
	current deliveryRecord,
) (*types.ConditionCheck, error) {
	valuesMap := map[string]any{
		":entity": "notification_delivery", ":outbox_id": current.OutboxID, ":delivery_id": current.DeliveryID,
		":current_state": string(current.State), ":current_revision": current.Revision,
	}
	condition := "#entity = :entity AND #outbox_id = :outbox_id AND #delivery_id = :delivery_id AND #state = :current_state AND #revision = :current_revision"
	condition = addOptionalStringCondition(condition, valuesMap, "#current_provider_outcome", ":current_provider_outcome", string(current.ProviderOutcome))
	condition = addOptionalStringCondition(condition, valuesMap, "#current_provider_message_id", ":current_provider_message_id", current.ProviderMessageID)
	condition = addOptionalStringCondition(condition, valuesMap, "#current_provider_attempt_id", ":current_provider_attempt_id", current.ProviderAttemptID)
	names := map[string]string{
		"#entity": "entity_type", "#outbox_id": "outbox_id", "#delivery_id": "delivery_id", "#state": "state",
		"#revision": "delivery_revision", "#current_provider_outcome": "provider_outcome",
		"#current_provider_message_id": "provider_message_id", "#current_provider_attempt_id": "provider_attempt_id",
	}
	values, err := attributevalue.MarshalMap(valuesMap)
	if err != nil {
		return nil, err
	}
	return &types.ConditionCheck{
		TableName: aws.String(store.Table), Key: marshalKey(key), ConditionExpression: aws.String(condition),
		ExpressionAttributeNames: usedNames(names, condition), ExpressionAttributeValues: values,
	}, nil
}

func (store Store) suppressionPut(
	event feedback.Event,
	delivery deliveryRecord,
	current suppressionRecord,
	now time.Time,
) (*types.Put, error) {
	key, err := notifications.DeliverabilityStorageKey(delivery.NormalizedEmail)
	if err != nil {
		return nil, err
	}
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK": key.PartitionKey, "SK": key.SortKey, "entity_type": "email_deliverability",
		"schema_version": int64(1), "deliverability": notifications.EmailDeliverabilitySuppressed,
		"suppression_reason": string(event.SuppressionReason), "suppression_rank": int64(event.SuppressionReason.Rank()),
		"source_delivery_id": delivery.DeliveryID, "source_attempt_id": event.AttemptID,
		"source_provider_message_id": event.ProviderMessageID, "suppressed_at": fixedTime(now), "updated_at": fixedTime(now),
	})
	if err != nil {
		return nil, err
	}
	put := &types.Put{TableName: aws.String(store.Table), Item: item}
	if !current.Exists {
		put.ConditionExpression = aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)")
		return put, nil
	}
	valuesMap := map[string]any{":deliverability": current.Deliverability}
	condition := "#deliverability = :deliverability"
	names := map[string]string{"#deliverability": "deliverability", "#rank": "suppression_rank", "#reason": "suppression_reason"}
	if current.Rank == 0 {
		condition += " AND attribute_not_exists(#rank)"
	} else {
		condition += " AND #rank = :rank AND #reason = :reason"
		valuesMap[":rank"] = int64(current.Rank)
		valuesMap[":reason"] = string(current.Reason)
	}
	values, err := attributevalue.MarshalMap(valuesMap)
	if err != nil {
		return nil, err
	}
	put.ConditionExpression = aws.String(condition)
	put.ExpressionAttributeNames = usedNames(names, condition)
	put.ExpressionAttributeValues = values
	return put, nil
}

func addCurrentCondition(condition string, values map[string]any, name, token, current string) string {
	if current == "" {
		return condition + " AND attribute_not_exists(" + name + ")"
	}
	values[token] = current
	return condition + " AND " + name + " = " + token
}

func addOptionalStringCondition(condition string, values map[string]any, name, token, current string) string {
	values[token] = current
	if current == "" {
		return condition + " AND (attribute_not_exists(" + name + ") OR " + name + " = " + token + ")"
	}
	return condition + " AND " + name + " = " + token
}

func marshalKey(key notifications.StorageKey) map[string]types.AttributeValue {
	encoded, _ := attributevalue.MarshalMap(map[string]string{"PK": key.PartitionKey, "SK": key.SortKey})
	return encoded
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

func reconcileProviderMetadata(
	currentOutcome notifications.ProviderOutcome,
	currentMessageID string,
	currentAccepted bool,
	event feedback.Event,
) (providerMetadata, error) {
	outcome, err := notifications.ReconcileProviderOutcome(currentOutcome, event.ProviderOutcome)
	if err != nil {
		return providerMetadata{}, err
	}
	messageID := currentMessageID
	if messageID == "" {
		messageID = event.ProviderMessageID
	}
	return providerMetadata{
		Outcome: outcome, MessageID: messageID,
		Accepted: currentAccepted || event.AcceptedEvidence,
	}, nil
}

func reconcileDeliveryProviderMetadata(
	current deliveryRecord,
	event feedback.Event,
) (deliveryProviderDecision, error) {
	sameOwner := current.ProviderAttemptID != "" && current.ProviderAttemptID == event.AttemptID
	latestAttempt := current.LastAttemptID != "" && current.LastAttemptID == event.AttemptID
	acceptedByAnotherAttempt := current.ProviderAccepted &&
		((current.ProviderAttemptID != "" && !sameOwner) ||
			(current.ProviderAttemptID == "" && current.ProviderMessageID != "" &&
				current.ProviderMessageID != event.ProviderMessageID))
	legacyOwner := current.ProviderAttemptID == "" &&
		(current.LastAttemptID == "" || latestAttempt) &&
		(current.ProviderMessageID == "" || current.ProviderMessageID == event.ProviderMessageID)
	acceptedWithoutProviderOwner := event.AcceptedEvidence && event.CompletesAttempt() && current.ProviderAttemptID == "" &&
		current.ProviderMessageID == "" && current.ProviderOutcome == ""
	if !sameOwner && !latestAttempt && !legacyOwner && !acceptedWithoutProviderOwner {
		return deliveryProviderDecision{
			Metadata: providerMetadata{
				Outcome: current.ProviderOutcome, MessageID: current.ProviderMessageID,
				AttemptID: current.ProviderAttemptID, Accepted: current.ProviderAccepted,
			},
			AcceptedByAnotherAttempt: acceptedByAnotherAttempt,
		}, nil
	}

	currentOutcome := current.ProviderOutcome
	currentMessageID := current.ProviderMessageID
	currentAccepted := current.ProviderAccepted
	legacyMessageHasDifferentOwner := current.ProviderAttemptID == "" &&
		current.ProviderMessageID != "" && current.ProviderMessageID != event.ProviderMessageID
	if latestAttempt && ((current.ProviderAttemptID != "" && !sameOwner) || legacyMessageHasDifferentOwner) {
		currentOutcome, currentMessageID = "", ""
	}
	provider, err := reconcileProviderMetadata(currentOutcome, currentMessageID, currentAccepted, event)
	if err != nil {
		return deliveryProviderDecision{}, err
	}
	provider.AttemptID = event.AttemptID
	return deliveryProviderDecision{
		Metadata: provider, OwnsAggregate: true,
		AcceptedByAnotherAttempt: acceptedByAnotherAttempt,
	}, nil
}
