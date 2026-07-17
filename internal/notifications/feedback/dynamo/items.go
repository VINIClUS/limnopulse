package dynamo

import (
	"fmt"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type attemptRecord struct {
	PK                string                        `dynamodbav:"PK"`
	SK                string                        `dynamodbav:"SK"`
	EntityType        string                        `dynamodbav:"entity_type"`
	DeliveryID        string                        `dynamodbav:"delivery_id"`
	AttemptID         string                        `dynamodbav:"attempt_id"`
	OutboxID          string                        `dynamodbav:"outbox_id"`
	Outcome           notifications.AttemptOutcome  `dynamodbav:"outcome"`
	ProviderOutcome   notifications.ProviderOutcome `dynamodbav:"provider_outcome"`
	ProviderMessageID string                        `dynamodbav:"provider_message_id"`
	ProviderAccepted  bool                          `dynamodbav:"provider_accepted"`
}

type deliveryRecord struct {
	PK                string                         `dynamodbav:"PK"`
	SK                string                         `dynamodbav:"SK"`
	EntityType        string                         `dynamodbav:"entity_type"`
	TenantID          string                         `dynamodbav:"tenant_id"`
	OutboxID          string                         `dynamodbav:"outbox_id"`
	DeliveryID        string                         `dynamodbav:"delivery_id"`
	EventID           string                         `dynamodbav:"event_id"`
	Kind              notifications.NotificationKind `dynamodbav:"kind"`
	Channel           notifications.Channel          `dynamodbav:"channel"`
	NormalizedEmail   string                         `dynamodbav:"normalized_email"`
	State             notifications.DeliveryState    `dynamodbav:"state"`
	Revision          int64                          `dynamodbav:"delivery_revision"`
	ProviderOutcome   notifications.ProviderOutcome  `dynamodbav:"provider_outcome"`
	ProviderMessageID string                         `dynamodbav:"provider_message_id"`
	ProviderAccepted  bool                           `dynamodbav:"provider_accepted"`
}

type suppressionRecord struct {
	Exists         bool
	Key            notifications.StorageKey
	Deliverability string
	Reason         feedback.SuppressionReason
	Rank           int
}

func decodeAttempt(
	item map[string]types.AttributeValue,
	key notifications.StorageKey,
	event feedback.Event,
) (attemptRecord, error) {
	var stored attemptRecord
	if err := attributevalue.UnmarshalMap(item, &stored); err != nil {
		return attemptRecord{}, err
	}
	if stored.PK != key.PartitionKey || stored.SK != key.SortKey || stored.EntityType != "notification_attempt" ||
		stored.DeliveryID != event.DeliveryID || stored.AttemptID != event.AttemptID || stored.OutboxID == "" ||
		stored.Outcome.Validate() != nil {
		return attemptRecord{}, fmt.Errorf("notification attempt association is invalid")
	}
	if stored.ProviderOutcome != "" && stored.ProviderOutcome.Validate() != nil {
		return attemptRecord{}, fmt.Errorf("notification attempt provider outcome is invalid")
	}
	if stored.ProviderMessageID != "" && stored.ProviderMessageID != event.ProviderMessageID {
		return attemptRecord{}, fmt.Errorf("notification attempt provider message association is invalid")
	}
	return stored, nil
}

func decodeDelivery(
	item map[string]types.AttributeValue,
	key notifications.StorageKey,
	event feedback.Event,
	outboxID string,
) (deliveryRecord, error) {
	var stored deliveryRecord
	if err := attributevalue.UnmarshalMap(item, &stored); err != nil {
		return deliveryRecord{}, err
	}
	if stored.PK != key.PartitionKey || stored.SK != key.SortKey || stored.EntityType != "notification_delivery" ||
		stored.OutboxID != outboxID || stored.DeliveryID != event.DeliveryID || stored.TenantID == "" ||
		stored.EventID == "" || stored.NormalizedEmail == "" || stored.Revision < 1 ||
		stored.Kind.Validate() != nil || stored.Channel.Validate() != nil || stored.State.Validate() != nil {
		return deliveryRecord{}, fmt.Errorf("notification delivery association is invalid")
	}
	if stored.ProviderOutcome != "" && stored.ProviderOutcome.Validate() != nil {
		return deliveryRecord{}, fmt.Errorf("notification delivery provider outcome is invalid")
	}
	return stored, nil
}

func decodeSuppression(
	item map[string]types.AttributeValue,
	key notifications.StorageKey,
) (suppressionRecord, error) {
	if len(item) == 0 {
		return suppressionRecord{Key: key}, nil
	}
	var stored struct {
		PK             string                     `dynamodbav:"PK"`
		SK             string                     `dynamodbav:"SK"`
		EntityType     string                     `dynamodbav:"entity_type"`
		Deliverability string                     `dynamodbav:"deliverability"`
		Reason         feedback.SuppressionReason `dynamodbav:"suppression_reason"`
		Rank           int                        `dynamodbav:"suppression_rank"`
	}
	if err := attributevalue.UnmarshalMap(item, &stored); err != nil {
		return suppressionRecord{}, err
	}
	if stored.PK != key.PartitionKey || stored.SK != key.SortKey {
		return suppressionRecord{}, fmt.Errorf("email deliverability identity is invalid")
	}
	record := suppressionRecord{
		Exists: true, Key: key, Deliverability: stored.Deliverability,
		Reason: stored.Reason, Rank: stored.Rank,
	}
	switch stored.Deliverability {
	case "unknown", "deliverable":
		if stored.Reason != "" || stored.Rank != 0 {
			return suppressionRecord{}, fmt.Errorf("email deliverability is invalid")
		}
	case "suppressed":
		if stored.EntityType != "" && stored.EntityType != "email_deliverability" {
			return suppressionRecord{}, fmt.Errorf("email deliverability entity is invalid")
		}
		if stored.Reason.Validate() != nil || stored.Rank != stored.Reason.Rank() {
			return suppressionRecord{}, fmt.Errorf("email deliverability suppression is invalid")
		}
	default:
		return suppressionRecord{}, fmt.Errorf("email deliverability is invalid")
	}
	return record, nil
}

func nextAttemptOutcome(current notifications.AttemptOutcome, event feedback.Event) notifications.AttemptOutcome {
	switch current {
	case notifications.AttemptOutcomeStarted, notifications.AttemptOutcomeRetryable, notifications.AttemptOutcomeAmbiguous:
		if event.PermanentFailure {
			return notifications.AttemptOutcomePermanentFailed
		}
		if event.AcceptedEvidence {
			return notifications.AttemptOutcomeSucceeded
		}
	}
	return current
}
