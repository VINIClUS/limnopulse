package dynamo

import (
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type deliveryItem struct {
	PK                     string                           `dynamodbav:"PK"`
	SK                     string                           `dynamodbav:"SK"`
	EntityType             string                           `dynamodbav:"entity_type"`
	RelaySchemaVersion     int64                            `dynamodbav:"relay_schema_version"`
	TenantID               string                           `dynamodbav:"tenant_id"`
	OutboxID               string                           `dynamodbav:"outbox_id"`
	DeliveryID             string                           `dynamodbav:"delivery_id"`
	EventID                string                           `dynamodbav:"event_id"`
	RuleID                 string                           `dynamodbav:"rule_id"`
	Kind                   notifications.NotificationKind   `dynamodbav:"kind"`
	Channel                notifications.Channel            `dynamodbav:"channel"`
	DependsOnOutboxID      string                           `dynamodbav:"depends_on_outbox_id"`
	DependsOnDeliveryID    string                           `dynamodbav:"depends_on_delivery_id"`
	RecipientID            string                           `dynamodbav:"recipient_id"`
	NormalizedEmail        string                           `dynamodbav:"normalized_email"`
	MembershipSnapshot     notifications.MembershipSnapshot `dynamodbav:"membership_snapshot"`
	State                  notifications.DeliveryState      `dynamodbav:"state"`
	Content                contentItem                      `dynamodbav:"content"`
	CancellationReason     notifications.CancellationReason `dynamodbav:"cancellation_reason"`
	CreatedAt              string                           `dynamodbav:"created_at"`
	UpdatedAt              string                           `dynamodbav:"updated_at"`
	DeliveryRevision       int64                            `dynamodbav:"delivery_revision"`
	AttemptCount           int                              `dynamodbav:"attempt_count"`
	LastAttemptID          string                           `dynamodbav:"last_attempt_id"`
	DeliveryLeaseOwner     string                           `dynamodbav:"delivery_lease_owner"`
	DeliveryLeaseEpoch     int64                            `dynamodbav:"delivery_lease_epoch"`
	DeliveryLeaseExpiresAt string                           `dynamodbav:"delivery_lease_expires_at"`
	RelayLeaseExpiresAt    string                           `dynamodbav:"relay_lease_expires_at"`
	NextAttemptAt          string                           `dynamodbav:"next_attempt_at"`
	ProviderOutcome        notifications.ProviderOutcome    `dynamodbav:"provider_outcome"`
	ProviderMessageID      string                           `dynamodbav:"provider_message_id"`
	ProviderAttemptID      string                           `dynamodbav:"provider_attempt_id"`
	PossiblyAccepted       bool                             `dynamodbav:"possibly_accepted"`
	AmbiguousExhausted     bool                             `dynamodbav:"ambiguous_exhausted"`
	AwaitingIntervention   bool                             `dynamodbav:"awaiting_intervention"`
	AwaitingDLQ            bool                             `dynamodbav:"awaiting_dlq"`
}

type contentItem struct {
	TemplateID      notifications.TemplateID `dynamodbav:"template_id"`
	TemplateVersion int                      `dynamodbav:"template_version"`
	Locale          notifications.Locale     `dynamodbav:"locale"`
	Subject         string                   `dynamodbav:"subject"`
	Text            string                   `dynamodbav:"text"`
	HTML            string                   `dynamodbav:"html"`
	ContentHash     string                   `dynamodbav:"content_hash"`
}

type attemptItem struct {
	PK                string                        `dynamodbav:"PK"`
	SK                string                        `dynamodbav:"SK"`
	EntityType        string                        `dynamodbav:"entity_type"`
	DeliveryID        string                        `dynamodbav:"delivery_id"`
	AttemptID         string                        `dynamodbav:"attempt_id"`
	Outcome           notifications.AttemptOutcome  `dynamodbav:"outcome"`
	CompletedAt       string                        `dynamodbav:"completed_at"`
	ProviderOutcome   notifications.ProviderOutcome `dynamodbav:"provider_outcome"`
	ProviderMessageID string                        `dynamodbav:"provider_message_id"`
}

func decodeDelivery(item map[string]types.AttributeValue) (deliveryItem, worker.DeliveryRecord, error) {
	var stored deliveryItem
	if err := attributevalue.UnmarshalMap(item, &stored); err != nil {
		return deliveryItem{}, worker.DeliveryRecord{}, fmt.Errorf("decode notification delivery: %w", err)
	}
	if stored.EntityType != "notification_delivery" || stored.RelaySchemaVersion != notifications.RelaySchemaVersion ||
		stored.PK != "NOTIFICATION_OUTBOX#"+stored.OutboxID || stored.SK != "DELIVERY#"+stored.DeliveryID ||
		stored.DeliveryRevision < 1 || stored.AttemptCount < 0 || stored.AttemptCount > worker.MaxProviderCalls ||
		stored.DeliveryLeaseEpoch < 0 {
		return deliveryItem{}, worker.DeliveryRecord{}, fmt.Errorf("notification delivery storage identity is invalid")
	}
	if stored.ProviderOutcome != "" && stored.ProviderOutcome.Validate() != nil {
		return deliveryItem{}, worker.DeliveryRecord{}, fmt.Errorf("notification delivery provider outcome is invalid")
	}
	createdAt, err := parseRequiredTime("delivery created_at", stored.CreatedAt)
	if err != nil {
		return deliveryItem{}, worker.DeliveryRecord{}, err
	}
	updatedAt, err := parseRequiredTime("delivery updated_at", stored.UpdatedAt)
	if err != nil {
		return deliveryItem{}, worker.DeliveryRecord{}, err
	}
	snapshot := notifications.DeliverySnapshot{
		TenantID: stored.TenantID, OutboxID: stored.OutboxID, DeliveryID: stored.DeliveryID,
		EventID: stored.EventID, RuleID: stored.RuleID, Kind: stored.Kind, Channel: stored.Channel,
		DependsOnOutboxID: stored.DependsOnOutboxID, DependsOnDeliveryID: stored.DependsOnDeliveryID,
		RecipientID: stored.RecipientID, NormalizedEmail: stored.NormalizedEmail,
		MembershipSnapshot: stored.MembershipSnapshot, State: stored.State,
		Content: notifications.RenderedContentSnapshot{
			TemplateID: stored.Content.TemplateID, TemplateVersion: stored.Content.TemplateVersion,
			Locale: stored.Content.Locale, Subject: stored.Content.Subject, Text: stored.Content.Text,
			HTML: stored.Content.HTML, ContentHash: stored.Content.ContentHash,
		},
		CancellationReason: stored.CancellationReason, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if _, err := notifications.RestoreDelivery(snapshot); err != nil {
		return deliveryItem{}, worker.DeliveryRecord{}, fmt.Errorf("validate notification delivery: %w", err)
	}
	record := worker.DeliveryRecord{
		Delivery: snapshot, Revision: stored.DeliveryRevision, AttemptCount: stored.AttemptCount,
		LastAttemptID: stored.LastAttemptID, LeaseOwner: stored.DeliveryLeaseOwner,
		LeaseEpoch: stored.DeliveryLeaseEpoch, ProviderOutcome: stored.ProviderOutcome,
		ProviderMessageID: stored.ProviderMessageID, ProviderAttemptID: stored.ProviderAttemptID,
		PossiblyAccepted:     stored.PossiblyAccepted,
		AmbiguousExhausted:   stored.AmbiguousExhausted,
		AwaitingIntervention: stored.AwaitingIntervention, AwaitingDLQ: stored.AwaitingDLQ,
	}
	if stored.DeliveryLeaseExpiresAt != "" {
		record.LeaseExpiresAt, err = parseRequiredTime("delivery lease expiry", stored.DeliveryLeaseExpiresAt)
		if err != nil {
			return deliveryItem{}, worker.DeliveryRecord{}, err
		}
	}
	if stored.NextAttemptAt != "" {
		record.NextAttemptAt, err = parseRequiredTime("next attempt time", stored.NextAttemptAt)
		if err != nil {
			return deliveryItem{}, worker.DeliveryRecord{}, err
		}
	}
	return stored, record, nil
}

func parseRequiredTime(name, raw string) (time.Time, error) {
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || value.IsZero() {
		return time.Time{}, fmt.Errorf("%s is invalid", name)
	}
	return value.UTC(), nil
}

func fixedTime(value time.Time) string { return value.UTC().Format("2006-01-02T15:04:05.000000000Z") }

func validateTransition(snapshot notifications.DeliverySnapshot, next notifications.DeliveryState) error {
	delivery, err := notifications.RestoreDelivery(snapshot)
	if err != nil {
		return err
	}
	_, err = delivery.ApplyTransition(next)
	return err
}
