package notifications

import (
	"fmt"
	"time"
)

type Outbox struct {
	TenantID          string
	OutboxID          string
	EventID           string
	RuleID            string
	Kind              NotificationKind
	Channel           Channel
	DependsOnOutboxID string
	CreatedAt         time.Time
}

func (outbox Outbox) Validate() error {
	for name, value := range map[string]string{
		"tenant ID": outbox.TenantID,
		"outbox ID": outbox.OutboxID,
		"event ID":  outbox.EventID,
		"rule ID":   outbox.RuleID,
	} {
		if err := validateIdentityField(name, value); err != nil {
			return err
		}
	}
	if err := outbox.Kind.Validate(); err != nil {
		return err
	}
	if err := outbox.Channel.Validate(); err != nil {
		return err
	}
	if err := validateOpeningRelationship(outbox.Kind, "dependent opening outbox ID", outbox.DependsOnOutboxID); err != nil {
		return err
	}
	if outbox.CreatedAt.IsZero() {
		return fmt.Errorf("outbox created time must not be zero")
	}
	return nil
}

type MembershipSnapshot struct {
	Role    string
	Status  string
	Version int64
}

func (snapshot MembershipSnapshot) Validate() error {
	if err := validateIdentityField("membership role", snapshot.Role); err != nil {
		return err
	}
	if err := validateIdentityField("membership status", snapshot.Status); err != nil {
		return err
	}
	if snapshot.Version < 1 {
		return fmt.Errorf("membership version must be at least 1")
	}
	return nil
}

type Delivery struct {
	TenantID            string
	OutboxID            string
	DeliveryID          string
	EventID             string
	RuleID              string
	Kind                NotificationKind
	Channel             Channel
	DependsOnOutboxID   string
	DependsOnDeliveryID string
	RecipientID         string
	NormalizedEmail     string
	MembershipSnapshot  MembershipSnapshot
	State               DeliveryState
	Content             RenderedContent
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (delivery Delivery) Validate() error {
	for name, value := range map[string]string{
		"tenant ID":        delivery.TenantID,
		"outbox ID":        delivery.OutboxID,
		"delivery ID":      delivery.DeliveryID,
		"event ID":         delivery.EventID,
		"rule ID":          delivery.RuleID,
		"recipient ID":     delivery.RecipientID,
		"normalized email": delivery.NormalizedEmail,
	} {
		if err := validateIdentityField(name, value); err != nil {
			return err
		}
	}
	if err := delivery.Kind.Validate(); err != nil {
		return err
	}
	if err := delivery.Channel.Validate(); err != nil {
		return err
	}
	if err := delivery.State.Validate(); err != nil {
		return err
	}
	if err := validateOpeningRelationship(delivery.Kind, "dependent opening outbox ID", delivery.DependsOnOutboxID); err != nil {
		return err
	}
	if err := validateOpeningRelationship(delivery.Kind, "dependent opening delivery ID", delivery.DependsOnDeliveryID); err != nil {
		return err
	}
	if err := delivery.MembershipSnapshot.Validate(); err != nil {
		return err
	}
	if err := delivery.Content.Validate(); err != nil {
		return err
	}
	wantTemplateID := TemplateAlertOpeningV1
	if delivery.Kind == NotificationKindRecovery {
		wantTemplateID = TemplateAlertRecoveryV1
	}
	if delivery.Content.TemplateID() != wantTemplateID {
		return fmt.Errorf("template ID %q does not match notification kind %q", delivery.Content.TemplateID(), delivery.Kind)
	}
	wantID, err := NewDeliveryID(delivery.EventID, delivery.Kind, delivery.Channel, delivery.RecipientID)
	if err != nil {
		return err
	}
	if delivery.DeliveryID != wantID {
		return fmt.Errorf("delivery ID does not match canonical identity inputs")
	}
	if delivery.CreatedAt.IsZero() || delivery.UpdatedAt.IsZero() {
		return fmt.Errorf("delivery timestamps must not be zero")
	}
	if delivery.UpdatedAt.Before(delivery.CreatedAt) {
		return fmt.Errorf("delivery updated time must not precede created time")
	}
	return nil
}

type Attempt struct {
	DeliveryID      string
	AttemptID       string
	Outcome         AttemptOutcome
	ProviderOutcome ProviderOutcome
	StartedAt       time.Time
	CompletedAt     time.Time
}

func (attempt Attempt) Validate() error {
	if err := validateIdentityField("delivery ID", attempt.DeliveryID); err != nil {
		return err
	}
	if err := validateIdentityField("attempt ID", attempt.AttemptID); err != nil {
		return err
	}
	if err := attempt.Outcome.Validate(); err != nil {
		return err
	}
	if attempt.ProviderOutcome != "" {
		if err := attempt.ProviderOutcome.Validate(); err != nil {
			return err
		}
	}
	if attempt.StartedAt.IsZero() {
		return fmt.Errorf("attempt start time must not be zero")
	}
	if !attempt.CompletedAt.IsZero() && attempt.CompletedAt.Before(attempt.StartedAt) {
		return fmt.Errorf("attempt completion time must not precede start time")
	}
	return nil
}

func validateOpeningRelationship(kind NotificationKind, name, value string) error {
	switch kind {
	case NotificationKindOpening:
		if value != "" {
			return fmt.Errorf("%s must be empty for an opening notification", name)
		}
	case NotificationKindRecovery:
		if err := validateIdentityField(name, value); err != nil {
			return err
		}
	default:
		return kind.Validate()
	}
	return nil
}
