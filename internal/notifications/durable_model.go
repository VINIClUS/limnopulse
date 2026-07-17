package notifications

import (
	"fmt"
	"time"
)

type CancellationReason string

const (
	CancellationReasonCancelled                   CancellationReason = "cancelled"
	CancellationReasonEmailSuppressed             CancellationReason = "email_suppressed"
	CancellationReasonRecipientMembershipInactive CancellationReason = "recipient_membership_inactive"
	CancellationReasonOpeningNotSucceeded         CancellationReason = "opening_delivery_not_succeeded"
)

func (reason CancellationReason) Validate() error {
	switch reason {
	case CancellationReasonCancelled,
		CancellationReasonEmailSuppressed,
		CancellationReasonRecipientMembershipInactive,
		CancellationReasonOpeningNotSucceeded:
		return nil
	default:
		return fmt.Errorf("unknown delivery cancellation reason %q", reason)
	}
}

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

type DeliveryParams struct {
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
	Content             RenderedContent
	CancellationReason  CancellationReason
	CreatedAt           time.Time
	UpdatedAt           time.Time
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
	Content             RenderedContent
	cancellationReason  CancellationReason
	CreatedAt           time.Time
	UpdatedAt           time.Time
	state               DeliveryState
}

type DeliverySnapshot struct {
	TenantID            string                  `json:"tenant_id"`
	OutboxID            string                  `json:"outbox_id"`
	DeliveryID          string                  `json:"delivery_id"`
	EventID             string                  `json:"event_id"`
	RuleID              string                  `json:"rule_id"`
	Kind                NotificationKind        `json:"kind"`
	Channel             Channel                 `json:"channel"`
	DependsOnOutboxID   string                  `json:"depends_on_outbox_id,omitempty"`
	DependsOnDeliveryID string                  `json:"depends_on_delivery_id,omitempty"`
	RecipientID         string                  `json:"recipient_id"`
	NormalizedEmail     string                  `json:"normalized_email"`
	MembershipSnapshot  MembershipSnapshot      `json:"membership_snapshot"`
	State               DeliveryState           `json:"state"`
	Content             RenderedContentSnapshot `json:"content"`
	CancellationReason  CancellationReason      `json:"cancellation_reason,omitempty"`
	CreatedAt           time.Time               `json:"created_at"`
	UpdatedAt           time.Time               `json:"updated_at"`
}

func NewPendingDelivery(params DeliveryParams) (Delivery, error) {
	return newDelivery(params, DeliveryStatePending)
}

func NewCancelledDelivery(params DeliveryParams) (Delivery, error) {
	if params.CancellationReason == "" {
		params.CancellationReason = CancellationReasonCancelled
	}
	return newDelivery(params, DeliveryStateCancelled)
}

func newDelivery(params DeliveryParams, state DeliveryState) (Delivery, error) {
	delivery := Delivery{
		TenantID:            params.TenantID,
		OutboxID:            params.OutboxID,
		DeliveryID:          params.DeliveryID,
		EventID:             params.EventID,
		RuleID:              params.RuleID,
		Kind:                params.Kind,
		Channel:             params.Channel,
		DependsOnOutboxID:   params.DependsOnOutboxID,
		DependsOnDeliveryID: params.DependsOnDeliveryID,
		RecipientID:         params.RecipientID,
		NormalizedEmail:     params.NormalizedEmail,
		MembershipSnapshot:  params.MembershipSnapshot,
		Content:             params.Content,
		cancellationReason:  params.CancellationReason,
		CreatedAt:           params.CreatedAt,
		UpdatedAt:           params.UpdatedAt,
		state:               state,
	}
	if err := delivery.Validate(); err != nil {
		return Delivery{}, err
	}
	return delivery, nil
}

func (delivery Delivery) State() DeliveryState {
	return delivery.state
}

func (delivery Delivery) CancellationReason() CancellationReason {
	return delivery.cancellationReason
}

func (delivery Delivery) Snapshot() DeliverySnapshot {
	return DeliverySnapshot{
		TenantID:            delivery.TenantID,
		OutboxID:            delivery.OutboxID,
		DeliveryID:          delivery.DeliveryID,
		EventID:             delivery.EventID,
		RuleID:              delivery.RuleID,
		Kind:                delivery.Kind,
		Channel:             delivery.Channel,
		DependsOnOutboxID:   delivery.DependsOnOutboxID,
		DependsOnDeliveryID: delivery.DependsOnDeliveryID,
		RecipientID:         delivery.RecipientID,
		NormalizedEmail:     delivery.NormalizedEmail,
		MembershipSnapshot:  delivery.MembershipSnapshot,
		State:               delivery.state,
		Content:             delivery.Content.Snapshot(),
		CancellationReason:  delivery.cancellationReason,
		CreatedAt:           delivery.CreatedAt,
		UpdatedAt:           delivery.UpdatedAt,
	}
}

func RestoreDelivery(snapshot DeliverySnapshot) (Delivery, error) {
	var content RenderedContent
	if snapshot.Content != (RenderedContentSnapshot{}) {
		var err error
		content, err = RestoreRenderedContent(snapshot.Content)
		if err != nil {
			return Delivery{}, err
		}
	}
	delivery := Delivery{
		TenantID:            snapshot.TenantID,
		OutboxID:            snapshot.OutboxID,
		DeliveryID:          snapshot.DeliveryID,
		EventID:             snapshot.EventID,
		RuleID:              snapshot.RuleID,
		Kind:                snapshot.Kind,
		Channel:             snapshot.Channel,
		DependsOnOutboxID:   snapshot.DependsOnOutboxID,
		DependsOnDeliveryID: snapshot.DependsOnDeliveryID,
		RecipientID:         snapshot.RecipientID,
		NormalizedEmail:     snapshot.NormalizedEmail,
		MembershipSnapshot:  snapshot.MembershipSnapshot,
		Content:             content,
		cancellationReason:  snapshot.CancellationReason,
		CreatedAt:           snapshot.CreatedAt,
		UpdatedAt:           snapshot.UpdatedAt,
		state:               snapshot.State,
	}
	if err := delivery.Validate(); err != nil {
		return Delivery{}, err
	}
	return delivery, nil
}

var deliveryTransitions = map[DeliveryState]map[DeliveryState]struct{}{
	DeliveryStatePending: {
		DeliveryStateQueued:    {},
		DeliveryStateCancelled: {},
	},
	DeliveryStateQueued: {
		DeliveryStateProcessing: {},
		DeliveryStateCancelled:  {},
	},
	DeliveryStateProcessing: {
		DeliveryStateRetryableFailed: {},
		DeliveryStateSucceeded:       {},
		DeliveryStatePermanentFailed: {},
		DeliveryStateCancelled:       {},
		DeliveryStateUnknown:         {},
	},
	DeliveryStateRetryableFailed: {
		DeliveryStateProcessing: {},
		DeliveryStateCancelled:  {},
	},
}

func (delivery *Delivery) ApplyTransition(next DeliveryState) (bool, error) {
	if err := delivery.state.Validate(); err != nil {
		return false, err
	}
	if err := next.Validate(); err != nil {
		return false, err
	}
	if delivery.state == next {
		return false, nil
	}
	if _, allowed := deliveryTransitions[delivery.state][next]; !allowed {
		return false, fmt.Errorf("delivery state transition %q -> %q is not allowed", delivery.state, next)
	}
	delivery.state = next
	if next == DeliveryStateCancelled && delivery.cancellationReason == "" {
		delivery.cancellationReason = CancellationReasonCancelled
	}
	return true, nil
}

func (delivery *Delivery) ApplyLateAcceptance() (bool, error) {
	if err := delivery.state.Validate(); err != nil {
		return false, err
	}
	switch delivery.state {
	case DeliveryStateUnknown:
		delivery.state = DeliveryStateSucceeded
		return true, nil
	case DeliveryStateSucceeded:
		return false, nil
	default:
		return false, fmt.Errorf("late acceptance cannot transition delivery from %q", delivery.state)
	}
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
	if err := delivery.state.Validate(); err != nil {
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
	if delivery.state == DeliveryStateCancelled {
		if err := delivery.cancellationReason.Validate(); err != nil {
			return err
		}
		if delivery.Content != (RenderedContent{}) {
			if err := delivery.Content.Validate(); err != nil {
				return err
			}
		}
	} else {
		if delivery.cancellationReason != "" {
			return fmt.Errorf("non-cancelled delivery must not have a cancellation reason")
		}
		if err := delivery.Content.Validate(); err != nil {
			return err
		}
	}
	if delivery.Content == (RenderedContent{}) {
		if delivery.state != DeliveryStateCancelled {
			return fmt.Errorf("non-cancelled delivery must have rendered content")
		}
		return nil
	}
	wantTemplateID := TemplateAlertOpeningV1
	if delivery.Kind == NotificationKindRecovery {
		wantTemplateID = TemplateAlertRecoveryV1
	}
	if delivery.Content.TemplateID() != wantTemplateID {
		return fmt.Errorf("template ID %q does not match notification kind %q", delivery.Content.TemplateID(), delivery.Kind)
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
