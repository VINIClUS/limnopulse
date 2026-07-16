package notifications

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDurableModelsCarryRequiredProvenanceAndNoTTL(t *testing.T) {
	content := mustRenderedContentFor(t, TemplateAlertRecoveryV1)
	deliveryID, err := NewDeliveryID("alert_1", NotificationKindRecovery, ChannelEmail, "user_1")
	if err != nil {
		t.Fatalf("NewDeliveryID() error = %v", err)
	}
	now := time.Date(2026, 7, 16, 12, 10, 0, 0, time.UTC)

	outbox := Outbox{
		TenantID:          "tnt_1",
		OutboxID:          "outbox_recovery_1",
		EventID:           "alert_1",
		RuleID:            "rule_1",
		Kind:              NotificationKindRecovery,
		Channel:           ChannelEmail,
		DependsOnOutboxID: "outbox_opening_1",
		CreatedAt:         now,
	}
	if err := outbox.Validate(); err != nil {
		t.Fatalf("Outbox.Validate() error = %v", err)
	}

	delivery := Delivery{
		TenantID:            "tnt_1",
		OutboxID:            outbox.OutboxID,
		DeliveryID:          deliveryID,
		EventID:             "alert_1",
		RuleID:              "rule_1",
		Kind:                NotificationKindRecovery,
		Channel:             ChannelEmail,
		DependsOnOutboxID:   "outbox_opening_1",
		DependsOnDeliveryID: "del_opening_1",
		RecipientID:         "user_1",
		NormalizedEmail:     "owner@example.com",
		MembershipSnapshot:  MembershipSnapshot{Role: "owner", Status: "active", Version: 7},
		State:               DeliveryStatePending,
		Content:             content,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if err := delivery.Validate(); err != nil {
		t.Fatalf("Delivery.Validate() error = %v", err)
	}

	attempt := Attempt{
		DeliveryID:      deliveryID,
		AttemptID:       "attempt_1",
		Outcome:         AttemptOutcomeSucceeded,
		ProviderOutcome: ProviderOutcomeAccepted,
		StartedAt:       now,
		CompletedAt:     now.Add(time.Second),
	}
	if err := attempt.Validate(); err != nil {
		t.Fatalf("Attempt.Validate() error = %v", err)
	}

	for _, model := range []any{outbox, delivery, attempt} {
		typeOf := reflect.TypeOf(model)
		for index := 0; index < typeOf.NumField(); index++ {
			name := strings.ToLower(typeOf.Field(index).Name)
			if strings.Contains(name, "ttl") || strings.Contains(name, "expires") {
				t.Fatalf("durable %s contains TTL field %q", typeOf.Name(), typeOf.Field(index).Name)
			}
		}
	}
}

func TestRecoveryModelsRequireOpeningRelationship(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 10, 0, 0, time.UTC)
	outbox := Outbox{
		TenantID:  "tnt_1",
		OutboxID:  "outbox_1",
		EventID:   "alert_1",
		RuleID:    "rule_1",
		Kind:      NotificationKindRecovery,
		Channel:   ChannelEmail,
		CreatedAt: now,
	}
	if err := outbox.Validate(); err == nil {
		t.Fatal("recovery outbox without opening outbox accepted")
	}

	deliveryID, err := NewDeliveryID("alert_1", NotificationKindRecovery, ChannelEmail, "user_1")
	if err != nil {
		t.Fatalf("NewDeliveryID() error = %v", err)
	}
	delivery := validDelivery(t, deliveryID)
	delivery.Kind = NotificationKindRecovery
	delivery.DependsOnOutboxID = "outbox_opening_1"
	delivery.DependsOnDeliveryID = ""
	if err := delivery.Validate(); err == nil {
		t.Fatal("recovery delivery without opening delivery accepted")
	}
}

func TestDeliveryValidationUsesOnlyCanonicalDeliveryIdentityInputs(t *testing.T) {
	deliveryID, err := NewDeliveryID("alert_1", NotificationKindOpening, ChannelEmail, "user_1")
	if err != nil {
		t.Fatalf("NewDeliveryID() error = %v", err)
	}
	delivery := validDelivery(t, deliveryID)
	if err := delivery.Validate(); err != nil {
		t.Fatalf("initial Delivery.Validate() error = %v", err)
	}

	delivery.TenantID = "tnt_2"
	delivery.OutboxID = "outbox_changed"
	delivery.NormalizedEmail = "different@example.com"
	if err := delivery.Validate(); err != nil {
		t.Fatalf("non-identity provenance changed delivery identity: %v", err)
	}

	delivery.EventID = "alert_changed"
	if err := delivery.Validate(); err == nil {
		t.Fatal("changed event ID did not invalidate delivery identity")
	}
}

func TestDurableModelsRejectUnknownEnumsAndNULIdentityInputs(t *testing.T) {
	delivery := validDelivery(t, "del_invalid")
	delivery.TenantID = "tnt\x001"
	delivery.State = "finished"
	if err := delivery.Validate(); err == nil {
		t.Fatal("invalid delivery accepted")
	}

	attempt := Attempt{
		DeliveryID: "del_1",
		AttemptID:  "attempt\x001",
		Outcome:    "failed",
		StartedAt:  time.Now(),
	}
	if err := attempt.Validate(); err == nil {
		t.Fatal("invalid attempt accepted")
	}
}

func TestDeliveryRequiresTemplateMatchingNotificationKind(t *testing.T) {
	deliveryID, err := NewDeliveryID("alert_1", NotificationKindOpening, ChannelEmail, "user_1")
	if err != nil {
		t.Fatalf("NewDeliveryID() error = %v", err)
	}
	delivery := validDelivery(t, deliveryID)
	delivery.Content = mustRenderedContentFor(t, TemplateAlertRecoveryV1)
	if err := delivery.Validate(); err == nil {
		t.Fatal("opening delivery accepted recovery template")
	}
}

func validDelivery(t *testing.T, deliveryID string) Delivery {
	t.Helper()
	now := time.Date(2026, 7, 16, 12, 10, 0, 0, time.UTC)
	return Delivery{
		TenantID:           "tnt_1",
		OutboxID:           "outbox_1",
		DeliveryID:         deliveryID,
		EventID:            "alert_1",
		RuleID:             "rule_1",
		Kind:               NotificationKindOpening,
		Channel:            ChannelEmail,
		RecipientID:        "user_1",
		NormalizedEmail:    "owner@example.com",
		MembershipSnapshot: MembershipSnapshot{Role: "owner", Status: "active", Version: 7},
		State:              DeliveryStatePending,
		Content:            mustRenderedContent(t),
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

func mustRenderedContent(t *testing.T) RenderedContent {
	return mustRenderedContentFor(t, TemplateAlertOpeningV1)
}

func mustRenderedContentFor(t *testing.T, templateID TemplateID) RenderedContent {
	t.Helper()
	content, err := mustTemplateRenderer(t).Render(
		templateID,
		LocalePTBR,
		validEmailTemplateData(),
	)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	return content
}
