package notifications

import (
	"encoding/json"
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

	delivery, err := NewPendingDelivery(DeliveryParams{
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
		Content:             content,
		CreatedAt:           now,
		UpdatedAt:           now,
	})
	if err != nil {
		t.Fatalf("NewPendingDelivery() error = %v", err)
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
	params := validDeliveryParams(t, deliveryID)
	params.Kind = NotificationKindRecovery
	params.Content = mustRenderedContentFor(t, TemplateAlertRecoveryV1)
	params.DependsOnOutboxID = "outbox_opening_1"
	params.DependsOnDeliveryID = ""
	if _, err := NewPendingDelivery(params); err == nil {
		t.Fatal("recovery delivery without opening delivery accepted")
	}

	params.DependsOnDeliveryID = "del_opening_1"
	waiting, err := NewWaitingDependencyDelivery(params)
	if err != nil || waiting.State() != DeliveryStateWaitingDependency {
		t.Fatalf("waiting recovery = %#v, %v", waiting.Snapshot(), err)
	}
	openingID, err := NewDeliveryID("alert_1", NotificationKindOpening, ChannelEmail, "user_1")
	if err != nil {
		t.Fatal(err)
	}
	params.Kind = NotificationKindOpening
	params.DeliveryID = openingID
	params.Content = mustRenderedContentFor(t, TemplateAlertOpeningV1)
	params.DependsOnOutboxID = ""
	params.DependsOnDeliveryID = ""
	if _, err := NewWaitingDependencyDelivery(params); err == nil {
		t.Fatal("opening delivery accepted waiting dependency state")
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
	delivery := validDelivery(t, mustDeliveryID(t))
	delivery.TenantID = "tnt\x001"
	delivery.state = "finished"
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

func TestDeliveryConstructorsRestrictInitialState(t *testing.T) {
	deliveryID, err := NewDeliveryID("alert_1", NotificationKindOpening, ChannelEmail, "user_1")
	if err != nil {
		t.Fatalf("NewDeliveryID() error = %v", err)
	}
	params := validDeliveryParams(t, deliveryID)

	pending, err := NewPendingDelivery(params)
	if err != nil {
		t.Fatalf("NewPendingDelivery() error = %v", err)
	}
	if pending.State() != DeliveryStatePending {
		t.Fatalf("pending state = %q", pending.State())
	}

	cancelled, err := NewCancelledDelivery(params)
	if err != nil {
		t.Fatalf("NewCancelledDelivery() error = %v", err)
	}
	if cancelled.State() != DeliveryStateCancelled {
		t.Fatalf("cancelled state = %q", cancelled.State())
	}
}

func TestCancelledDeliveryCarriesReasonWithoutRenderedContent(t *testing.T) {
	deliveryID := mustDeliveryID(t)
	params := validDeliveryParams(t, deliveryID)
	params.Content = RenderedContent{}
	params.CancellationReason = CancellationReasonEmailSuppressed

	cancelled, err := NewCancelledDelivery(params)
	if err != nil {
		t.Fatalf("NewCancelledDelivery() error = %v", err)
	}
	if cancelled.State() != DeliveryStateCancelled ||
		cancelled.CancellationReason() != CancellationReasonEmailSuppressed {
		t.Fatalf("cancelled delivery = %#v", cancelled.Snapshot())
	}
	if snapshot := cancelled.Snapshot(); snapshot.Content != (RenderedContentSnapshot{}) {
		t.Fatalf("cancelled delivery rendered content = %#v", snapshot.Content)
	}
	if _, err := NewPendingDelivery(params); err == nil {
		t.Fatal("pending delivery accepted cancellation reason and missing content")
	}
	restored, err := RestoreDelivery(cancelled.Snapshot())
	if err != nil || restored.CancellationReason() != CancellationReasonEmailSuppressed {
		t.Fatalf("RestoreDelivery() = %#v, %v", restored.Snapshot(), err)
	}
}

func TestDeliveryStateTransitionMatrix(t *testing.T) {
	allowed := map[[2]DeliveryState]bool{
		{DeliveryStateWaitingDependency, DeliveryStatePending}:   true,
		{DeliveryStateWaitingDependency, DeliveryStateCancelled}: true,
		{DeliveryStatePending, DeliveryStateQueued}:              true,
		{DeliveryStatePending, DeliveryStateCancelled}:           true,
		{DeliveryStateQueued, DeliveryStateProcessing}:           true,
		{DeliveryStateQueued, DeliveryStateCancelled}:            true,
		{DeliveryStateProcessing, DeliveryStateRetryableFailed}:  true,
		{DeliveryStateProcessing, DeliveryStateSucceeded}:        true,
		{DeliveryStateProcessing, DeliveryStatePermanentFailed}:  true,
		{DeliveryStateProcessing, DeliveryStateCancelled}:        true,
		{DeliveryStateProcessing, DeliveryStateUnknown}:          true,
		{DeliveryStateRetryableFailed, DeliveryStateProcessing}:  true,
		{DeliveryStateRetryableFailed, DeliveryStateCancelled}:   true,
	}

	for _, current := range DeliveryStates() {
		for _, next := range DeliveryStates() {
			name := string(current) + "_to_" + string(next)
			t.Run(name, func(t *testing.T) {
				delivery := validDelivery(t, mustDeliveryID(t))
				delivery.state = current

				changed, err := delivery.ApplyTransition(next)
				if current == next {
					if err != nil || changed || delivery.State() != current {
						t.Fatalf("same-state result = changed:%v state:%q err:%v", changed, delivery.State(), err)
					}
					return
				}
				if allowed[[2]DeliveryState{current, next}] {
					if err != nil || !changed || delivery.State() != next {
						t.Fatalf("allowed result = changed:%v state:%q err:%v", changed, delivery.State(), err)
					}
					return
				}
				if err == nil || changed || delivery.State() != current {
					t.Fatalf("forbidden result = changed:%v state:%q err:%v", changed, delivery.State(), err)
				}
			})
		}
	}
}

func TestLateAcceptanceIsExplicitAndCannotResurrectCancelled(t *testing.T) {
	delivery := validDelivery(t, mustDeliveryID(t))
	delivery.state = DeliveryStateUnknown

	if changed, err := delivery.ApplyTransition(DeliveryStateSucceeded); err == nil || changed {
		t.Fatalf("general unknown -> succeeded = changed:%v err:%v", changed, err)
	}
	if delivery.State() != DeliveryStateUnknown {
		t.Fatalf("failed general transition changed state to %q", delivery.State())
	}
	if changed, err := delivery.ApplyLateAcceptance(); err != nil || !changed {
		t.Fatalf("late acceptance = changed:%v err:%v", changed, err)
	}
	if delivery.State() != DeliveryStateSucceeded {
		t.Fatalf("late acceptance state = %q", delivery.State())
	}
	if changed, err := delivery.ApplyLateAcceptance(); err != nil || changed {
		t.Fatalf("duplicate late acceptance = changed:%v err:%v", changed, err)
	}

	cancelled := validDelivery(t, mustDeliveryID(t))
	cancelled.state = DeliveryStateCancelled
	if changed, err := cancelled.ApplyLateAcceptance(); err == nil || changed {
		t.Fatalf("cancelled late acceptance = changed:%v err:%v", changed, err)
	}
	if cancelled.State() != DeliveryStateCancelled {
		t.Fatalf("cancelled delivery resurrected as %q", cancelled.State())
	}
}

func TestProviderFeedbackReconcilesDeliveryStateWithoutResurrectingCancellation(t *testing.T) {
	tests := []struct {
		name     string
		current  DeliveryState
		incoming ProviderOutcome
		want     DeliveryState
	}{
		{name: "fast send completes processing", current: DeliveryStateProcessing, incoming: ProviderOutcomeAccepted, want: DeliveryStateSucceeded},
		{name: "delay remains nonterminal", current: DeliveryStateRetryableFailed, incoming: ProviderOutcomeDelayed, want: DeliveryStateRetryableFailed},
		{name: "delay does not complete processing", current: DeliveryStateProcessing, incoming: ProviderOutcomeDelayed, want: DeliveryStateProcessing},
		{name: "late send promotes unknown", current: DeliveryStateUnknown, incoming: ProviderOutcomeAccepted, want: DeliveryStateSucceeded},
		{name: "reject is permanent", current: DeliveryStateProcessing, incoming: ProviderOutcomeRejected, want: DeliveryStatePermanentFailed},
		{name: "cancelled recovery stays cancelled", current: DeliveryStateCancelled, incoming: ProviderOutcomeAccepted, want: DeliveryStateCancelled},
		{name: "terminal failure is not resurrected", current: DeliveryStatePermanentFailed, incoming: ProviderOutcomeDeliveredToMailServer, want: DeliveryStatePermanentFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ReconcileDeliveryStateFromProvider(test.current, test.incoming)
			if err != nil {
				t.Fatalf("ReconcileDeliveryStateFromProvider() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("state = %q, want %q", got, test.want)
			}
		})
	}
}

func TestProviderFeedbackStateReconciliationRejectsImpossibleOrUnknownInputs(t *testing.T) {
	for _, test := range []struct {
		current  DeliveryState
		incoming ProviderOutcome
	}{
		{current: DeliveryStatePending, incoming: ProviderOutcomeAccepted},
		{current: "invented", incoming: ProviderOutcomeAccepted},
		{current: DeliveryStateProcessing, incoming: "invented"},
	} {
		if _, err := ReconcileDeliveryStateFromProvider(test.current, test.incoming); err == nil {
			t.Fatalf("reconciliation accepted state=%q outcome=%q", test.current, test.incoming)
		}
	}
}

func TestDeliverySnapshotMarshalsAndRestoresEncapsulatedState(t *testing.T) {
	delivery := validDelivery(t, mustDeliveryID(t))
	if changed, err := delivery.ApplyTransition(DeliveryStateQueued); err != nil || !changed {
		t.Fatalf("ApplyTransition() = changed:%v err:%v", changed, err)
	}

	snapshot := delivery.Snapshot()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode snapshot JSON: %v", err)
	}
	if fields["state"] != string(DeliveryStateQueued) {
		t.Fatalf("marshaled state = %#v, JSON = %s", fields["state"], encoded)
	}

	restored, err := RestoreDelivery(snapshot)
	if err != nil {
		t.Fatalf("RestoreDelivery() error = %v", err)
	}
	if restored.State() != DeliveryStateQueued {
		t.Fatalf("restored state = %q", restored.State())
	}

	snapshot.State = DeliveryStateUnknown
	if delivery.State() != DeliveryStateQueued {
		t.Fatalf("snapshot mutation changed entity state to %q", delivery.State())
	}
	snapshot.State = "invalid"
	if _, err := RestoreDelivery(snapshot); err == nil {
		t.Fatal("invalid persisted state restored")
	}
}

func validDelivery(t *testing.T, deliveryID string) Delivery {
	t.Helper()
	delivery, err := NewPendingDelivery(validDeliveryParams(t, deliveryID))
	if err != nil {
		t.Fatalf("NewPendingDelivery() error = %v", err)
	}
	return delivery
}

func validDeliveryParams(t *testing.T, deliveryID string) DeliveryParams {
	t.Helper()
	now := time.Date(2026, 7, 16, 12, 10, 0, 0, time.UTC)
	return DeliveryParams{
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
		Content:            mustRenderedContent(t),
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

func mustDeliveryID(t *testing.T) string {
	t.Helper()
	deliveryID, err := NewDeliveryID("alert_1", NotificationKindOpening, ChannelEmail, "user_1")
	if err != nil {
		t.Fatalf("NewDeliveryID() error = %v", err)
	}
	return deliveryID
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
