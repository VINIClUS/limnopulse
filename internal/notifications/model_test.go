package notifications

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type validatable interface {
	Validate() error
}

func TestNotificationEnumsExposeOnlyContractValues(t *testing.T) {
	tests := []struct {
		name string
		got  []string
		want []string
	}{
		{
			name: "work kinds",
			got:  stringsOf(WorkKinds()),
			want: []string{"INTENT", "DEPENDENCY", "DELIVERY"},
		},
		{
			name: "notification kinds",
			got:  stringsOf(NotificationKinds()),
			want: []string{"opening", "recovery"},
		},
		{
			name: "channels",
			got:  stringsOf(Channels()),
			want: []string{"email", "telegram"},
		},
		{
			name: "email deliverability",
			got:  stringsOf(EmailDeliverabilities()),
			want: []string{"unknown", "deliverable", "suppressed"},
		},
		{
			name: "delivery states",
			got:  stringsOf(DeliveryStates()),
			want: []string{"pending", "waiting_dependency", "queued", "processing", "retryable_failed", "succeeded", "permanent_failed", "cancelled", "unknown"},
		},
		{
			name: "attempt outcomes",
			got:  stringsOf(AttemptOutcomes()),
			want: []string{"started", "succeeded", "retryable", "ambiguous", "permanent_failed"},
		},
		{
			name: "provider outcomes",
			got:  stringsOf(ProviderOutcomes()),
			want: []string{"accepted", "delivered_to_mail_server", "delayed", "soft_bounced", "hard_bounced", "complained", "rejected"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Fatalf("values = %v, want %v", tt.got, tt.want)
			}
		})
	}
}

func TestNotificationEnumsRejectUnknownAndNULValues(t *testing.T) {
	tests := []struct {
		name  string
		value validatable
	}{
		{name: "work kind", value: WorkKind("OTHER")},
		{name: "notification kind", value: NotificationKind("opening\x00")},
		{name: "channel", value: Channel("sms")},
		{name: "email deliverability", value: EmailDeliverability("blocked")},
		{name: "delivery state", value: DeliveryState("finished")},
		{name: "attempt outcome", value: AttemptOutcome("failed")},
		{name: "provider outcome", value: ProviderOutcome("bounce")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.value.Validate(); err == nil {
				t.Fatal("Validate() succeeded")
			}
		})
	}
}

func TestNotificationEnumsRejectUnknownJSON(t *testing.T) {
	tests := []struct {
		name   string
		target any
	}{
		{name: "work kind", target: new(WorkKind)},
		{name: "notification kind", target: new(NotificationKind)},
		{name: "channel", target: new(Channel)},
		{name: "email deliverability", target: new(EmailDeliverability)},
		{name: "delivery state", target: new(DeliveryState)},
		{name: "attempt outcome", target: new(AttemptOutcome)},
		{name: "provider outcome", target: new(ProviderOutcome)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, input := range []string{`"not-a-contract-value"`, `null`, `123`} {
				if err := json.Unmarshal([]byte(input), tt.target); err == nil {
					t.Fatalf("json.Unmarshal(%s) succeeded", input)
				}
			}
		})
	}
}

func TestEmailDeliverabilityMatchesSharedPythonContract(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "email_deliverability_values.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if got := stringsOf(EmailDeliverabilities()); !reflect.DeepEqual(got, fixture.Values) {
		t.Fatalf("Go values = %v, shared values = %v", got, fixture.Values)
	}
}

func TestProviderOutcomeReconciliationIsMonotonic(t *testing.T) {
	tests := []struct {
		name     string
		current  ProviderOutcome
		incoming ProviderOutcome
		want     ProviderOutcome
	}{
		{name: "first outcome", incoming: ProviderOutcomeAccepted, want: ProviderOutcomeAccepted},
		{name: "forward progress", current: ProviderOutcomeDelayed, incoming: ProviderOutcomeDeliveredToMailServer, want: ProviderOutcomeDeliveredToMailServer},
		{name: "no downgrade", current: ProviderOutcomeHardBounced, incoming: ProviderOutcomeDeliveredToMailServer, want: ProviderOutcomeHardBounced},
		{name: "complaint outranks hard bounce", current: ProviderOutcomeHardBounced, incoming: ProviderOutcomeComplained, want: ProviderOutcomeComplained},
		{name: "hard bounce cannot overwrite complaint", current: ProviderOutcomeComplained, incoming: ProviderOutcomeHardBounced, want: ProviderOutcomeComplained},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReconcileProviderOutcome(tt.current, tt.incoming)
			if err != nil {
				t.Fatalf("ReconcileProviderOutcome() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ReconcileProviderOutcome() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProviderOutcomeReconciliationRejectsUnknownValues(t *testing.T) {
	if _, err := ReconcileProviderOutcome("invalid", ProviderOutcomeAccepted); err == nil {
		t.Fatal("invalid current outcome accepted")
	}
	if _, err := ReconcileProviderOutcome(ProviderOutcomeAccepted, "invalid"); err == nil {
		t.Fatal("invalid incoming outcome accepted")
	}
}

func stringsOf[T ~string](values []T) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	return result
}
