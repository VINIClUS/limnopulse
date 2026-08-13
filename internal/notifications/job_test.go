package notifications

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestDeliveryJobJSONContainsIdentifiersOnly(t *testing.T) {
	job, err := NewDeliveryJob(
		"tnt_1",
		"outbox_1",
		"del_1",
		"alert_1",
		NotificationKindOpening,
		ChannelEmail,
	)
	if err != nil {
		t.Fatalf("NewDeliveryJob() error = %v", err)
	}

	got, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	want := `{"schema_version":1,"message_type":"notification.delivery","tenant_id":"tnt_1","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"opening","channel":"email"}`
	if string(got) != want {
		t.Fatalf("job JSON = %s, want %s", got, want)
	}

	var body map[string]any
	if err := json.Unmarshal(got, &body); err != nil {
		t.Fatalf("decode job JSON: %v", err)
	}
	wantKeys := []string{"channel", "delivery_id", "event_id", "kind", "message_type", "outbox_id", "schema_version", "tenant_id"}
	gotKeys := make([]string, 0, len(body))
	for key := range body {
		gotKeys = append(gotKeys, key)
	}
	slicesSort(gotKeys)
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("job keys = %v, want %v", gotKeys, wantKeys)
	}

	for _, forbidden := range []string{"email", "email_address", "subject", "html", "text", "token", "recipient_id", "traceparent"} {
		if _, exists := body[forbidden]; exists {
			t.Fatalf("job JSON contains forbidden field %q: %s", forbidden, got)
		}
	}
	if TraceparentMessageAttribute != "traceparent" {
		t.Fatalf("traceparent attribute name = %q", TraceparentMessageAttribute)
	}
}

func TestTelegramDeliveryJobUsesSameCompactPIIFreeEnvelope(t *testing.T) {
	job, err := NewDeliveryJob(
		"tnt_1", "outbox_1", "del_1", "alert_1",
		NotificationKindOpening, ChannelTelegram,
	)
	if err != nil {
		t.Fatalf("NewDeliveryJob() error = %v", err)
	}
	body, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema_version":1,"message_type":"notification.delivery","tenant_id":"tnt_1","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"opening","channel":"telegram"}`
	if string(body) != want {
		t.Fatalf("Telegram job = %s, want %s", body, want)
	}
	for _, forbidden := range []string{"chat_id", "destination_id", "body_text", "token"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("Telegram job contains %q: %s", forbidden, body)
		}
	}
}

func TestDeliveryJobRejectsInvalidIdentityAndEnumInputs(t *testing.T) {
	tests := []struct {
		name       string
		tenantID   string
		outboxID   string
		deliveryID string
		eventID    string
		kind       NotificationKind
		channel    Channel
	}{
		{name: "empty tenant", outboxID: "outbox_1", deliveryID: "del_1", eventID: "alert_1", kind: NotificationKindOpening, channel: ChannelEmail},
		{name: "NUL tenant", tenantID: "tnt\x001", outboxID: "outbox_1", deliveryID: "del_1", eventID: "alert_1", kind: NotificationKindOpening, channel: ChannelEmail},
		{name: "empty outbox", tenantID: "tnt_1", deliveryID: "del_1", eventID: "alert_1", kind: NotificationKindOpening, channel: ChannelEmail},
		{name: "NUL delivery", tenantID: "tnt_1", outboxID: "outbox_1", deliveryID: "del\x001", eventID: "alert_1", kind: NotificationKindOpening, channel: ChannelEmail},
		{name: "empty event", tenantID: "tnt_1", outboxID: "outbox_1", deliveryID: "del_1", kind: NotificationKindOpening, channel: ChannelEmail},
		{name: "unknown kind", tenantID: "tnt_1", outboxID: "outbox_1", deliveryID: "del_1", eventID: "alert_1", kind: "other", channel: ChannelEmail},
		{name: "unknown channel", tenantID: "tnt_1", outboxID: "outbox_1", deliveryID: "del_1", eventID: "alert_1", kind: NotificationKindOpening, channel: "sms"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewDeliveryJob(tt.tenantID, tt.outboxID, tt.deliveryID, tt.eventID, tt.kind, tt.channel); err == nil {
				t.Fatal("NewDeliveryJob() succeeded")
			}
		})
	}
}

func TestDeliveryJobJSONDecodeValidatesEnvelope(t *testing.T) {
	invalidBodies := []string{
		`{"schema_version":2,"message_type":"notification.delivery","tenant_id":"tnt_1","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"opening","channel":"email"}`,
		`{"schema_version":1,"message_type":"other","tenant_id":"tnt_1","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"opening","channel":"email"}`,
		`{"schema_version":1,"message_type":"notification.delivery","tenant_id":"","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"opening","channel":"email"}`,
		`{"schema_version":1,"message_type":"notification.delivery","tenant_id":"tnt_1","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"other","channel":"email"}`,
	}
	for index, body := range invalidBodies {
		var job JobEnvelope
		if err := json.Unmarshal([]byte(body), &job); err == nil {
			t.Fatalf("body %d decoded: %#v", index, job)
		}
	}
}

func TestDeliveryJobJSONRejectsUnknownAndContentFields(t *testing.T) {
	for _, field := range []string{
		"unexpected",
		"email",
		"subject",
		"html",
		"text",
		"token",
		"traceparent",
	} {
		t.Run(field, func(t *testing.T) {
			body := validDeliveryJobJSONWithExtraField(field, "must-not-enter-body")
			original := JobEnvelope{TenantID: "unchanged"}
			job := original
			if err := json.Unmarshal([]byte(body), &job); err == nil {
				t.Fatalf("job with %q decoded: %#v", field, job)
			}
			if job != original {
				t.Fatalf("failed decode mutated receiver: got %#v, want %#v", job, original)
			}
		})
	}
}

func TestDeliveryJobJSONRejectsMultipleValuesAndTrailingJSON(t *testing.T) {
	valid := `{"schema_version":1,"message_type":"notification.delivery","tenant_id":"tnt_1","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"opening","channel":"email"}`
	for _, suffix := range []string{
		valid,
		` null`,
		` []`,
		` {"traceparent":"forbidden"}`,
		` trailing`,
	} {
		var job JobEnvelope
		if err := json.Unmarshal([]byte(valid+suffix), &job); err == nil {
			t.Fatalf("job with trailing %q decoded: %#v", suffix, job)
		}
	}

	var job JobEnvelope
	if err := json.Unmarshal([]byte(valid+" \n\t"), &job); err != nil {
		t.Fatalf("job with trailing whitespace rejected: %v", err)
	}
}

func validDeliveryJobJSONWithExtraField(field, value string) string {
	return `{"schema_version":1,"message_type":"notification.delivery","tenant_id":"tnt_1","outbox_id":"outbox_1","delivery_id":"del_1","event_id":"alert_1","kind":"opening","channel":"email","` +
		field + `":"` + value + `"}`
}

func slicesSort(values []string) {
	for index := 1; index < len(values); index++ {
		for cursor := index; cursor > 0 && values[cursor] < values[cursor-1]; cursor-- {
			values[cursor], values[cursor-1] = values[cursor-1], values[cursor]
		}
	}
}
