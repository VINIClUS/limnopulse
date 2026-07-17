package feedback

import (
	"fmt"
	"strings"
	"testing"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

func TestParseEventBridgeSESEventsMapsOnlyContractFeedback(t *testing.T) {
	tests := []struct {
		name               string
		eventType          string
		detail             string
		wantSemantic       SemanticType
		wantOutcome        notifications.ProviderOutcome
		wantSuppression    SuppressionReason
		wantPermanentState bool
	}{
		{name: "send", eventType: "Send", wantSemantic: SemanticSend, wantOutcome: notifications.ProviderOutcomeAccepted},
		{name: "delivery", eventType: "Delivery", wantSemantic: SemanticDelivery, wantOutcome: notifications.ProviderOutcomeDeliveredToMailServer},
		{name: "delay", eventType: "DeliveryDelay", detail: `,"deliveryDelay":{"delayType":"MailboxFull"}`, wantSemantic: SemanticDeliveryDelay, wantOutcome: notifications.ProviderOutcomeDelayed},
		{name: "soft bounce", eventType: "Bounce", detail: `,"bounce":{"bounceType":"Transient","bouncedRecipients":[{"emailAddress":"never-trusted@example.com"}]}`, wantSemantic: SemanticSoftBounce, wantOutcome: notifications.ProviderOutcomeSoftBounced},
		{name: "hard bounce", eventType: "Bounce", detail: `,"bounce":{"bounceType":"Permanent"}`, wantSemantic: SemanticHardBounce, wantOutcome: notifications.ProviderOutcomeHardBounced, wantSuppression: SuppressionHardBounce},
		{name: "complaint", eventType: "Complaint", detail: `,"complaint":{"complaintFeedbackType":"abuse"}`, wantSemantic: SemanticComplaint, wantOutcome: notifications.ProviderOutcomeComplained, wantSuppression: SuppressionComplaint},
		{name: "content reject", eventType: "Reject", detail: `,"reject":{"reason":"Bad content"}`, wantSemantic: SemanticReject, wantOutcome: notifications.ProviderOutcomeRejected, wantPermanentState: true},
		{name: "recipient reject", eventType: "Reject", detail: `,"reject":{"reason":"Invalid recipient address"}`, wantSemantic: SemanticReject, wantOutcome: notifications.ProviderOutcomeRejected, wantSuppression: SuppressionRecipientReject, wantPermanentState: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ParseEvent([]byte(eventJSON(test.eventType, test.detail)))
			if err != nil {
				t.Fatalf("ParseEvent() error = %v", err)
			}
			if result.Disposition != ParseProcess || result.Event.EventBridgeID != "evt_1" ||
				result.Event.ProviderMessageID != "ses_message_1" || result.Event.DeliveryID != "del_1" ||
				result.Event.AttemptID != "att_1" || result.Event.SemanticType != test.wantSemantic ||
				result.Event.ProviderOutcome != test.wantOutcome || result.Event.SuppressionReason != test.wantSuppression ||
				result.Event.PermanentFailure != test.wantPermanentState {
				t.Fatalf("parsed = %#v", result)
			}
			if strings.Contains(fmt.Sprintf("%#v", result.Event), "never-trusted@example.com") {
				t.Fatal("event retained an untrusted SES destination")
			}
		})
	}
}

func TestParseEventBridgeSESOpenAndClickAsIgnoredNoOps(t *testing.T) {
	for _, eventType := range []string{"Open", "Click"} {
		result, err := ParseEvent([]byte(eventJSON(eventType, `,"open":{"ipAddress":"private"}`)))
		if err != nil || result.Disposition != ParseIgnore || result.Event.EventBridgeID != "evt_1" {
			t.Fatalf("%s result=%#v err=%v", eventType, result, err)
		}
	}
}

func TestParseEventBridgeSESRejectsMalformedIdentityAndUnknownEvents(t *testing.T) {
	valid := eventJSON("Send", "")
	tests := []string{
		"{}",
		strings.Replace(valid, `"id":"evt_1"`, `"id":""`, 1),
		strings.Replace(valid, `"source":"aws.ses"`, `"source":"other"`, 1),
		strings.Replace(valid, `"messageId":"ses_message_1"`, `"messageId":""`, 1),
		strings.Replace(valid, `"delivery_id":["del_1"]`, `"delivery_id":[]`, 1),
		strings.Replace(valid, `"attempt_id":["att_1"]`, `"attempt_id":["att_1","att_2"]`, 1),
		strings.Replace(valid, `"eventType":"Send"`, `"eventType":"RenderingFailure"`, 1),
		eventJSON("Bounce", `,"bounce":{"bounceType":"Undetermined"}`),
		valid + `{}`,
	}
	for index, input := range tests {
		if result, err := ParseEvent([]byte(input)); err == nil || result.Disposition != ParseAwaitDLQ {
			t.Fatalf("case %d result=%#v err=%v", index, result, err)
		}
	}
}

func eventJSON(eventType, detail string) string {
	return `{"version":"0","id":"evt_1","detail-type":"Email ` + eventType +
		`","source":"aws.ses","account":"123","time":"2026-07-17T12:00:00Z","region":"sa-east-1","resources":[],"detail":{"eventType":"` + eventType +
		`","mail":{"messageId":"ses_message_1","destination":["never-trusted@example.com"],"tags":{"delivery_id":["del_1"],"attempt_id":["att_1"]}}` + detail + `}}`
}
