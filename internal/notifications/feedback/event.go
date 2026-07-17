package feedback

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

type SemanticType string

const (
	SemanticSend          SemanticType = "send"
	SemanticDelivery      SemanticType = "delivery"
	SemanticDeliveryDelay SemanticType = "delivery_delay"
	SemanticSoftBounce    SemanticType = "bounce_soft"
	SemanticHardBounce    SemanticType = "bounce_hard"
	SemanticComplaint     SemanticType = "complaint"
	SemanticReject        SemanticType = "reject"
)

func (eventType SemanticType) Validate() error {
	switch eventType {
	case SemanticSend, SemanticDelivery, SemanticDeliveryDelay, SemanticSoftBounce,
		SemanticHardBounce, SemanticComplaint, SemanticReject:
		return nil
	default:
		return fmt.Errorf("unknown feedback semantic type")
	}
}

type SuppressionReason string

const (
	SuppressionHardBounce      SuppressionReason = "hard_bounce"
	SuppressionComplaint       SuppressionReason = "complaint"
	SuppressionRecipientReject SuppressionReason = "recipient_reject"
)

func (reason SuppressionReason) Validate() error {
	switch reason {
	case SuppressionHardBounce, SuppressionComplaint, SuppressionRecipientReject:
		return nil
	default:
		return fmt.Errorf("unknown feedback suppression reason")
	}
}

func (reason SuppressionReason) Rank() int {
	switch reason {
	case SuppressionRecipientReject:
		return 1
	case SuppressionHardBounce:
		return 2
	case SuppressionComplaint:
		return 3
	default:
		return 0
	}
}

type Event struct {
	EventBridgeID     string
	ProviderMessageID string
	DeliveryID        string
	AttemptID         string
	SemanticType      SemanticType
	ProviderOutcome   notifications.ProviderOutcome
	SuppressionReason SuppressionReason
	PermanentFailure  bool
	AcceptedEvidence  bool
}

func (event Event) Validate() error {
	for _, field := range []string{event.EventBridgeID, event.ProviderMessageID, event.DeliveryID, event.AttemptID} {
		if field == "" || strings.ContainsRune(field, '\x00') {
			return fmt.Errorf("feedback identity is invalid")
		}
	}
	if err := event.SemanticType.Validate(); err != nil {
		return err
	}
	if err := event.ProviderOutcome.Validate(); err != nil {
		return err
	}
	if event.SuppressionReason != "" {
		if err := event.SuppressionReason.Validate(); err != nil {
			return err
		}
	}
	return event.validateSemantics()
}

func (event Event) validateSemantics() error {
	wantOutcome := notifications.ProviderOutcome("")
	wantSuppression := SuppressionReason("")
	wantAccepted := true
	wantPermanent := false
	switch event.SemanticType {
	case SemanticSend:
		wantOutcome = notifications.ProviderOutcomeAccepted
	case SemanticDelivery:
		wantOutcome = notifications.ProviderOutcomeDeliveredToMailServer
	case SemanticDeliveryDelay:
		wantOutcome = notifications.ProviderOutcomeDelayed
	case SemanticSoftBounce:
		wantOutcome = notifications.ProviderOutcomeSoftBounced
	case SemanticHardBounce:
		wantOutcome = notifications.ProviderOutcomeHardBounced
		wantSuppression = SuppressionHardBounce
	case SemanticComplaint:
		wantOutcome = notifications.ProviderOutcomeComplained
		wantSuppression = SuppressionComplaint
	case SemanticReject:
		wantOutcome = notifications.ProviderOutcomeRejected
		wantAccepted = false
		wantPermanent = true
		if event.SuppressionReason != "" && event.SuppressionReason != SuppressionRecipientReject {
			return fmt.Errorf("feedback rejection suppression is invalid")
		}
	default:
		return fmt.Errorf("unknown feedback semantic type")
	}
	if event.ProviderOutcome != wantOutcome || event.AcceptedEvidence != wantAccepted ||
		event.PermanentFailure != wantPermanent {
		return fmt.Errorf("feedback semantic flags are inconsistent")
	}
	if event.SemanticType != SemanticReject && event.SuppressionReason != wantSuppression {
		return fmt.Errorf("feedback suppression is inconsistent")
	}
	return nil
}

type ParseDisposition uint8

const (
	ParseProcess ParseDisposition = iota + 1
	ParseIgnore
	ParseAwaitDLQ
)

type ParseResult struct {
	Disposition ParseDisposition
	Event       Event
}

type eventBridgeEnvelope struct {
	Version    string    `json:"version"`
	ID         string    `json:"id"`
	DetailType string    `json:"detail-type"`
	Source     string    `json:"source"`
	Detail     sesDetail `json:"detail"`
}

type sesDetail struct {
	EventType     string         `json:"eventType"`
	Mail          sesMail        `json:"mail"`
	Bounce        *sesBounce     `json:"bounce"`
	Reject        *sesReject     `json:"reject"`
	DeliveryDelay map[string]any `json:"deliveryDelay"`
	Complaint     map[string]any `json:"complaint"`
}

type sesMail struct {
	MessageID string              `json:"messageId"`
	Tags      map[string][]string `json:"tags"`
}

type sesBounce struct {
	BounceType string `json:"bounceType"`
}

type sesReject struct {
	Reason string `json:"reason"`
}

func ParseEvent(body []byte) (ParseResult, error) {
	fail := func() (ParseResult, error) {
		return ParseResult{Disposition: ParseAwaitDLQ}, fmt.Errorf("SES feedback envelope is malformed")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var envelope eventBridgeEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return fail()
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fail()
	}
	if envelope.Version != "0" || envelope.Source != "aws.ses" ||
		envelope.ID == "" || strings.ContainsRune(envelope.ID, '\x00') ||
		envelope.DetailType == "" || strings.ContainsRune(envelope.DetailType, '\x00') ||
		envelope.Detail.EventType == "" {
		return fail()
	}
	if envelope.Detail.EventType == "Open" || envelope.Detail.EventType == "Click" {
		return ParseResult{Disposition: ParseIgnore, Event: Event{EventBridgeID: envelope.ID}}, nil
	}
	deliveryID, ok := oneTag(envelope.Detail.Mail.Tags, "delivery_id")
	if !ok {
		return fail()
	}
	attemptID, ok := oneTag(envelope.Detail.Mail.Tags, "attempt_id")
	if !ok || envelope.Detail.Mail.MessageID == "" || strings.ContainsRune(envelope.Detail.Mail.MessageID, '\x00') {
		return fail()
	}
	event := Event{
		EventBridgeID: envelope.ID, ProviderMessageID: envelope.Detail.Mail.MessageID,
		DeliveryID: deliveryID, AttemptID: attemptID,
	}
	switch envelope.Detail.EventType {
	case "Send":
		event.SemanticType = SemanticSend
		event.ProviderOutcome = notifications.ProviderOutcomeAccepted
		event.AcceptedEvidence = true
	case "Delivery":
		event.SemanticType = SemanticDelivery
		event.ProviderOutcome = notifications.ProviderOutcomeDeliveredToMailServer
		event.AcceptedEvidence = true
	case "DeliveryDelay":
		if envelope.Detail.DeliveryDelay == nil {
			return fail()
		}
		event.SemanticType = SemanticDeliveryDelay
		event.ProviderOutcome = notifications.ProviderOutcomeDelayed
		event.AcceptedEvidence = true
	case "Bounce":
		if envelope.Detail.Bounce == nil {
			return fail()
		}
		switch envelope.Detail.Bounce.BounceType {
		case "Transient":
			event.SemanticType = SemanticSoftBounce
			event.ProviderOutcome = notifications.ProviderOutcomeSoftBounced
			event.AcceptedEvidence = true
		case "Permanent":
			event.SemanticType = SemanticHardBounce
			event.ProviderOutcome = notifications.ProviderOutcomeHardBounced
			event.SuppressionReason = SuppressionHardBounce
			event.AcceptedEvidence = true
		default:
			return fail()
		}
	case "Complaint":
		if envelope.Detail.Complaint == nil {
			return fail()
		}
		event.SemanticType = SemanticComplaint
		event.ProviderOutcome = notifications.ProviderOutcomeComplained
		event.SuppressionReason = SuppressionComplaint
		event.AcceptedEvidence = true
	case "Reject":
		if envelope.Detail.Reject == nil || strings.TrimSpace(envelope.Detail.Reject.Reason) == "" {
			return fail()
		}
		event.SemanticType = SemanticReject
		event.ProviderOutcome = notifications.ProviderOutcomeRejected
		event.PermanentFailure = true
		if recipientSpecificReject(envelope.Detail.Reject.Reason) {
			event.SuppressionReason = SuppressionRecipientReject
		}
	default:
		return fail()
	}
	if err := event.Validate(); err != nil {
		return fail()
	}
	return ParseResult{Disposition: ParseProcess, Event: event}, nil
}

func oneTag(tags map[string][]string, name string) (string, bool) {
	values := tags[name]
	if len(values) != 1 || values[0] == "" || strings.ContainsRune(values[0], '\x00') {
		return "", false
	}
	return values[0], true
}

func recipientSpecificReject(reason string) bool {
	normalized := strings.ToLower(strings.TrimSpace(reason))
	_, allowed := map[string]struct{}{
		"bad recipient":              {},
		"invalid recipient":          {},
		"invalid recipient address":  {},
		"mailbox does not exist":     {},
		"recipient address rejected": {},
		"unknown recipient":          {},
	}[normalized]
	return allowed
}
