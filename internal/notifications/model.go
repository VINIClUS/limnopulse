package notifications

import (
	"encoding/json"
	"fmt"
)

type WorkKind string

const (
	WorkKindIntent     WorkKind = "INTENT"
	WorkKindDependency WorkKind = "DEPENDENCY"
	WorkKindDelivery   WorkKind = "DELIVERY"
)

func (kind WorkKind) Validate() error {
	switch kind {
	case WorkKindIntent, WorkKindDependency, WorkKindDelivery:
		return nil
	default:
		return fmt.Errorf("unknown work kind %q", kind)
	}
}

func (kind *WorkKind) UnmarshalText(text []byte) error {
	return assignEnum(kind, text, WorkKind.Validate)
}

func (kind *WorkKind) UnmarshalJSON(data []byte) error {
	return unmarshalEnumJSON(kind, data, WorkKind.Validate)
}

func WorkKinds() []WorkKind {
	return []WorkKind{WorkKindIntent, WorkKindDependency, WorkKindDelivery}
}

type NotificationKind string

const (
	NotificationKindOpening  NotificationKind = "opening"
	NotificationKindRecovery NotificationKind = "recovery"
)

func (kind NotificationKind) Validate() error {
	switch kind {
	case NotificationKindOpening, NotificationKindRecovery:
		return nil
	default:
		return fmt.Errorf("unknown notification kind %q", kind)
	}
}

func (kind *NotificationKind) UnmarshalText(text []byte) error {
	return assignEnum(kind, text, NotificationKind.Validate)
}

func (kind *NotificationKind) UnmarshalJSON(data []byte) error {
	return unmarshalEnumJSON(kind, data, NotificationKind.Validate)
}

func NotificationKinds() []NotificationKind {
	return []NotificationKind{NotificationKindOpening, NotificationKindRecovery}
}

type Channel string

const ChannelEmail Channel = "email"

func (channel Channel) Validate() error {
	if channel != ChannelEmail {
		return fmt.Errorf("unknown channel %q", channel)
	}
	return nil
}

func (channel *Channel) UnmarshalText(text []byte) error {
	return assignEnum(channel, text, Channel.Validate)
}

func (channel *Channel) UnmarshalJSON(data []byte) error {
	return unmarshalEnumJSON(channel, data, Channel.Validate)
}

func Channels() []Channel {
	return []Channel{ChannelEmail}
}

type DeliveryState string

const (
	DeliveryStatePending         DeliveryState = "pending"
	DeliveryStateQueued          DeliveryState = "queued"
	DeliveryStateProcessing      DeliveryState = "processing"
	DeliveryStateRetryableFailed DeliveryState = "retryable_failed"
	DeliveryStateSucceeded       DeliveryState = "succeeded"
	DeliveryStatePermanentFailed DeliveryState = "permanent_failed"
	DeliveryStateCancelled       DeliveryState = "cancelled"
	DeliveryStateUnknown         DeliveryState = "unknown"
)

func (state DeliveryState) Validate() error {
	switch state {
	case DeliveryStatePending,
		DeliveryStateQueued,
		DeliveryStateProcessing,
		DeliveryStateRetryableFailed,
		DeliveryStateSucceeded,
		DeliveryStatePermanentFailed,
		DeliveryStateCancelled,
		DeliveryStateUnknown:
		return nil
	default:
		return fmt.Errorf("unknown delivery state %q", state)
	}
}

func (state *DeliveryState) UnmarshalText(text []byte) error {
	return assignEnum(state, text, DeliveryState.Validate)
}

func (state *DeliveryState) UnmarshalJSON(data []byte) error {
	return unmarshalEnumJSON(state, data, DeliveryState.Validate)
}

func DeliveryStates() []DeliveryState {
	return []DeliveryState{
		DeliveryStatePending,
		DeliveryStateQueued,
		DeliveryStateProcessing,
		DeliveryStateRetryableFailed,
		DeliveryStateSucceeded,
		DeliveryStatePermanentFailed,
		DeliveryStateCancelled,
		DeliveryStateUnknown,
	}
}

type AttemptOutcome string

const (
	AttemptOutcomeStarted         AttemptOutcome = "started"
	AttemptOutcomeSucceeded       AttemptOutcome = "succeeded"
	AttemptOutcomeRetryable       AttemptOutcome = "retryable"
	AttemptOutcomeAmbiguous       AttemptOutcome = "ambiguous"
	AttemptOutcomePermanentFailed AttemptOutcome = "permanent_failed"
)

func (outcome AttemptOutcome) Validate() error {
	switch outcome {
	case AttemptOutcomeStarted,
		AttemptOutcomeSucceeded,
		AttemptOutcomeRetryable,
		AttemptOutcomeAmbiguous,
		AttemptOutcomePermanentFailed:
		return nil
	default:
		return fmt.Errorf("unknown attempt outcome %q", outcome)
	}
}

func (outcome *AttemptOutcome) UnmarshalText(text []byte) error {
	return assignEnum(outcome, text, AttemptOutcome.Validate)
}

func (outcome *AttemptOutcome) UnmarshalJSON(data []byte) error {
	return unmarshalEnumJSON(outcome, data, AttemptOutcome.Validate)
}

func AttemptOutcomes() []AttemptOutcome {
	return []AttemptOutcome{
		AttemptOutcomeStarted,
		AttemptOutcomeSucceeded,
		AttemptOutcomeRetryable,
		AttemptOutcomeAmbiguous,
		AttemptOutcomePermanentFailed,
	}
}

type ProviderOutcome string

const (
	ProviderOutcomeAccepted              ProviderOutcome = "accepted"
	ProviderOutcomeDeliveredToMailServer ProviderOutcome = "delivered_to_mail_server"
	ProviderOutcomeDelayed               ProviderOutcome = "delayed"
	ProviderOutcomeSoftBounced           ProviderOutcome = "soft_bounced"
	ProviderOutcomeHardBounced           ProviderOutcome = "hard_bounced"
	ProviderOutcomeComplained            ProviderOutcome = "complained"
	ProviderOutcomeRejected              ProviderOutcome = "rejected"
)

func (outcome ProviderOutcome) Validate() error {
	switch outcome {
	case ProviderOutcomeAccepted,
		ProviderOutcomeDeliveredToMailServer,
		ProviderOutcomeDelayed,
		ProviderOutcomeSoftBounced,
		ProviderOutcomeHardBounced,
		ProviderOutcomeComplained,
		ProviderOutcomeRejected:
		return nil
	default:
		return fmt.Errorf("unknown provider outcome %q", outcome)
	}
}

func (outcome *ProviderOutcome) UnmarshalText(text []byte) error {
	return assignEnum(outcome, text, ProviderOutcome.Validate)
}

func (outcome *ProviderOutcome) UnmarshalJSON(data []byte) error {
	return unmarshalEnumJSON(outcome, data, ProviderOutcome.Validate)
}

func ProviderOutcomes() []ProviderOutcome {
	return []ProviderOutcome{
		ProviderOutcomeAccepted,
		ProviderOutcomeDeliveredToMailServer,
		ProviderOutcomeDelayed,
		ProviderOutcomeSoftBounced,
		ProviderOutcomeHardBounced,
		ProviderOutcomeComplained,
		ProviderOutcomeRejected,
	}
}

var providerOutcomeRank = map[ProviderOutcome]int{
	ProviderOutcomeAccepted:              1,
	ProviderOutcomeDelayed:               2,
	ProviderOutcomeSoftBounced:           3,
	ProviderOutcomeDeliveredToMailServer: 4,
	ProviderOutcomeRejected:              5,
	ProviderOutcomeHardBounced:           6,
	ProviderOutcomeComplained:            7,
}

func ReconcileProviderOutcome(current, incoming ProviderOutcome) (ProviderOutcome, error) {
	if current != "" {
		if err := current.Validate(); err != nil {
			return "", err
		}
	}
	if err := incoming.Validate(); err != nil {
		return "", err
	}
	if current == "" || providerOutcomeRank[incoming] > providerOutcomeRank[current] {
		return incoming, nil
	}
	return current, nil
}

func assignEnum[T ~string](target *T, text []byte, validate func(T) error) error {
	candidate := T(text)
	if err := validate(candidate); err != nil {
		return err
	}
	*target = candidate
	return nil
}

func unmarshalEnumJSON[T ~string](target *T, data []byte, validate func(T) error) error {
	var decoded *string
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded == nil {
		return fmt.Errorf("enum value must be a non-null string")
	}
	return assignEnum(target, []byte(*decoded), validate)
}
