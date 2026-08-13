package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

var ErrConcurrentTerminal = errors.New("delivery became terminal during worker completion")

type AcquireDisposition uint8

const (
	AcquireClaimed AcquireDisposition = iota + 1
	AcquireTerminal
	AcquireDeferred
	AcquireAwaitDLQ
)

type DeliveryRecord struct {
	Delivery             notifications.DeliverySnapshot
	Revision             int64
	AttemptCount         int
	LastAttemptID        string
	StartedAttemptID     string
	LeaseOwner           string
	LeaseEpoch           int64
	LeaseExpiresAt       time.Time
	NextAttemptAt        time.Time
	ProviderOutcome      notifications.ProviderOutcome
	ProviderMessageID    string
	ProviderAttemptID    string
	PossiblyAccepted     bool
	AmbiguousExhausted   bool
	AwaitingIntervention bool
	AwaitingDLQ          bool
}

type AcquireResult struct {
	Disposition AcquireDisposition
	Record      DeliveryRecord
	RetryAfter  time.Duration
}

type ClaimRequest struct {
	Owner        string
	SQSMessageID string
	Now          time.Time
	ExpiresAt    time.Time
}

type GateResult struct {
	Allowed            bool
	CancellationReason notifications.CancellationReason
	Fence              GateFence
}

// GateFence records every durable value that allowed the final recipient gate.
// BeginAttempt condition-checks it atomically with recording the started
// Attempt, so an eligibility change after CheckGates cannot reach the provider.
type GateFence struct {
	Channel                    notifications.Channel
	MembershipVersion          int64
	PreferenceVersion          int64
	PreferenceEmailAddress     string
	PreferenceMinimumSeverity  string
	EventSeverity              string
	EventStatus                string
	DeliverabilityDependencies []DeliverabilityDependency
	TelegramDestinationID      string
	TelegramChatID             int64
	TelegramBindingVersion     int64
	TelegramDestinationVersion int64
}

type DeliverabilityDependency struct {
	Key    notifications.StorageKey
	Exists bool
	State  notifications.EmailDeliverability
}

func (fence GateFence) IsComplete() bool {
	channel := fence.Channel
	if channel == "" {
		channel = notifications.ChannelEmail
	}
	if fence.MembershipVersion < 1 || fence.PreferenceVersion < 1 ||
		!isGateSeverity(fence.PreferenceMinimumSeverity) || !isGateSeverity(fence.EventSeverity) ||
		channel.Validate() != nil {
		return false
	}
	if channel == notifications.ChannelTelegram {
		return fence.TelegramDestinationID != "" &&
			!strings.ContainsRune(fence.TelegramDestinationID, '\x00') &&
			fence.TelegramChatID > 0 && fence.TelegramBindingVersion > 0 &&
			fence.TelegramDestinationVersion > 0 && isTelegramEventStatus(fence.EventStatus) &&
			fence.PreferenceEmailAddress == "" && len(fence.DeliverabilityDependencies) == 0
	}
	if fence.PreferenceEmailAddress == "" || strings.ContainsRune(fence.PreferenceEmailAddress, '\x00') ||
		len(fence.DeliverabilityDependencies) < 1 || len(fence.DeliverabilityDependencies) > 2 {
		return false
	}
	seen := make(map[notifications.StorageKey]struct{}, len(fence.DeliverabilityDependencies))
	for _, dependency := range fence.DeliverabilityDependencies {
		if dependency.Key.PartitionKey == "" || dependency.Key.SortKey == "" ||
			strings.ContainsRune(dependency.Key.PartitionKey, '\x00') || strings.ContainsRune(dependency.Key.SortKey, '\x00') {
			return false
		}
		if _, exists := seen[dependency.Key]; exists {
			return false
		}
		seen[dependency.Key] = struct{}{}
		if dependency.Exists && dependency.State != notifications.EmailDeliverabilityUnknown &&
			dependency.State != notifications.EmailDeliverabilityDeliverable {
			return false
		}
	}
	return true
}

func isTelegramEventStatus(value string) bool {
	return value == "open" || value == "acknowledged" || value == "resolved"
}

func isGateSeverity(value string) bool {
	return value == "warning" || value == "critical"
}

type DeferRequest struct {
	Now           time.Time
	NextAttemptAt time.Time
	ErrorCategory string
}

type BeginAttemptRequest struct {
	AttemptID          string
	StartedAt          time.Time
	LeaseRequiredUntil time.Time
	GateFence          GateFence
}

type AttemptCompletion struct {
	AttemptID                   string
	CompletedAt                 time.Time
	Outcome                     notifications.AttemptOutcome
	ErrorCategory               string
	ProviderMessageID           string
	ProviderOutcome             notifications.ProviderOutcome
	NextState                   notifications.DeliveryState
	NextAttemptAt               time.Time
	PossiblyAccepted            bool
	AmbiguousExhausted          bool
	AwaitingIntervention        bool
	SuppressTelegramDestination bool
}

type PreflightFailureCompletion struct {
	CompletedAt          time.Time
	ErrorCategory        string
	NextState            notifications.DeliveryState
	NextAttemptAt        time.Time
	PossiblyAccepted     bool
	AmbiguousExhausted   bool
	AwaitingIntervention bool
}

type Store interface {
	Acquire(context.Context, notifications.JobEnvelope, ClaimRequest) (AcquireResult, error)
	CheckGates(context.Context, DeliveryRecord) (GateResult, error)
	Cancel(context.Context, DeliveryRecord, notifications.CancellationReason, time.Time) error
	Defer(context.Context, DeliveryRecord, DeferRequest) error
	CompletePreflightFailure(context.Context, DeliveryRecord, PreflightFailureCompletion) error
	BeginAttempt(context.Context, DeliveryRecord, BeginAttemptRequest) (DeliveryRecord, error)
	Refresh(context.Context, DeliveryRecord) (DeliveryRecord, bool, error)
	CompleteAttempt(context.Context, DeliveryRecord, AttemptCompletion) error
	FinalizeUnknown(context.Context, DeliveryRecord, time.Time) error
	Renew(context.Context, DeliveryRecord, time.Time) error
}

type QueueMessage struct {
	MessageID     string
	ReceiptHandle string
	Body          string
	ReceiveCount  int
}

type Queue interface {
	Receive(context.Context, int, time.Duration, time.Duration) ([]QueueMessage, error)
	Delete(context.Context, QueueMessage) error
	ChangeVisibility(context.Context, QueueMessage, time.Duration) error
}

type SendRequest struct {
	Delivery      notifications.DeliverySnapshot
	AttemptID     string
	AttemptNumber int
}

type SendResult struct {
	ProviderMessageID string
}

type SESConfigurationSetName string

func (name SESConfigurationSetName) Validate() error {
	value := string(name)
	if len(value) < 1 || len(value) > 64 {
		return fmt.Errorf("SES configuration set name must contain 1 to 64 characters")
	}
	for index := range len(value) {
		character := value[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' {
			continue
		}
		return fmt.Errorf("SES configuration set name contains an invalid character")
	}
	return nil
}

type EmailSender interface {
	Preflight(SendRequest) error
	Send(context.Context, SendRequest) (SendResult, error)
}

type Sender = EmailSender

type Limiter interface {
	Wait(context.Context) error
}

// DeliveryLimiter allows a shared limiter to apply both provider-wide and
// destination-specific budgets without exposing raw destinations in errors or
// metrics. Implementations must not consume a provider-call attempt.
type DeliveryLimiter interface {
	WaitFor(context.Context, notifications.DeliverySnapshot) error
}

type RateLimitError struct {
	RetryAfter  time.Duration
	Unavailable bool
}

func (err *RateLimitError) Error() string {
	if err.Unavailable {
		return "notification rate limiter unavailable"
	}
	return "notification rate limit exceeded"
}

type LeaseGuard interface {
	Protect(context.Context, QueueMessage, DeliveryRecord) (context.Context, func() error)
}

type SendErrorCategory string

const (
	ErrorPermanentRecipient             SendErrorCategory = "permanent_bad_recipient"
	ErrorProviderCallLimitExhausted     SendErrorCategory = "provider_call_limit_exhausted"
	ErrorRetryableThrottling            SendErrorCategory = "retryable_throttling"
	ErrorRetryableQuota                 SendErrorCategory = "retryable_daily_quota"
	ErrorRetryableService               SendErrorCategory = "retryable_service_unavailable"
	ErrorRetryableUnknown               SendErrorCategory = "retryable_unknown"
	ErrorAmbiguousTimeout               SendErrorCategory = "ambiguous_timeout"
	ErrorAmbiguousConnectionReset       SendErrorCategory = "ambiguous_connection_reset"
	ErrorFatalAccountSuspended          SendErrorCategory = "fatal_account_suspended"
	ErrorFatalFromIdentity              SendErrorCategory = "fatal_from_identity"
	ErrorFatalConfigurationSet          SendErrorCategory = "fatal_configuration_set"
	ErrorFatalCredentials               SendErrorCategory = "fatal_credentials"
	ErrorTelegramRateLimited            SendErrorCategory = "telegram_rate_limited"
	ErrorTelegramDestinationUnavailable SendErrorCategory = "telegram_destination_unavailable"
	ErrorTelegramCredentials            SendErrorCategory = "telegram_invalid_credentials"
	ErrorTelegramAmbiguous              SendErrorCategory = "telegram_ambiguous_send"
)

type SendError struct {
	Category         SendErrorCategory
	RetryAfter       time.Duration
	NoAutomaticRetry bool
	err              error
}

func NewSendError(category SendErrorCategory, err error) *SendError {
	return &SendError{Category: category, err: err}
}

func (err *SendError) Error() string {
	return fmt.Sprintf("notification provider call failed (%s)", err.Category)
}

func (err *SendError) Unwrap() error { return err.err }

func (category SendErrorCategory) disposition() sendDisposition {
	switch category {
	case ErrorPermanentRecipient, ErrorProviderCallLimitExhausted,
		ErrorTelegramDestinationUnavailable:
		return sendPermanent
	case ErrorAmbiguousTimeout, ErrorAmbiguousConnectionReset, ErrorTelegramAmbiguous:
		return sendAmbiguous
	case ErrorFatalAccountSuspended, ErrorFatalFromIdentity,
		ErrorFatalConfigurationSet, ErrorFatalCredentials, ErrorTelegramCredentials:
		return sendFatal
	case ErrorRetryableThrottling, ErrorRetryableQuota, ErrorRetryableService, ErrorRetryableUnknown,
		ErrorTelegramRateLimited:
		return sendRetryable
	default:
		return sendInvalid
	}
}

type sendDisposition uint8

const (
	sendRetryable sendDisposition = iota + 1
	sendPermanent
	sendAmbiguous
	sendFatal
	sendInvalid
)
