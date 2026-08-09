package worker

import (
	"context"
	"errors"
	"fmt"
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
}

type AttemptCompletion struct {
	AttemptID            string
	CompletedAt          time.Time
	Outcome              notifications.AttemptOutcome
	ErrorCategory        string
	ProviderMessageID    string
	ProviderOutcome      notifications.ProviderOutcome
	NextState            notifications.DeliveryState
	NextAttemptAt        time.Time
	PossiblyAccepted     bool
	AmbiguousExhausted   bool
	AwaitingIntervention bool
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

type Limiter interface {
	Wait(context.Context) error
}

type LeaseGuard interface {
	Protect(context.Context, QueueMessage, DeliveryRecord) (context.Context, func() error)
}

type SendErrorCategory string

const (
	ErrorPermanentRecipient       SendErrorCategory = "permanent_bad_recipient"
	ErrorRetryableThrottling      SendErrorCategory = "retryable_throttling"
	ErrorRetryableQuota           SendErrorCategory = "retryable_daily_quota"
	ErrorRetryableService         SendErrorCategory = "retryable_service_unavailable"
	ErrorRetryableUnknown         SendErrorCategory = "retryable_unknown"
	ErrorAmbiguousTimeout         SendErrorCategory = "ambiguous_timeout"
	ErrorAmbiguousConnectionReset SendErrorCategory = "ambiguous_connection_reset"
	ErrorFatalAccountSuspended    SendErrorCategory = "fatal_account_suspended"
	ErrorFatalFromIdentity        SendErrorCategory = "fatal_from_identity"
	ErrorFatalConfigurationSet    SendErrorCategory = "fatal_configuration_set"
	ErrorFatalCredentials         SendErrorCategory = "fatal_credentials"
)

type SendError struct {
	Category SendErrorCategory
	err      error
}

func NewSendError(category SendErrorCategory, err error) *SendError {
	return &SendError{Category: category, err: err}
}

func (err *SendError) Error() string {
	return fmt.Sprintf("email provider call failed (%s)", err.Category)
}

func (err *SendError) Unwrap() error { return err.err }

func (category SendErrorCategory) disposition() sendDisposition {
	switch category {
	case ErrorPermanentRecipient:
		return sendPermanent
	case ErrorAmbiguousTimeout, ErrorAmbiguousConnectionReset:
		return sendAmbiguous
	case ErrorFatalAccountSuspended, ErrorFatalFromIdentity,
		ErrorFatalConfigurationSet, ErrorFatalCredentials:
		return sendFatal
	case ErrorRetryableThrottling, ErrorRetryableQuota, ErrorRetryableService, ErrorRetryableUnknown:
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
