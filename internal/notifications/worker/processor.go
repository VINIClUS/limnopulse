package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

const durableMutationTimeout = 5 * time.Second

type DecisionAction uint8

const (
	ActionDelete DecisionAction = iota + 1
	ActionChangeVisibility
	ActionFatal
)

type Decision struct {
	Action        DecisionAction
	Visibility    time.Duration
	ErrorCategory string
}

type Processor struct {
	Store             Store
	Sender            EmailSender
	Limiter           Limiter
	Guard             LeaseGuard
	Owner             string
	ProcessingLease   time.Duration
	ProviderTimeout   time.Duration
	InvalidVisibility time.Duration
	Now               func() time.Time
	NewAttemptID      func() string
	JitterFraction    func() float64
	Metrics           *Metrics
}

func (processor Processor) Handle(ctx context.Context, message QueueMessage) Decision {
	if processor.Store == nil || processor.Sender == nil || processor.Limiter == nil ||
		processor.Now == nil || processor.NewAttemptID == nil || processor.JitterFraction == nil {
		return Decision{Action: ActionFatal, Visibility: time.Minute, ErrorCategory: "configuration"}
	}
	invalidVisibility := processor.InvalidVisibility
	if invalidVisibility <= 0 {
		invalidVisibility = time.Minute
	}
	var job notifications.JobEnvelope
	if err := json.Unmarshal([]byte(message.Body), &job); err != nil {
		return Decision{Action: ActionChangeVisibility, Visibility: invalidVisibility, ErrorCategory: "invalid_job"}
	}
	claimTime := processor.Now().UTC()
	lease := processor.ProcessingLease
	if lease <= 0 {
		lease = time.Minute
	}
	acquired, err := processor.Store.Acquire(ctx, job, ClaimRequest{
		Owner: processor.Owner, SQSMessageID: message.MessageID,
		Now: claimTime, ExpiresAt: claimTime.Add(lease),
	})
	if err != nil {
		return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "claim_failure"}
	}
	switch acquired.Disposition {
	case AcquireTerminal:
		return Decision{Action: ActionDelete}
	case AcquireDeferred:
		return Decision{Action: ActionChangeVisibility, Visibility: normalizedVisibility(acquired.RetryAfter)}
	case AcquireAwaitDLQ:
		return Decision{Action: ActionChangeVisibility, Visibility: invalidVisibility, ErrorCategory: "awaiting_dlq"}
	case AcquireClaimed:
	default:
		return Decision{Action: ActionChangeVisibility, Visibility: invalidVisibility, ErrorCategory: "invalid_claim"}
	}
	record := acquired.Record
	if _, err := notifications.RestoreDelivery(record.Delivery); err != nil ||
		record.Delivery.State != notifications.DeliveryStateProcessing || record.Revision < 1 ||
		record.LeaseOwner == "" || record.LeaseEpoch < 1 {
		return Decision{Action: ActionChangeVisibility, Visibility: invalidVisibility, ErrorCategory: "malformed_delivery"}
	}

	if record.StartedAttemptID != "" {
		return processor.completeInterrupted(ctx, record, claimTime)
	}
	if record.PossiblyAccepted || record.AmbiguousExhausted ||
		record.ProviderOutcome == notifications.ProviderOutcomeDelayed {
		refreshed, terminal, refreshErr := processor.Store.Refresh(ctx, record)
		if refreshErr != nil {
			return processor.deferClaim(ctx, record, processor.Now().UTC(), "reconciliation_failure", time.Minute)
		}
		if terminal {
			return Decision{Action: ActionDelete}
		}
		record = refreshed
	}
	if record.ProviderOutcome == notifications.ProviderOutcomeDelayed {
		// DeliveryDelay proves that SES owns the original message, but it does
		// not prove final mailbox delivery. Once the existing retry/grace becomes
		// due, preserve that uncertainty as unknown instead of issuing a duplicate.
		record.PossiblyAccepted = true
		record.AmbiguousExhausted = true
	}
	if record.AmbiguousExhausted {
		mutationCtx, cancel := mutationContext(ctx)
		defer cancel()
		if err := processor.Store.FinalizeUnknown(mutationCtx, record, processor.Now().UTC()); err != nil {
			return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "unknown_transition_failure"}
		}
		processor.Metrics.RecordUnknown()
		return Decision{Action: ActionDelete}
	}
	if record.AttemptCount >= MaxProviderCalls {
		return processor.completePreflightFailure(
			ctx,
			record,
			processor.Now().UTC(),
			NewSendError(ErrorProviderCallLimitExhausted, errors.New("provider call limit exhausted")),
		)
	}

	limiterCtx := ctx
	stopLimiterGuard := func() error { return nil }
	if processor.Guard != nil {
		limiterCtx, stopLimiterGuard = processor.Guard.Protect(ctx, message, record)
	}
	limiterGuardStopped := false
	stopLimiter := func() error {
		if limiterGuardStopped {
			return nil
		}
		limiterGuardStopped = true
		return stopLimiterGuard()
	}
	defer stopLimiter()
	if limiterCtx.Err() != nil {
		_ = stopLimiter()
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "lease_renewal_failed", time.Minute)
	}

	gate, err := processor.Store.CheckGates(limiterCtx, record)
	if err != nil {
		_ = stopLimiter()
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "gate_lookup_failure", time.Minute)
	}
	if !gate.Allowed {
		_ = stopLimiter()
		mutationCtx, cancel := mutationContext(ctx)
		defer cancel()
		if gate.CancellationReason == "" {
			return processor.deferClaim(ctx, record, processor.Now().UTC(), "invalid_gate", time.Minute)
		}
		if err := processor.Store.Cancel(mutationCtx, record, gate.CancellationReason, processor.Now().UTC()); err != nil {
			return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "cancellation_failure"}
		}
		return Decision{Action: ActionDelete}
	}
	attemptID := processor.NewAttemptID()
	sendRequest := SendRequest{
		Delivery: record.Delivery, AttemptID: attemptID, AttemptNumber: record.AttemptCount + 1,
	}
	if preflightErr := processor.Sender.Preflight(sendRequest); preflightErr != nil {
		if err := stopLimiter(); err != nil {
			return processor.deferClaim(ctx, record, processor.Now().UTC(), "lease_renewal_failed", time.Minute)
		}
		return processor.completePreflightFailure(ctx, record, processor.Now().UTC(), preflightErr)
	}

	limiterDone := processor.Metrics.BeginLimiterWait()
	var limiterErr error
	if deliveryLimiter, ok := processor.Limiter.(DeliveryLimiter); ok {
		limiterErr = deliveryLimiter.WaitFor(limiterCtx, record.Delivery)
	} else {
		limiterErr = processor.Limiter.Wait(limiterCtx)
	}
	if limiterErr != nil {
		limiterDone()
		_ = stopLimiter()
		category := "limiter_cancelled"
		delay := time.Minute
		var rateLimitErr *RateLimitError
		if errors.As(limiterErr, &rateLimitErr) {
			if rateLimitErr.Unavailable {
				category = "limiter_unavailable"
			} else {
				category = "limiter_rate_limited"
			}
			if rateLimitErr.RetryAfter > 0 {
				delay = rateLimitErr.RetryAfter
			}
		}
		return processor.deferClaim(ctx, record, processor.Now().UTC(), category, delay)
	}
	limiterDone()
	if limiterCtx.Err() != nil {
		_ = stopLimiter()
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "lease_renewal_failed", time.Minute)
	}
	// The first gate check avoids spending a token for already-ineligible work.
	// This second check closes the membership/suppression race while a token was
	// pending. Keep the revision-N renewal guard active through this final read,
	// then stop it immediately before BeginAttempt mutates the revision.
	gate, err = processor.Store.CheckGates(limiterCtx, record)
	if err != nil {
		_ = stopLimiter()
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "gate_lookup_failure", time.Minute)
	}
	if !gate.Allowed {
		if err := stopLimiter(); err != nil {
			return processor.deferClaim(ctx, record, processor.Now().UTC(), "lease_renewal_failed", time.Minute)
		}
		mutationCtx, cancel := mutationContext(ctx)
		defer cancel()
		if gate.CancellationReason == "" {
			return processor.deferClaim(ctx, record, processor.Now().UTC(), "invalid_gate", time.Minute)
		}
		if err := processor.Store.Cancel(mutationCtx, record, gate.CancellationReason, processor.Now().UTC()); err != nil {
			return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "cancellation_failure"}
		}
		return Decision{Action: ActionDelete}
	}
	if !gate.Fence.IsComplete() {
		if err := stopLimiter(); err != nil {
			return processor.deferClaim(ctx, record, processor.Now().UTC(), "lease_renewal_failed", time.Minute)
		}
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "invalid_gate", time.Minute)
	}
	if limiterCtx.Err() != nil {
		_ = stopLimiter()
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "lease_renewal_failed", time.Minute)
	}
	if err := stopLimiter(); err != nil {
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "lease_renewal_failed", time.Minute)
	}
	if ctx.Err() != nil {
		return processor.deferClaim(ctx, record, processor.Now().UTC(), "worker_context_cancelled", time.Minute)
	}

	providerTimeout := processor.ProviderTimeout
	if providerTimeout <= 0 {
		providerTimeout = 15 * time.Second
	}
	attemptStartedAt := processor.Now().UTC()
	mutationCtx, cancelMutation := mutationContext(ctx)
	record, err = processor.Store.BeginAttempt(mutationCtx, record, BeginAttemptRequest{
		AttemptID: attemptID, StartedAt: attemptStartedAt,
		LeaseRequiredUntil: attemptStartedAt.Add(providerTimeout + 2*durableMutationTimeout),
		GateFence:          gate.Fence,
	})
	cancelMutation()
	if err != nil {
		return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "attempt_start_failure"}
	}

	// The limiter guard is stopped only after its in-flight renewal completes.
	// BeginAttempt conditionally reserves enough durable lease headroom for its
	// own transaction, the bounded provider call, and the completion write, while
	// remaining immediately adjacent to the single provider call.
	// This keeps attempt_count equal to the number of EmailSender.Send invocations.
	providerCtx, cancelProvider := context.WithTimeout(ctx, providerTimeout)
	processor.Metrics.ProviderCallStarted(record.PossiblyAccepted)
	result, sendErr := processor.Sender.Send(providerCtx, sendRequest)
	processor.Metrics.ProviderCallFinished()
	cancelProvider()
	completedAt := processor.Now().UTC()
	return processor.finishAttempt(ctx, record, attemptID, completedAt, result, sendErr)
}

func (processor Processor) completePreflightFailure(
	ctx context.Context,
	record DeliveryRecord,
	now time.Time,
	preflightErr error,
) Decision {
	var senderErr *SendError
	if !errors.As(preflightErr, &senderErr) {
		senderErr = NewSendError(ErrorRetryableUnknown, preflightErr)
	}
	if disposition := senderErr.Category.disposition(); disposition != sendPermanent && disposition != sendFatal {
		senderErr = NewSendError(ErrorFatalConfigurationSet, senderErr)
	}
	plan, ok := processor.planDefinitiveFailure(record, senderErr.Category, now)
	if !ok {
		return processor.deferClaim(ctx, record, now, "invalid_sender_preflight", time.Minute)
	}
	completion := PreflightFailureCompletion{
		CompletedAt: now, ErrorCategory: string(senderErr.Category), NextState: plan.NextState,
		NextAttemptAt: plan.NextAttemptAt, PossiblyAccepted: plan.PossiblyAccepted,
		AmbiguousExhausted: plan.AmbiguousExhausted, AwaitingIntervention: plan.AwaitingIntervention,
	}
	decision := Decision{
		Action: plan.Action, Visibility: plan.Visibility, ErrorCategory: string(senderErr.Category),
	}
	mutationCtx, cancel := mutationContext(ctx)
	defer cancel()
	if err := processor.Store.CompletePreflightFailure(mutationCtx, record, completion); err != nil {
		if errors.Is(err, ErrConcurrentTerminal) {
			if decision.Action == ActionFatal {
				return decision
			}
			return Decision{Action: ActionDelete}
		}
		if decision.Action == ActionFatal {
			return Decision{Action: ActionFatal, Visibility: decision.Visibility, ErrorCategory: "fatal_persistence_failure"}
		}
		return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "preflight_completion_failure"}
	}
	return decision
}

func (processor Processor) completeInterrupted(
	ctx context.Context,
	record DeliveryRecord,
	now time.Time,
) Decision {
	if record.Delivery.Channel == notifications.ChannelTelegram {
		completion := AttemptCompletion{
			AttemptID: record.StartedAttemptID, CompletedAt: now,
			Outcome:            notifications.AttemptOutcomeAmbiguous,
			ErrorCategory:      string(ErrorTelegramAmbiguous),
			NextState:          notifications.DeliveryStateUnknown,
			PossiblyAccepted:   true,
			AmbiguousExhausted: true,
		}
		mutationCtx, cancel := mutationContext(ctx)
		defer cancel()
		if err := processor.Store.CompleteAttempt(mutationCtx, record, completion); err != nil {
			if errors.Is(err, ErrConcurrentTerminal) {
				return Decision{Action: ActionDelete}
			}
			return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "interrupted_attempt_recovery_failure"}
		}
		processor.Metrics.RecordUnknown()
		return Decision{Action: ActionDelete, ErrorCategory: string(ErrorTelegramAmbiguous)}
	}
	delay := RetryDelay(record.AttemptCount, true, processor.JitterFraction())
	completion := AttemptCompletion{
		AttemptID: record.StartedAttemptID, CompletedAt: now,
		Outcome:       notifications.AttemptOutcomeAmbiguous,
		ErrorCategory: string(ErrorAmbiguousConnectionReset),
		NextState:     notifications.DeliveryStateRetryableFailed,
		NextAttemptAt: now.Add(delay), PossiblyAccepted: true,
		AmbiguousExhausted: record.AttemptCount >= MaxProviderCalls,
	}
	mutationCtx, cancel := mutationContext(ctx)
	defer cancel()
	if err := processor.Store.CompleteAttempt(mutationCtx, record, completion); err != nil {
		if errors.Is(err, ErrConcurrentTerminal) {
			return Decision{Action: ActionDelete}
		}
		return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "interrupted_attempt_recovery_failure"}
	}
	processor.Metrics.RecordRetry(ErrorAmbiguousConnectionReset, true)
	return Decision{Action: ActionChangeVisibility, Visibility: normalizedVisibility(delay), ErrorCategory: string(ErrorAmbiguousConnectionReset)}
}

func (processor Processor) finishAttempt(
	ctx context.Context,
	record DeliveryRecord,
	attemptID string,
	now time.Time,
	result SendResult,
	sendErr error,
) Decision {
	completion := AttemptCompletion{AttemptID: attemptID, CompletedAt: now}
	decision := Decision{}
	metricSucceededAfterRetry := false
	metricRetryCategory := SendErrorCategory("")
	metricRetryAmbiguous := false
	metricUnknown := false
	if sendErr == nil {
		if result.ProviderMessageID == "" {
			sendErr = NewSendError(ErrorRetryableUnknown, errors.New("provider message ID is missing"))
		} else {
			completion.Outcome = notifications.AttemptOutcomeSucceeded
			completion.NextState = notifications.DeliveryStateSucceeded
			completion.ProviderOutcome = notifications.ProviderOutcomeAccepted
			completion.ProviderMessageID = result.ProviderMessageID
			decision.Action = ActionDelete
			metricSucceededAfterRetry = record.AttemptCount > 1
		}
	}
	if sendErr != nil {
		var providerErr *SendError
		if !errors.As(sendErr, &providerErr) {
			providerErr = NewSendError(ErrorRetryableUnknown, sendErr)
		}
		if providerErr.Category.disposition() == sendInvalid {
			providerErr = NewSendError(ErrorRetryableUnknown, providerErr)
		}
		completion.ErrorCategory = string(providerErr.Category)
		switch providerErr.Category.disposition() {
		case sendPermanent, sendFatal:
			if providerErr.Category.disposition() == sendPermanent {
				completion.Outcome = notifications.AttemptOutcomePermanentFailed
			} else {
				completion.Outcome = notifications.AttemptOutcomeRetryable
			}
			plan, _ := processor.planDefinitiveFailure(record, providerErr.Category, now)
			completion.SuppressTelegramDestination =
				providerErr.Category == ErrorTelegramDestinationUnavailable &&
					record.Delivery.Channel == notifications.ChannelTelegram
			completion.NextState = plan.NextState
			completion.NextAttemptAt = plan.NextAttemptAt
			completion.PossiblyAccepted = plan.PossiblyAccepted
			completion.AmbiguousExhausted = plan.AmbiguousExhausted
			completion.AwaitingIntervention = plan.AwaitingIntervention
			decision.Action = plan.Action
			decision.Visibility = plan.Visibility
			if plan.Action != ActionDelete {
				decision.ErrorCategory = string(providerErr.Category)
			}
		case sendRetryable:
			metricRetryCategory = providerErr.Category
			completion.Outcome = notifications.AttemptOutcomeRetryable
			if record.AttemptCount >= MaxProviderCalls {
				if record.PossiblyAccepted {
					delay := RetryDelay(record.AttemptCount, true, processor.JitterFraction())
					completion.NextState = notifications.DeliveryStateRetryableFailed
					completion.NextAttemptAt = now.Add(delay)
					completion.PossiblyAccepted = true
					completion.AmbiguousExhausted = true
					decision = Decision{Action: ActionChangeVisibility, Visibility: normalizedVisibility(delay), ErrorCategory: string(providerErr.Category)}
				} else {
					completion.NextState = notifications.DeliveryStatePermanentFailed
					decision.Action = ActionDelete
				}
			} else {
				delay := RetryDelay(record.AttemptCount, false, processor.JitterFraction())
				if providerErr.RetryAfter > delay {
					delay = providerErr.RetryAfter
				}
				completion.NextState = notifications.DeliveryStateRetryableFailed
				completion.NextAttemptAt = now.Add(delay)
				decision = Decision{Action: ActionChangeVisibility, Visibility: normalizedVisibility(delay), ErrorCategory: string(providerErr.Category)}
			}
		case sendAmbiguous:
			completion.Outcome = notifications.AttemptOutcomeAmbiguous
			completion.PossiblyAccepted = true
			if providerErr.NoAutomaticRetry {
				completion.NextState = notifications.DeliveryStateUnknown
				completion.AmbiguousExhausted = true
				decision.Action = ActionDelete
				metricUnknown = true
			} else {
				metricRetryCategory = providerErr.Category
				metricRetryAmbiguous = true
				delay := RetryDelay(record.AttemptCount, true, processor.JitterFraction())
				completion.NextState = notifications.DeliveryStateRetryableFailed
				completion.NextAttemptAt = now.Add(delay)
				completion.AmbiguousExhausted = record.AttemptCount >= MaxProviderCalls
				decision = Decision{Action: ActionChangeVisibility, Visibility: normalizedVisibility(delay), ErrorCategory: string(providerErr.Category)}
			}
		}
	}
	mutationCtx, cancel := mutationContext(ctx)
	defer cancel()
	if err := processor.Store.CompleteAttempt(mutationCtx, record, completion); err != nil {
		if errors.Is(err, ErrConcurrentTerminal) {
			if decision.Action == ActionFatal {
				return decision
			}
			return Decision{Action: ActionDelete}
		}
		if decision.Action == ActionFatal {
			return Decision{
				Action: ActionFatal, Visibility: decision.Visibility,
				ErrorCategory: "fatal_persistence_failure",
			}
		}
		return Decision{Action: ActionChangeVisibility, Visibility: time.Minute, ErrorCategory: "attempt_completion_failure"}
	}
	if metricSucceededAfterRetry {
		processor.Metrics.RecordSucceeded(true)
	}
	if metricRetryCategory != "" {
		processor.Metrics.RecordRetry(metricRetryCategory, metricRetryAmbiguous)
	}
	if metricUnknown {
		processor.Metrics.RecordUnknown()
	}
	return decision
}

func (processor Processor) deferClaim(
	ctx context.Context,
	record DeliveryRecord,
	now time.Time,
	category string,
	delay time.Duration,
) Decision {
	mutationCtx, cancel := mutationContext(ctx)
	defer cancel()
	if err := processor.Store.Defer(mutationCtx, record, DeferRequest{
		Now: now, NextAttemptAt: now.Add(delay), ErrorCategory: category,
	}); err != nil {
		category = "defer_failure"
	}
	return Decision{Action: ActionChangeVisibility, Visibility: normalizedVisibility(delay), ErrorCategory: category}
}

func normalizedVisibility(delay time.Duration) time.Duration {
	if delay <= 0 {
		return time.Second
	}
	seconds := (delay + time.Second - 1) / time.Second
	if seconds > 12*time.Hour/time.Second {
		seconds = 12 * time.Hour / time.Second
	}
	return seconds * time.Second
}

func mutationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), durableMutationTimeout)
}
