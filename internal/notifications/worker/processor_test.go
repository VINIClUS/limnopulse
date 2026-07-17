package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

func TestProcessorRejectsInvalidAndDeletesTerminalDuplicatesWithoutSES(t *testing.T) {
	store := &fakeStore{}
	sender := &fakeSender{}
	processor := testProcessor(store, sender)

	decision := processor.Handle(context.Background(), QueueMessage{
		Body:          `{\"schema_version\":1,\"normalized_email\":\"secret@example.com\"}`,
		ReceiptHandle: "receipt_invalid",
	})
	if decision.Action != ActionChangeVisibility || decision.Visibility != time.Minute || sender.calls != 0 {
		t.Fatalf("invalid decision = %#v, sends = %d", decision, sender.calls)
	}
	validWithPII := validMessage(t)
	validWithPII.Body = `{\"schema_version\":1,\"message_type\":\"notification.delivery\",` +
		`\"tenant_id\":\"tnt_1\",\"outbox_id\":\"outbox_1\",\"delivery_id\":\"del_1\",` +
		`\"event_id\":\"alert_1\",\"kind\":\"opening\",\"channel\":\"email\",` +
		`\"normalized_email\":\"secret@example.com\"}`
	decision = processor.Handle(context.Background(), validWithPII)
	if decision.Action != ActionChangeVisibility || sender.calls != 0 {
		t.Fatalf("PII-bearing strict envelope decision = %#v", decision)
	}

	store.acquire = AcquireResult{Disposition: AcquireTerminal}
	decision = processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionDelete || sender.calls != 0 {
		t.Fatalf("terminal duplicate decision = %#v, sends = %d", decision, sender.calls)
	}
}

func TestProcessorRenewalFailureBeforeSendNeverCallsProvider(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{}
	processor := testProcessor(store, sender)
	processor.Guard = failingGuard{}
	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionChangeVisibility || store.deferred != 1 || store.begun != 0 || sender.calls != 0 {
		t.Fatalf("renewal failure decision=%#v store=%#v sends=%d", decision, store, sender.calls)
	}
}

func TestProcessorRestartsRenewalGuardWithAttemptRevisionBeforeProviderCall(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{result: SendResult{ProviderMessageID: "ses_1"}}
	guard := &recordingGuard{}
	processor := testProcessor(store, sender)
	processor.Guard = guard

	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionDelete || len(guard.records) != 2 || guard.stopped != 2 {
		t.Fatalf("decision=%#v records=%#v stopped=%d", decision, guard.records, guard.stopped)
	}
	if guard.records[0].Revision != 3 || guard.records[0].AttemptCount != 0 ||
		guard.records[1].Revision != 4 || guard.records[1].AttemptCount != 1 {
		t.Fatalf("guard records = %#v", guard.records)
	}
}

func TestProcessorWaitsForCrossingLimiterRenewalBeforeBeginningAttempt(t *testing.T) {
	beginCalled := make(chan struct{})
	store := &fakeStore{
		acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true},
		beginCalled: beginCalled,
	}
	sender := &fakeSender{result: SendResult{ProviderMessageID: "ses_1"}}
	guard := &crossingGuard{renewalStarted: make(chan struct{}), releaseRenewal: make(chan struct{})}
	processor := testProcessor(store, sender)
	processor.Guard = guard
	message := validMessage(t)
	done := make(chan Decision, 1)
	go func() { done <- processor.Handle(context.Background(), message) }()

	<-guard.renewalStarted
	select {
	case <-beginCalled:
		t.Fatal("attempt began while the revision-N renewal was still running")
	default:
	}
	close(guard.releaseRenewal)
	decision := <-done
	if decision.Action != ActionDelete {
		t.Fatalf("decision = %#v", decision)
	}
	select {
	case <-beginCalled:
	default:
		t.Fatal("attempt did not begin after the revision-N guard stopped")
	}
}

func TestProcessorProviderGuardFailureAfterBeginCompletesAttemptAsAmbiguous(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{}
	processor := testProcessor(store, sender)
	processor.Guard = &secondFailingGuard{}

	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionChangeVisibility || store.begun != 1 || store.completion == nil ||
		store.completion.Outcome != notifications.AttemptOutcomeRetryable ||
		store.completion.ErrorCategory != "lease_renewal_failed" ||
		store.completion.PossiblyAccepted || sender.calls != 0 {
		t.Fatalf("decision=%#v completion=%#v sends=%d", decision, store.completion, sender.calls)
	}
}

func TestProcessorPreSendFifthSlotPreservesHistoricalAmbiguityForFinalGrace(t *testing.T) {
	acquired := claimedRecord(t, 4)
	acquired.Record.PossiblyAccepted = true
	store := &fakeStore{acquire: acquired, gate: GateResult{Allowed: true}}
	sender := &fakeSender{}
	processor := testProcessor(store, sender)
	processor.Guard = &secondFailingGuard{}

	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionChangeVisibility || sender.calls != 0 || store.completion == nil ||
		store.completion.Outcome != notifications.AttemptOutcomeRetryable ||
		store.completion.NextState != notifications.DeliveryStateRetryableFailed ||
		!store.completion.PossiblyAccepted || !store.completion.AmbiguousExhausted ||
		store.completion.NextAttemptAt.IsZero() {
		t.Fatalf("decision=%#v completion=%#v sends=%d", decision, store.completion, sender.calls)
	}
}

func TestProcessorProviderGuardStopFailureOverridesApparentSuccessAsAmbiguous(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{result: SendResult{ProviderMessageID: "ses_maybe_accepted"}}
	processor := testProcessor(store, sender)
	processor.Guard = &secondStopFailingGuard{}

	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionChangeVisibility || sender.calls != 1 || store.completion == nil ||
		store.completion.Outcome != notifications.AttemptOutcomeAmbiguous ||
		store.completion.ProviderMessageID != "" || !store.completion.PossiblyAccepted {
		t.Fatalf("decision=%#v completion=%#v sends=%d", decision, store.completion, sender.calls)
	}
}

func TestProcessorAmbiguousRefreshAlreadyTerminalIsNoop(t *testing.T) {
	acquired := claimedRecord(t, 2)
	acquired.Record.PossiblyAccepted = true
	store := &fakeStore{acquire: acquired, refreshTerminal: true, gate: GateResult{Allowed: true}}
	sender := &fakeSender{}
	decision := testProcessor(store, sender).Handle(context.Background(), validMessage(t))
	if decision.Action != ActionDelete || store.refreshes != 1 || store.begun != 0 || sender.calls != 0 {
		t.Fatalf("reconciled decision=%#v store=%#v sends=%d", decision, store, sender.calls)
	}
}

func TestProcessorCapturesAttemptAndCompletionTimesAroundProviderCall(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{result: SendResult{ProviderMessageID: "ses_1"}}
	processor := testProcessor(store, sender)
	times := []time.Time{
		time.Date(2026, 7, 16, 15, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 16, 15, 0, 7, 0, time.UTC),
		time.Date(2026, 7, 16, 15, 0, 19, 0, time.UTC),
	}
	processor.Now = func() time.Time {
		value := times[0]
		times = times[1:]
		return value
	}
	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionDelete || !store.beginRequest.StartedAt.Equal(time.Date(2026, 7, 16, 15, 0, 7, 0, time.UTC)) ||
		store.completion == nil || !store.completion.CompletedAt.Equal(time.Date(2026, 7, 16, 15, 0, 19, 0, time.UTC)) {
		t.Fatalf("clock decision=%#v begin=%#v completion=%#v", decision, store.beginRequest, store.completion)
	}
}

func TestProcessorUnknownProviderCategoryIsDurablyClassifiedRetryableUnknown(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{err: NewSendError("invented", errors.New("unexpected"))}
	decision := testProcessor(store, sender).Handle(context.Background(), validMessage(t))
	if decision.Action != ActionChangeVisibility || store.completion == nil ||
		store.completion.ErrorCategory != string(ErrorRetryableUnknown) {
		t.Fatalf("unknown category decision=%#v completion=%#v", decision, store.completion)
	}
}

func TestProcessorWiresAggregateMetricsWithoutIdentifiers(t *testing.T) {
	acquired := claimedRecord(t, 1)
	acquired.Record.PossiblyAccepted = true
	store := &fakeStore{acquire: acquired, gate: GateResult{Allowed: true}}
	sender := &fakeSender{err: NewSendError(ErrorRetryableThrottling, errors.New("slow"))}
	processor := testProcessor(store, sender)
	processor.Metrics = NewMetrics(1)
	decision := processor.Handle(context.Background(), validMessage(t))
	snapshot := processor.Metrics.Snapshot()
	if decision.Action != ActionChangeVisibility || snapshot.Attempts != 1 || snapshot.SendStarted != 1 ||
		snapshot.Retries != 1 || snapshot.Throttling != 1 || snapshot.PossibleDuplicates != 1 ||
		snapshot.ActiveConcurrency != 0 {
		t.Fatalf("decision=%#v metrics=%#v", decision, snapshot)
	}
}

func TestProcessorMembershipAndSuppressionGatesDoNotConsumeTokenOrAttempt(t *testing.T) {
	for _, gate := range []GateResult{
		{Allowed: false, CancellationReason: notifications.CancellationReasonRecipientMembershipInactive},
		{Allowed: false, CancellationReason: notifications.CancellationReasonEmailSuppressed},
	} {
		store := &fakeStore{acquire: claimedRecord(t, 0), gate: gate}
		limiter := &fakeLimiter{}
		sender := &fakeSender{}
		processor := testProcessor(store, sender)
		processor.Limiter = limiter

		decision := processor.Handle(context.Background(), validMessage(t))
		if decision.Action != ActionDelete || !store.cancelled || store.begun != 0 ||
			limiter.calls != 0 || sender.calls != 0 {
			t.Fatalf("gate decision = %#v store=%#v limiter=%d sender=%d", decision, store, limiter.calls, sender.calls)
		}
	}
}

func TestProcessorPersistsAttemptBeforeSESAndSucceeds(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{result: SendResult{ProviderMessageID: "ses_message_1"}}
	processor := testProcessor(store, sender)

	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionDelete || store.begun != 1 || sender.calls != 1 || store.completion == nil {
		t.Fatalf("success decision=%#v store=%#v sends=%d", decision, store, sender.calls)
	}
	if sender.sawAttemptCount != 1 || store.sequence != "begin,send,complete" {
		t.Fatalf("persistence/send sequence = %q, sender attempt count = %d", store.sequence, sender.sawAttemptCount)
	}
	if store.completion.NextState != notifications.DeliveryStateSucceeded ||
		store.completion.ProviderOutcome != notifications.ProviderOutcomeAccepted ||
		store.completion.ProviderMessageID != "ses_message_1" {
		t.Fatalf("completion = %#v", store.completion)
	}
}

func TestProcessorLimiterCancellationDefersWithoutAttempt(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	processor := testProcessor(store, &fakeSender{})
	processor.Limiter = &fakeLimiter{err: context.Canceled}

	decision := processor.Handle(context.Background(), validMessage(t))
	if decision.Action != ActionChangeVisibility || store.deferred != 1 || store.begun != 0 {
		t.Fatalf("limiter cancellation decision=%#v store=%#v", decision, store)
	}
}

func TestProcessorRetryAndAmbiguousExhaustionUseVisibilityWithoutSixthCall(t *testing.T) {
	t.Run("fifth confirmed retry is permanent", func(t *testing.T) {
		store := &fakeStore{acquire: claimedRecord(t, 4), gate: GateResult{Allowed: true}}
		sender := &fakeSender{err: NewSendError(ErrorRetryableService, errors.New("unavailable"))}
		processor := testProcessor(store, sender)
		decision := processor.Handle(context.Background(), validMessage(t))
		if decision.Action != ActionDelete || sender.calls != 1 || store.completion == nil ||
			store.completion.NextState != notifications.DeliveryStatePermanentFailed {
			t.Fatalf("retry exhaustion decision=%#v completion=%#v sends=%d", decision, store.completion, sender.calls)
		}
	})

	t.Run("fifth ambiguous waits final grace then becomes unknown", func(t *testing.T) {
		store := &fakeStore{acquire: claimedRecord(t, 4), gate: GateResult{Allowed: true}}
		sender := &fakeSender{err: NewSendError(ErrorAmbiguousTimeout, context.DeadlineExceeded)}
		processor := testProcessor(store, sender)
		decision := processor.Handle(context.Background(), validMessage(t))
		if decision.Action != ActionChangeVisibility || sender.calls != 1 || store.completion == nil ||
			!store.completion.AmbiguousExhausted || !store.completion.PossiblyAccepted ||
			store.completion.NextState != notifications.DeliveryStateRetryableFailed {
			t.Fatalf("ambiguous fifth decision=%#v completion=%#v", decision, store.completion)
		}

		unknownRecord := claimedRecord(t, 5)
		unknownRecord.Record.AmbiguousExhausted = true
		unknownRecord.Record.PossiblyAccepted = true
		store = &fakeStore{acquire: unknownRecord, gate: GateResult{Allowed: true}}
		sender = &fakeSender{}
		processor = testProcessor(store, sender)
		decision = processor.Handle(context.Background(), validMessage(t))
		if decision.Action != ActionDelete || !store.unknown || sender.calls != 0 {
			t.Fatalf("final grace decision=%#v unknown=%t sends=%d", decision, store.unknown, sender.calls)
		}
	})
}

func TestProcessorMixedAmbiguousHistoryNeverBecomesPermanentOnFifthConfirmedFailure(t *testing.T) {
	for _, category := range []SendErrorCategory{ErrorRetryableService, ErrorPermanentRecipient} {
		acquired := claimedRecord(t, 4)
		acquired.Record.PossiblyAccepted = true
		store := &fakeStore{acquire: acquired, gate: GateResult{Allowed: true}}
		sender := &fakeSender{err: NewSendError(category, errors.New("fifth failure"))}
		decision := testProcessor(store, sender).Handle(context.Background(), validMessage(t))
		if decision.Action != ActionChangeVisibility || store.completion == nil ||
			store.completion.NextState != notifications.DeliveryStateRetryableFailed ||
			!store.completion.PossiblyAccepted || !store.completion.AmbiguousExhausted ||
			store.completion.NextAttemptAt.IsZero() {
			t.Fatalf("category=%s decision=%#v completion=%#v", category, decision, store.completion)
		}
	}
}

func TestProcessorExpiredStartedAttemptBecomesAmbiguousWithoutImmediateSES(t *testing.T) {
	acquired := claimedRecord(t, 1)
	acquired.Record.StartedAttemptID = "att_interrupted"
	store := &fakeStore{acquire: acquired, gate: GateResult{Allowed: true}}
	sender := &fakeSender{}
	decision := testProcessor(store, sender).Handle(context.Background(), validMessage(t))
	if decision.Action != ActionChangeVisibility || sender.calls != 0 || store.completion == nil ||
		store.completion.AttemptID != "att_interrupted" ||
		store.completion.Outcome != notifications.AttemptOutcomeAmbiguous {
		t.Fatalf("interrupted decision=%#v completion=%#v sends=%d", decision, store.completion, sender.calls)
	}
}

func TestProcessorFatalSystemicLeavesMessageAndStopsWorker(t *testing.T) {
	store := &fakeStore{acquire: claimedRecord(t, 0), gate: GateResult{Allowed: true}}
	sender := &fakeSender{err: NewSendError(ErrorFatalCredentials, errors.New("credentials"))}
	decision := testProcessor(store, sender).Handle(context.Background(), validMessage(t))
	if decision.Action != ActionFatal || decision.Visibility <= 0 || store.completion == nil ||
		!store.completion.AwaitingIntervention ||
		store.completion.NextState != notifications.DeliveryStateRetryableFailed {
		t.Fatalf("fatal decision=%#v completion=%#v", decision, store.completion)
	}
}

type fakeStore struct {
	acquire         AcquireResult
	acquireErr      error
	gate            GateResult
	gateErr         error
	cancelled       bool
	deferred        int
	begun           int
	unknown         bool
	completion      *AttemptCompletion
	sequence        string
	beginRequest    BeginAttemptRequest
	refreshTerminal bool
	refreshes       int
	beginCalled     chan struct{}
}

func (store *fakeStore) Acquire(context.Context, notifications.JobEnvelope, ClaimRequest) (AcquireResult, error) {
	return store.acquire, store.acquireErr
}
func (store *fakeStore) CheckGates(context.Context, DeliveryRecord) (GateResult, error) {
	return store.gate, store.gateErr
}
func (store *fakeStore) Cancel(_ context.Context, _ DeliveryRecord, _ notifications.CancellationReason, _ time.Time) error {
	store.cancelled = true
	return nil
}
func (store *fakeStore) Defer(_ context.Context, _ DeliveryRecord, _ DeferRequest) error {
	store.deferred++
	return nil
}
func (store *fakeStore) BeginAttempt(_ context.Context, record DeliveryRecord, request BeginAttemptRequest) (DeliveryRecord, error) {
	if store.beginCalled != nil {
		close(store.beginCalled)
	}
	store.begun++
	store.sequence = appendSequence(store.sequence, "begin")
	store.beginRequest = request
	record.Revision++
	record.AttemptCount++
	record.LastAttemptID = request.AttemptID
	return record, nil
}
func (store *fakeStore) Refresh(_ context.Context, record DeliveryRecord) (DeliveryRecord, bool, error) {
	store.refreshes++
	return record, store.refreshTerminal, nil
}
func (store *fakeStore) CompleteAttempt(_ context.Context, _ DeliveryRecord, completion AttemptCompletion) error {
	store.sequence = appendSequence(store.sequence, "complete")
	store.completion = &completion
	return nil
}
func (store *fakeStore) FinalizeUnknown(context.Context, DeliveryRecord, time.Time) error {
	store.unknown = true
	return nil
}
func (store *fakeStore) Renew(context.Context, DeliveryRecord, time.Time) error { return nil }

type fakeSender struct {
	result          SendResult
	err             error
	calls           int
	sawAttemptCount int
	store           *fakeStore
}

func (sender *fakeSender) Send(_ context.Context, request SendRequest) (SendResult, error) {
	sender.calls++
	sender.sawAttemptCount = request.AttemptNumber
	if sender.store != nil {
		sender.store.sequence = appendSequence(sender.store.sequence, "send")
	}
	return sender.result, sender.err
}

type fakeLimiter struct {
	calls int
	err   error
}

func (limiter *fakeLimiter) Wait(context.Context) error {
	limiter.calls++
	return limiter.err
}

type fakeGuard struct{}

func (fakeGuard) Protect(ctx context.Context, _ QueueMessage, _ DeliveryRecord) (context.Context, func() error) {
	return ctx, func() error { return nil }
}

type recordingGuard struct {
	records []DeliveryRecord
	stopped int
}

func (guard *recordingGuard) Protect(ctx context.Context, _ QueueMessage, record DeliveryRecord) (context.Context, func() error) {
	guard.records = append(guard.records, record)
	return ctx, func() error {
		guard.stopped++
		return nil
	}
}

type crossingGuard struct {
	renewalStarted chan struct{}
	releaseRenewal chan struct{}
	calls          int
}

func (guard *crossingGuard) Protect(ctx context.Context, _ QueueMessage, _ DeliveryRecord) (context.Context, func() error) {
	guard.calls++
	if guard.calls != 1 {
		return ctx, func() error { return nil }
	}
	var once sync.Once
	done := make(chan struct{})
	return ctx, func() error {
		once.Do(func() {
			go func() {
				close(guard.renewalStarted)
				<-guard.releaseRenewal
				close(done)
			}()
		})
		<-done
		return nil
	}
}

type secondFailingGuard struct{ calls int }

func (guard *secondFailingGuard) Protect(ctx context.Context, _ QueueMessage, _ DeliveryRecord) (context.Context, func() error) {
	guard.calls++
	if guard.calls == 1 {
		return ctx, func() error { return nil }
	}
	failed, cancel := context.WithCancel(ctx)
	cancel()
	return failed, func() error { return errors.New("provider guard renewal failed") }
}

type secondStopFailingGuard struct{ calls int }

func (guard *secondStopFailingGuard) Protect(ctx context.Context, _ QueueMessage, _ DeliveryRecord) (context.Context, func() error) {
	guard.calls++
	if guard.calls == 1 {
		return ctx, func() error { return nil }
	}
	return ctx, func() error { return errors.New("provider guard stop failed") }
}

type failingGuard struct{}

func (failingGuard) Protect(ctx context.Context, _ QueueMessage, _ DeliveryRecord) (context.Context, func() error) {
	failed, cancel := context.WithCancel(ctx)
	cancel()
	return failed, func() error { return errors.New("renewal failed") }
}

func testProcessor(store *fakeStore, sender *fakeSender) Processor {
	if sender.store == nil {
		sender.store = store
	}
	return Processor{
		Store: store, Sender: sender, Limiter: &fakeLimiter{}, Guard: fakeGuard{},
		Now:             func() time.Time { return time.Date(2026, 7, 16, 15, 0, 0, 0, time.UTC) },
		NewAttemptID:    func() string { return "att_deterministic" },
		JitterFraction:  func() float64 { return 0.5 },
		ProviderTimeout: 15 * time.Second, InvalidVisibility: time.Minute,
	}
}

func claimedRecord(t *testing.T, attempts int) AcquireResult {
	t.Helper()
	return AcquireResult{Disposition: AcquireClaimed, Record: DeliveryRecord{
		Delivery: validDeliverySnapshot(t), Revision: 3, AttemptCount: attempts,
		LeaseOwner: "worker_1", LeaseEpoch: 2,
		LeaseExpiresAt: time.Date(2026, 7, 16, 15, 1, 0, 0, time.UTC),
	}}
}

func validMessage(t *testing.T) QueueMessage {
	t.Helper()
	snapshot := validDeliverySnapshot(t)
	job, err := notifications.NewDeliveryJob(snapshot.TenantID, snapshot.OutboxID, snapshot.DeliveryID,
		snapshot.EventID, snapshot.Kind, snapshot.Channel)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	return QueueMessage{MessageID: "sqs_1", ReceiptHandle: "receipt_1", Body: string(body)}
}

func validDeliverySnapshot(t *testing.T) notifications.DeliverySnapshot {
	t.Helper()
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 14, 0, 0, 0, time.UTC)
	content, err := renderer.Render(notifications.TemplateAlertOpeningV1, notifications.LocalePTBR,
		notifications.EmailTemplateData{
			RuleName: "Oxigênio baixo", Severity: "critical", TenantID: "tnt_1", PondID: "pond_1",
			DeviceID: "dev_1", Metric: "dissolved_oxygen", Unit: "mg/L", Operator: "<",
			Threshold: 4.5, EvaluationWindow: 5 * time.Minute, WindowStart: now.Add(-5 * time.Minute),
			WindowEnd: now, EvaluatedAt: now, EventID: "alert_1",
		})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := notifications.NewDeliveryID("alert_1", notifications.NotificationKindOpening,
		notifications.ChannelEmail, "user_1")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := notifications.NewPendingDelivery(notifications.DeliveryParams{
		TenantID: "tnt_1", OutboxID: "outbox_1", DeliveryID: deliveryID, EventID: "alert_1",
		RuleID: "rule_1", Kind: notifications.NotificationKindOpening, Channel: notifications.ChannelEmail,
		RecipientID: "user_1", NormalizedEmail: "owner@example.com",
		MembershipSnapshot: notifications.MembershipSnapshot{Role: "owner", Status: "active", Version: 1},
		Content:            content, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := delivery.Snapshot()
	snapshot.State = notifications.DeliveryStateProcessing
	return snapshot
}

func appendSequence(current, next string) string {
	if current == "" {
		return next
	}
	return current + "," + next
}
