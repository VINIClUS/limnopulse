package feedback

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
)

func TestProcessorDeletesOnlyDurablyAppliedDuplicateOrIgnoredFeedback(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		body         string
		result       ReconcileResult
		storeErr     error
		wantAction   worker.DecisionAction
		wantCategory string
		wantCalls    int
	}{
		{name: "applied", body: eventJSON("Send", ""), result: ReconcileResult{Disposition: ReconcileApplied}, wantAction: worker.ActionDelete, wantCalls: 1},
		{name: "semantic duplicate", body: eventJSON("Delivery", ""), result: ReconcileResult{Disposition: ReconcileDuplicate}, wantAction: worker.ActionDelete, wantCalls: 1},
		{name: "ignored open", body: eventJSON("Open", ""), wantAction: worker.ActionDelete},
		{name: "unknown association", body: eventJSON("Send", ""), result: ReconcileResult{Disposition: ReconcileAwaitDLQ}, wantAction: worker.ActionChangeVisibility, wantCategory: "feedback_unknown_association", wantCalls: 1},
		{name: "persistence failure", body: eventJSON("Send", ""), storeErr: errors.New("private database error"), wantAction: worker.ActionChangeVisibility, wantCategory: "feedback_persistence_failure", wantCalls: 1},
		{name: "malformed", body: `{}`, wantAction: worker.ActionChangeVisibility, wantCategory: "invalid_feedback"},
		{name: "empty poison body", body: "", wantAction: worker.ActionChangeVisibility, wantCategory: "invalid_feedback"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &recordingStore{result: test.result, err: test.storeErr}
			metrics := NewMetrics()
			decision := (Processor{Store: store, Now: func() time.Time { return now }, Metrics: metrics}).Handle(
				context.Background(), worker.QueueMessage{MessageID: "sqs_1", Body: test.body},
			)
			if decision.Action != test.wantAction || decision.ErrorCategory != test.wantCategory || store.calls != test.wantCalls {
				t.Fatalf("decision=%#v calls=%d", decision, store.calls)
			}
			if decision.Action == worker.ActionChangeVisibility && decision.Visibility != time.Minute {
				t.Fatalf("visibility = %s", decision.Visibility)
			}
		})
	}
}

func TestProcessorFeedbackMetricsContainOnlyBoundedCounters(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	store := &recordingStore{result: ReconcileResult{
		Disposition: ReconcileApplied, Suppressed: true,
	}}
	metrics := NewMetrics()
	processor := Processor{Store: store, Now: func() time.Time { return now }, Metrics: metrics}
	processor.Handle(context.Background(), worker.QueueMessage{Body: eventJSON("Complaint", `,"complaint":{}`)})
	processor.Handle(context.Background(), worker.QueueMessage{Body: eventJSON("Open", "")})
	processor.Handle(context.Background(), worker.QueueMessage{Body: `{}`})
	snapshot := metrics.Snapshot()
	if snapshot.Applied != 1 || snapshot.Suppressed != 1 || snapshot.Ignored != 1 || snapshot.Malformed != 1 {
		t.Fatalf("metrics = %#v", snapshot)
	}
}

type recordingStore struct {
	result ReconcileResult
	err    error
	calls  int
}

func (store *recordingStore) Reconcile(context.Context, Event, time.Time) (ReconcileResult, error) {
	store.calls++
	return store.result, store.err
}
