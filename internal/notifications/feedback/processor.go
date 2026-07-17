package feedback

import (
	"context"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
)

type Processor struct {
	Store             Store
	Now               func() time.Time
	InvalidVisibility time.Duration
	Metrics           *Metrics
}

func (processor Processor) Handle(ctx context.Context, message worker.QueueMessage) worker.Decision {
	visibility := processor.InvalidVisibility
	if visibility <= 0 {
		visibility = time.Minute
	}
	if processor.Store == nil || processor.Now == nil {
		return worker.Decision{Action: worker.ActionFatal, Visibility: visibility, ErrorCategory: "feedback_configuration"}
	}
	parsed, err := ParseEvent([]byte(message.Body))
	if err != nil {
		processor.Metrics.recordParse(ParseAwaitDLQ)
		return worker.Decision{Action: worker.ActionChangeVisibility, Visibility: visibility, ErrorCategory: "invalid_feedback"}
	}
	if parsed.Disposition == ParseIgnore {
		processor.Metrics.recordParse(ParseIgnore)
		return worker.Decision{Action: worker.ActionDelete}
	}
	mutationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	result, err := processor.Store.Reconcile(mutationCtx, parsed.Event, processor.Now().UTC())
	processor.Metrics.recordReconciliation(result, err)
	if err != nil {
		return worker.Decision{Action: worker.ActionChangeVisibility, Visibility: visibility, ErrorCategory: "feedback_persistence_failure"}
	}
	switch result.Disposition {
	case ReconcileApplied, ReconcileDuplicate:
		return worker.Decision{Action: worker.ActionDelete}
	case ReconcileAwaitDLQ:
		return worker.Decision{Action: worker.ActionChangeVisibility, Visibility: visibility, ErrorCategory: "feedback_unknown_association"}
	default:
		return worker.Decision{Action: worker.ActionChangeVisibility, Visibility: visibility, ErrorCategory: "feedback_invalid_reconciliation"}
	}
}

var _ worker.Handler = Processor{}
