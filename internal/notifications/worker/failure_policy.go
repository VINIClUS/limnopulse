package worker

import (
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

type definitiveFailurePlan struct {
	NextState            notifications.DeliveryState
	NextAttemptAt        time.Time
	PossiblyAccepted     bool
	AmbiguousExhausted   bool
	AwaitingIntervention bool
	Action               DecisionAction
	Visibility           time.Duration
}

func (processor Processor) planDefinitiveFailure(
	record DeliveryRecord,
	category SendErrorCategory,
	now time.Time,
) (definitiveFailurePlan, bool) {
	jitterFraction := 0.0
	if category.disposition() == sendPermanent && record.PossiblyAccepted {
		jitterFraction = processor.JitterFraction()
	}
	return planDefinitiveFailure(record, category, now, jitterFraction)
}

func planDefinitiveFailure(
	record DeliveryRecord,
	category SendErrorCategory,
	now time.Time,
	jitterFraction float64,
) (definitiveFailurePlan, bool) {
	switch category.disposition() {
	case sendPermanent:
		if !record.PossiblyAccepted {
			return definitiveFailurePlan{
				NextState: notifications.DeliveryStatePermanentFailed,
				Action:    ActionDelete,
			}, true
		}
		delay := RetryDelay(record.AttemptCount, true, jitterFraction)
		return definitiveFailurePlan{
			NextState:          notifications.DeliveryStateRetryableFailed,
			NextAttemptAt:      now.Add(delay),
			PossiblyAccepted:   true,
			AmbiguousExhausted: true,
			Action:             ActionChangeVisibility,
			Visibility:         normalizedVisibility(delay),
		}, true
	case sendFatal:
		return definitiveFailurePlan{
			NextState:            notifications.DeliveryStateRetryableFailed,
			NextAttemptAt:        now.Add(15 * time.Minute),
			PossiblyAccepted:     record.PossiblyAccepted,
			AwaitingIntervention: true,
			Action:               ActionFatal,
			Visibility:           15 * time.Minute,
		}, true
	default:
		return definitiveFailurePlan{}, false
	}
}
