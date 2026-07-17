package feedback

import (
	"sync/atomic"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
)

type MetricsSnapshot = worker.FeedbackMetricsSnapshot

type Metrics struct {
	applied           atomic.Int64
	duplicates        atomic.Int64
	ignored           atomic.Int64
	malformed         atomic.Int64
	awaitingDLQ       atomic.Int64
	persistenceErrors atomic.Int64
	suppressed        atomic.Int64
}

func NewMetrics() *Metrics { return &Metrics{} }

func (metrics *Metrics) recordParse(disposition ParseDisposition) {
	if metrics == nil {
		return
	}
	switch disposition {
	case ParseIgnore:
		metrics.ignored.Add(1)
	case ParseAwaitDLQ:
		metrics.malformed.Add(1)
	}
}

func (metrics *Metrics) recordReconciliation(result ReconcileResult, err error) {
	if metrics == nil {
		return
	}
	if err != nil {
		metrics.persistenceErrors.Add(1)
		return
	}
	switch result.Disposition {
	case ReconcileApplied:
		metrics.applied.Add(1)
		if result.Suppressed {
			metrics.suppressed.Add(1)
		}
	case ReconcileDuplicate:
		metrics.duplicates.Add(1)
	case ReconcileAwaitDLQ:
		metrics.awaitingDLQ.Add(1)
	}
}

func (metrics *Metrics) Snapshot() MetricsSnapshot {
	if metrics == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		Applied: metrics.applied.Load(), Duplicates: metrics.duplicates.Load(),
		Ignored: metrics.ignored.Load(), Malformed: metrics.malformed.Load(),
		AwaitingDLQ: metrics.awaitingDLQ.Load(), PersistenceErrors: metrics.persistenceErrors.Load(),
		Suppressed: metrics.suppressed.Load(),
	}
}
