package feedback

import (
	"context"
	"time"
)

type ReconcileDisposition uint8

const (
	ReconcileApplied ReconcileDisposition = iota + 1
	ReconcileDuplicate
	ReconcileAwaitDLQ
)

type ReconcileResult struct {
	Disposition ReconcileDisposition
	Suppressed  bool
}

type Store interface {
	Reconcile(context.Context, Event, time.Time) (ReconcileResult, error)
}
