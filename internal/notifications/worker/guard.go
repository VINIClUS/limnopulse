package worker

import (
	"context"
	"errors"
	"sync"
	"time"
)

type LeaseRenewer interface {
	Renew(context.Context, DeliveryRecord, time.Time) error
}

type RenewalGuard struct {
	Store      LeaseRenewer
	Queue      Queue
	Interval   time.Duration
	LeaseTTL   time.Duration
	Visibility time.Duration
	Now        func() time.Time
}

func (guard RenewalGuard) Protect(
	ctx context.Context,
	message QueueMessage,
	record DeliveryRecord,
) (context.Context, func() error) {
	protectedCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	finished := make(chan struct{})
	var renewalErr error
	var stopOnce sync.Once
	interval := guard.Interval
	if interval <= 0 {
		interval = 20 * time.Second
	}
	leaseTTL := guard.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	visibility := guard.Visibility
	if visibility <= 0 {
		visibility = time.Minute
	}
	now := guard.Now
	if now == nil {
		now = time.Now
	}
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if guard.Store == nil || guard.Queue == nil {
					renewalErr = errors.New("renewal guard is not configured")
					cancel()
					return
				}
				renewCtx, renewCancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
				err := guard.Store.Renew(renewCtx, record, now().UTC().Add(leaseTTL))
				if err == nil {
					err = guard.Queue.ChangeVisibility(renewCtx, message, visibility)
				}
				renewCancel()
				if err != nil {
					renewalErr = err
					cancel()
					return
				}
			}
		}
	}()
	stop := func() error {
		stopOnce.Do(func() {
			close(done)
			<-finished
			cancel()
		})
		return renewalErr
	}
	return protectedCtx, stop
}

var _ LeaseGuard = RenewalGuard{}
