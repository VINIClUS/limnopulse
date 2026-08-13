package worker

import (
	"sync/atomic"
	"time"
)

type MetricsSnapshot struct {
	Attempts             int64   `json:"attempts"`
	Ambiguous            int64   `json:"ambiguous"`
	Retries              int64   `json:"retries"`
	PossibleDuplicates   int64   `json:"possible_duplicates"`
	Unknown              int64   `json:"unknown"`
	SucceededAfterRetry  int64   `json:"succeeded_after_retry"`
	LimiterWaitNanos     int64   `json:"limiter_wait_nanoseconds"`
	LimiterWaiters       int64   `json:"limiter_waiters"`
	SendStarted          int64   `json:"send_started"`
	Throttling           int64   `json:"throttling"`
	Quota                int64   `json:"quota"`
	ConfiguredRate       float64 `json:"configured_rate"`
	ActiveConcurrency    int64   `json:"active_concurrency"`
	MaxActiveConcurrency int64   `json:"max_active_concurrency"`
}

type Metrics struct {
	configuredRate       float64
	observer             MetricsObserver
	attempts             atomic.Int64
	ambiguous            atomic.Int64
	retries              atomic.Int64
	possibleDuplicates   atomic.Int64
	unknown              atomic.Int64
	succeededAfterRetry  atomic.Int64
	limiterWaitNanos     atomic.Int64
	limiterWaiters       atomic.Int64
	sendStarted          atomic.Int64
	throttling           atomic.Int64
	quota                atomic.Int64
	activeConcurrency    atomic.Int64
	maxActiveConcurrency atomic.Int64
}

type MetricsObserver interface {
	ConfiguredRate(float64)
	LimiterWaitStarted()
	LimiterWaitFinished(time.Duration)
	ProviderCallStarted(bool)
	ProviderCallFinished()
	Retry(SendErrorCategory, bool)
	SucceededAfterRetry()
	Unknown()
}

func NewMetrics(configuredRate float64) *Metrics { return NewMetricsWithObserver(configuredRate, nil) }

func NewMetricsWithObserver(configuredRate float64, observer MetricsObserver) *Metrics {
	metrics := &Metrics{configuredRate: configuredRate, observer: observer}
	if observer != nil {
		observer.ConfiguredRate(configuredRate)
	}
	return metrics
}

func (metrics *Metrics) BeginLimiterWait() func() {
	if metrics == nil {
		return func() {}
	}
	started := time.Now()
	metrics.limiterWaiters.Add(1)
	if metrics.observer != nil {
		metrics.observer.LimiterWaitStarted()
	}
	return func() {
		waited := time.Since(started)
		metrics.limiterWaitNanos.Add(waited.Nanoseconds())
		metrics.limiterWaiters.Add(-1)
		if metrics.observer != nil {
			metrics.observer.LimiterWaitFinished(waited)
		}
	}
}

func (metrics *Metrics) ProviderCallStarted(possibleDuplicate bool) {
	if metrics == nil {
		return
	}
	metrics.attempts.Add(1)
	metrics.sendStarted.Add(1)
	if possibleDuplicate {
		metrics.possibleDuplicates.Add(1)
	}
	if metrics.observer != nil {
		metrics.observer.ProviderCallStarted(possibleDuplicate)
	}
	active := metrics.activeConcurrency.Add(1)
	for {
		maximum := metrics.maxActiveConcurrency.Load()
		if active <= maximum || metrics.maxActiveConcurrency.CompareAndSwap(maximum, active) {
			break
		}
	}
}

func (metrics *Metrics) ProviderCallFinished() {
	if metrics != nil {
		metrics.activeConcurrency.Add(-1)
		if metrics.observer != nil {
			metrics.observer.ProviderCallFinished()
		}
	}
}

func (metrics *Metrics) RecordRetry(category SendErrorCategory, ambiguous bool) {
	if metrics == nil {
		return
	}
	metrics.retries.Add(1)
	if ambiguous {
		metrics.ambiguous.Add(1)
	}
	if category == ErrorRetryableThrottling || category == ErrorTelegramRateLimited {
		metrics.throttling.Add(1)
	}
	if category == ErrorRetryableQuota {
		metrics.quota.Add(1)
	}
	if metrics.observer != nil {
		metrics.observer.Retry(category, ambiguous)
	}
}

func (metrics *Metrics) RecordSucceeded(afterRetry bool) {
	if metrics != nil && afterRetry {
		metrics.succeededAfterRetry.Add(1)
		if metrics.observer != nil {
			metrics.observer.SucceededAfterRetry()
		}
	}
}

func (metrics *Metrics) RecordUnknown() {
	if metrics != nil {
		metrics.unknown.Add(1)
		if metrics.observer != nil {
			metrics.observer.Unknown()
		}
	}
}

func (metrics *Metrics) Snapshot() MetricsSnapshot {
	if metrics == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		Attempts: metrics.attempts.Load(), Ambiguous: metrics.ambiguous.Load(),
		Retries: metrics.retries.Load(), PossibleDuplicates: metrics.possibleDuplicates.Load(),
		Unknown: metrics.unknown.Load(), SucceededAfterRetry: metrics.succeededAfterRetry.Load(),
		LimiterWaitNanos: metrics.limiterWaitNanos.Load(), LimiterWaiters: metrics.limiterWaiters.Load(),
		SendStarted: metrics.sendStarted.Load(), Throttling: metrics.throttling.Load(), Quota: metrics.quota.Load(),
		ConfiguredRate: metrics.configuredRate, ActiveConcurrency: metrics.activeConcurrency.Load(),
		MaxActiveConcurrency: metrics.maxActiveConcurrency.Load(),
	}
}
