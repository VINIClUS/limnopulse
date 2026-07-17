package worker

import (
	"context"
	"sync"
	"time"
)

type Handler interface {
	Handle(context.Context, QueueMessage) Decision
}

type RunSummary struct {
	Result               string          `json:"result"`
	ExitCode             int             `json:"exit_code"`
	Graceful             bool            `json:"graceful"`
	Fatal                bool            `json:"fatal"`
	DrainTimedOut        bool            `json:"drain_timed_out"`
	MessagesReceived     int             `json:"messages_received"`
	MessagesDeleted      int             `json:"messages_deleted"`
	VisibilityChanged    int             `json:"visibility_changed"`
	QueueErrors          int             `json:"queue_errors"`
	ErrorCategories      map[string]int  `json:"error_categories,omitempty"`
	Metrics              MetricsSnapshot `json:"metrics"`
	TelemetryExportError string          `json:"telemetry_export_error,omitempty"`
}

type Runner struct {
	Queue        Queue
	Handler      Handler
	Concurrency  int
	ReceiveBatch int
	ReceiveWait  time.Duration
	Visibility   time.Duration
	DrainTimeout time.Duration

	// afterSlotAcquired is a deterministic admission seam for package tests.
	afterSlotAcquired func(QueueMessage)
}

type summaryCollector struct {
	mu      sync.Mutex
	summary RunSummary
}

type runFatalState struct {
	once     sync.Once
	mu       sync.RWMutex
	category string
	signal   chan struct{}
}

func newRunFatalState() *runFatalState { return &runFatalState{signal: make(chan struct{})} }

func (state *runFatalState) record(category string) {
	if category == "" {
		category = "fatal_worker"
	}
	state.once.Do(func() {
		state.mu.Lock()
		state.category = category
		state.mu.Unlock()
		close(state.signal)
	})
}

func (state *runFatalState) current() string {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.category
}

func (runner Runner) Run(ctx context.Context) RunSummary {
	collector := &summaryCollector{summary: RunSummary{ErrorCategories: map[string]int{}}}
	if runner.Queue == nil || runner.Handler == nil || runner.Concurrency < 1 ||
		runner.ReceiveBatch < 1 || runner.ReceiveBatch > 10 || runner.ReceiveWait < 0 || runner.Visibility <= 0 {
		collector.addError("configuration")
		collector.summary.Fatal = true
		return collector.snapshot()
	}
	drainTimeout := runner.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	receiveCtx, cancelReceive := context.WithCancel(ctx)
	defer cancelReceive()
	processCtx, cancelProcess := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelProcess()
	semaphore := make(chan struct{}, runner.Concurrency)
	var inflight sync.WaitGroup
	fatal := newRunFatalState()

receiveLoop:
	for {
		select {
		case <-fatal.signal:
			break receiveLoop
		default:
		}
		messages, err := runner.Queue.Receive(receiveCtx, runner.ReceiveBatch, runner.ReceiveWait, runner.Visibility)
		if err != nil {
			if receiveCtx.Err() != nil {
				break receiveLoop
			}
			fatal.record("queue_receive_failure")
			break receiveLoop
		}
		collector.addReceived(len(messages))
		for _, message := range messages {
			if admissionStopped(receiveCtx, fatal.signal) {
				break receiveLoop
			}
			select {
			case semaphore <- struct{}{}:
			case <-fatal.signal:
				break receiveLoop
			case <-receiveCtx.Done():
				break receiveLoop
			}
			if runner.afterSlotAcquired != nil {
				runner.afterSlotAcquired(message)
			}
			if admissionStopped(receiveCtx, fatal.signal) {
				<-semaphore
				break receiveLoop
			}
			inflight.Add(1)
			go func(message QueueMessage) {
				defer inflight.Done()
				defer func() { <-semaphore }()
				decision := runner.Handler.Handle(processCtx, message)
				if decision.Action == ActionFatal {
					fatal.record(decision.ErrorCategory)
					cancelReceive()
				}
				runner.applyDecision(processCtx, collector, message, decision)
			}(message)
		}
	}
	cancelReceive()
	drained := make(chan struct{})
	go func() { inflight.Wait(); close(drained) }()
	select {
	case <-drained:
		if fatalCategory := fatal.current(); fatalCategory != "" {
			collector.addError(fatalCategory)
			collector.setFatal()
		} else {
			collector.setGraceful()
		}
	case <-time.After(drainTimeout):
		cancelProcess()
		if fatalCategory := fatal.current(); fatalCategory != "" {
			collector.addError(fatalCategory)
		}
		collector.setDrainTimedOut()
		collector.addError("drain_timeout")
		collector.setFatal()
	}
	return collector.snapshot()
}

func admissionStopped(ctx context.Context, fatal <-chan struct{}) bool {
	select {
	case <-fatal:
		return true
	default:
	}
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (runner Runner) applyDecision(
	ctx context.Context,
	collector *summaryCollector,
	message QueueMessage,
	decision Decision,
) {
	switch decision.Action {
	case ActionDelete:
		if err := runner.Queue.Delete(ctx, message); err != nil {
			collector.addQueueError("queue_delete_failure")
			return
		}
		collector.addDeleted()
	case ActionChangeVisibility, ActionFatal:
		if err := runner.Queue.ChangeVisibility(ctx, message, normalizedVisibility(decision.Visibility)); err != nil {
			collector.addQueueError("queue_visibility_failure")
			return
		}
		collector.addVisibility()
	default:
		collector.addQueueError("invalid_decision")
	}
}

func (collector *summaryCollector) addReceived(count int) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.summary.MessagesReceived += count
}
func (collector *summaryCollector) addDeleted() {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.summary.MessagesDeleted++
}
func (collector *summaryCollector) addVisibility() {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.summary.VisibilityChanged++
}
func (collector *summaryCollector) addQueueError(category string) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.summary.QueueErrors++
	collector.summary.ErrorCategories[category]++
}
func (collector *summaryCollector) addError(category string) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if category != "" {
		collector.summary.ErrorCategories[category]++
	}
}
func (collector *summaryCollector) setFatal() {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.summary.Fatal = true
	collector.summary.Graceful = false
}
func (collector *summaryCollector) setGraceful() {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.summary.Graceful = true
}
func (collector *summaryCollector) setDrainTimedOut() {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.summary.DrainTimedOut = true
}
func (collector *summaryCollector) snapshot() RunSummary {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	snapshot := collector.summary
	if snapshot.Fatal || !snapshot.Graceful {
		snapshot.Result = "fatal_failure"
		snapshot.ExitCode = 1
	} else {
		snapshot.Result = "success"
		snapshot.ExitCode = 0
	}
	snapshot.ErrorCategories = make(map[string]int, len(collector.summary.ErrorCategories))
	for category, count := range collector.summary.ErrorCategories {
		snapshot.ErrorCategories[category] = count
	}
	return snapshot
}
