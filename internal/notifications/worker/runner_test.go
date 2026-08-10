package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunnerStopsReceivingAndGracefullyDrainsInflightWork(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{MessageID: "m1", ReceiptHandle: "r1"}, {MessageID: "m2", ReceiptHandle: "r2"}})
	handler := &blockingHandler{started: make(chan struct{}, 2), release: make(chan struct{})}
	runner := Runner{Queue: queue, Handler: handler, Concurrency: 4, ReceiveBatch: 10,
		ReceiveWait: 20 * time.Second, Visibility: time.Minute, DrainTimeout: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() { done <- runner.Run(ctx) }()
	<-handler.started
	<-handler.started
	cancel()
	close(handler.release)
	summary := <-done
	if !summary.Graceful || summary.Fatal || summary.MessagesReceived != 2 || summary.MessagesDeleted != 2 {
		t.Fatalf("summary = %#v", summary)
	}
	if queue.receives.Load() > 2 {
		t.Fatalf("receives continued after cancellation: %d", queue.receives.Load())
	}
}

func TestRunnerPassesZeroReceiveWaitToQueue(t *testing.T) {
	queue := newRunnerQueue(nil)
	queue.receiveWait = make(chan time.Duration, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() {
		done <- (Runner{Queue: queue, Handler: handlerFunc(func(context.Context, QueueMessage) Decision {
			return Decision{Action: ActionDelete}
		}), Concurrency: 1, ReceiveBatch: 10, ReceiveWait: 0, Visibility: time.Minute}).Run(ctx)
	}()
	if wait := <-queue.receiveWait; wait != 0 {
		t.Fatalf("receive wait = %s", wait)
	}
	cancel()
	if summary := <-done; !summary.Graceful || summary.Fatal {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerReservesFreeCapacityBeforeReceiving(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{MessageID: "m1", ReceiptHandle: "r1"}})
	queue.receiveMax = make(chan int, 2)
	started := make(chan struct{})
	release := make(chan struct{})
	handler := handlerFunc(func(context.Context, QueueMessage) Decision {
		close(started)
		<-release
		return Decision{Action: ActionDelete}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() {
		done <- (Runner{
			Queue: queue, Handler: handler, Concurrency: 1, ReceiveBatch: 10,
			ReceiveWait: 20 * time.Second, Visibility: time.Minute,
		}).Run(ctx)
	}()

	if max := <-queue.receiveMax; max != 1 {
		t.Fatalf("first receive max = %d, want free capacity 1", max)
	}
	<-started
	select {
	case max := <-queue.receiveMax:
		t.Fatalf("second receive started without free capacity, max=%d", max)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if max := <-queue.receiveMax; max != 1 {
		t.Fatalf("second receive max = %d, want free capacity 1", max)
	}
	cancel()
	if summary := <-done; !summary.Graceful || summary.Fatal || summary.MessagesDeleted != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerFatalProducedDuringSignalDrainCannotExitSuccessfully(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{MessageID: "m1", ReceiptHandle: "r1"}})
	started := make(chan struct{})
	release := make(chan struct{})
	handler := handlerFunc(func(context.Context, QueueMessage) Decision {
		close(started)
		<-release
		return Decision{Action: ActionFatal, Visibility: 15 * time.Minute, ErrorCategory: "fatal_credentials"}
	})
	runner := Runner{Queue: queue, Handler: handler, Concurrency: 1, ReceiveBatch: 10,
		ReceiveWait: 20 * time.Second, Visibility: time.Minute, DrainTimeout: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	cancel()
	close(release)
	summary := <-done
	if !summary.Fatal || summary.Graceful || summary.ExitCode != 1 ||
		summary.ErrorCategories["fatal_credentials"] != 1 || queue.receives.Load() != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerDoesNotAdmitRemainingBatchAfterFatalSignal(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{
		{MessageID: "fatal", ReceiptHandle: "r1"},
		{MessageID: "admitted_before_fatal", ReceiptHandle: "r2"},
		{MessageID: "must_remain_at_least_once", ReceiptHandle: "r3"},
	})
	queue.changedMessages = make(chan QueueMessage, 1)
	concurrentStarted := make(chan struct{})
	releaseConcurrent := make(chan struct{})
	var admittedHandlerCalls atomic.Int32
	var unstartedHandlerCalls atomic.Int32
	handler := handlerFunc(func(_ context.Context, message QueueMessage) Decision {
		switch message.MessageID {
		case "fatal":
			<-concurrentStarted
			return Decision{Action: ActionFatal, Visibility: 15 * time.Minute, ErrorCategory: "fatal_credentials"}
		case "admitted_before_fatal":
			admittedHandlerCalls.Add(1)
			close(concurrentStarted)
			<-releaseConcurrent
			return Decision{Action: ActionDelete}
		default:
			unstartedHandlerCalls.Add(1)
			return Decision{Action: ActionDelete}
		}
	})
	runner := Runner{
		Queue: queue, Handler: handler, Concurrency: 3, ReceiveBatch: 10,
		ReceiveWait: 20 * time.Second, Visibility: time.Minute, DrainTimeout: 100 * time.Millisecond,
		afterSlotAcquired: func(message QueueMessage) {
			if message.MessageID != "must_remain_at_least_once" {
				return
			}
			changed := <-queue.changedMessages
			if changed.MessageID != "fatal" {
				t.Errorf("visibility changed for %q before fatal admission fence", changed.MessageID)
			}
			close(releaseConcurrent)
		},
	}

	summary := runner.Run(context.Background())
	if !summary.Fatal || summary.MessagesReceived != 3 || summary.VisibilityChanged != 1 ||
		summary.MessagesDeleted != 1 || admittedHandlerCalls.Load() != 1 ||
		unstartedHandlerCalls.Load() != 0 || queue.changes.Load() != 1 || queue.deletes.Load() != 1 {
		t.Fatalf("summary=%#v admitted_calls=%d unstarted_calls=%d queue_changes=%d queue_deletes=%d",
			summary, admittedHandlerCalls.Load(), unstartedHandlerCalls.Load(), queue.changes.Load(), queue.deletes.Load())
	}
}

func TestRunnerFatalStopsReceivesAfterVisibilityAndDrains(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{MessageID: "m1", ReceiptHandle: "r1"}})
	handler := handlerFunc(func(context.Context, QueueMessage) Decision {
		return Decision{Action: ActionFatal, Visibility: 15 * time.Minute, ErrorCategory: "fatal_credentials"}
	})
	runner := Runner{Queue: queue, Handler: handler, Concurrency: 4, ReceiveBatch: 10,
		ReceiveWait: 20 * time.Second, Visibility: time.Minute, DrainTimeout: 100 * time.Millisecond}
	summary := runner.Run(context.Background())
	if !summary.Fatal || summary.Graceful || summary.VisibilityChanged != 1 || summary.ErrorCategories["fatal_credentials"] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerReportsNonfatalDecisionCategory(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{MessageID: "m1", ReceiptHandle: "r1"}})
	queue.changedMessages = make(chan QueueMessage, 1)
	handler := handlerFunc(func(context.Context, QueueMessage) Decision {
		return Decision{
			Action: ActionChangeVisibility, Visibility: time.Minute,
			ErrorCategory: "attempt_start_failure",
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() {
		done <- (Runner{Queue: queue, Handler: handler, Concurrency: 1, ReceiveBatch: 10,
			ReceiveWait: 20 * time.Second, Visibility: time.Minute}).Run(ctx)
	}()
	<-queue.changedMessages
	cancel()
	summary := <-done
	if !summary.Graceful || summary.ErrorCategories["attempt_start_failure"] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerKeepsEmptyBodyOnPerMessagePoisonRedrivePath(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{
		MessageID: "poison", ReceiptHandle: "receipt", Body: "", ReceiveCount: 8,
	}})
	queue.changedMessages = make(chan QueueMessage, 1)
	handler := handlerFunc(func(_ context.Context, message QueueMessage) Decision {
		if message.Body != "" || message.ReceiveCount != 8 {
			t.Fatalf("poison message = %#v", message)
		}
		return Decision{
			Action: ActionChangeVisibility, Visibility: time.Second,
			ErrorCategory: "invalid_feedback",
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() {
		done <- (Runner{
			Queue: queue, Handler: handler, Concurrency: 1, ReceiveBatch: 10,
			ReceiveWait: 20 * time.Second, Visibility: time.Minute,
		}).Run(ctx)
	}()
	changed := <-queue.changedMessages
	if changed.MessageID != "poison" {
		t.Fatalf("visibility changed for %#v", changed)
	}
	cancel()
	summary := <-done
	if summary.Fatal || !summary.Graceful || summary.VisibilityChanged != 1 ||
		summary.ErrorCategories["invalid_feedback"] != 1 || summary.MessagesDeleted != 0 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerFatalWithoutCategoryCannotExitSuccessfully(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{MessageID: "m1", ReceiptHandle: "r1"}})
	handler := handlerFunc(func(context.Context, QueueMessage) Decision {
		return Decision{Action: ActionFatal, Visibility: time.Minute}
	})
	summary := (Runner{Queue: queue, Handler: handler, Concurrency: 1, ReceiveBatch: 10,
		ReceiveWait: 20 * time.Second, Visibility: time.Minute, DrainTimeout: 100 * time.Millisecond}).Run(context.Background())
	if !summary.Fatal || summary.ExitCode != 1 || summary.ErrorCategories["fatal_worker"] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerBoundsConcurrencyAndDrainTimeout(t *testing.T) {
	messages := make([]QueueMessage, 8)
	for index := range messages {
		messages[index] = QueueMessage{MessageID: "m", ReceiptHandle: "r"}
	}
	queue := newRunnerQueue(messages)
	handler := &countingHandler{release: make(chan struct{})}
	runner := Runner{Queue: queue, Handler: handler, Concurrency: 4, ReceiveBatch: 10,
		ReceiveWait: 20 * time.Second, Visibility: time.Minute, DrainTimeout: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() { done <- runner.Run(ctx) }()
	deadline := time.After(100 * time.Millisecond)
	for handler.maximum.Load() < 4 {
		select {
		case <-deadline:
			t.Fatal("handler did not reach configured concurrency")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	summary := <-done
	close(handler.release)
	if !summary.DrainTimedOut || handler.maximum.Load() > 4 {
		t.Fatalf("summary=%#v max=%d", summary, handler.maximum.Load())
	}
}

func TestRunnerSummaryIsFrozenWhenHandlerFinishesAfterDrainTimeout(t *testing.T) {
	queue := newRunnerQueue([]QueueMessage{{MessageID: "m1", ReceiptHandle: "r1"}})
	started := make(chan struct{})
	release := make(chan struct{})
	handler := handlerFunc(func(context.Context, QueueMessage) Decision {
		close(started)
		<-release
		return Decision{Action: ActionChangeVisibility, Visibility: time.Minute}
	})
	runner := Runner{Queue: queue, Handler: handler, Concurrency: 1, ReceiveBatch: 10,
		ReceiveWait: 20 * time.Second, Visibility: time.Minute, DrainTimeout: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan RunSummary, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	cancel()
	summary := <-done
	if !summary.DrainTimedOut {
		t.Fatalf("summary = %#v", summary)
	}
	wantVisibility := summary.VisibilityChanged
	close(release)
	time.Sleep(5 * time.Millisecond)
	if summary.VisibilityChanged != wantVisibility {
		t.Fatalf("returned summary mutated after return: %#v", summary)
	}
}

func TestRunnerQueueMutationFailuresRemainAtLeastOnceSafe(t *testing.T) {
	for _, test := range []struct {
		name                 string
		decision             Decision
		deleteErr, changeErr error
		want                 string
	}{
		{name: "terminal delete", decision: Decision{Action: ActionDelete}, deleteErr: errors.New("delete failed"), want: "queue_delete_failure"},
		{name: "durable retry visibility", decision: Decision{Action: ActionChangeVisibility, Visibility: time.Minute}, changeErr: errors.New("visibility failed"), want: "queue_visibility_failure"},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := newRunnerQueue(nil)
			queue.deleteError = test.deleteErr
			queue.changeError = test.changeErr
			collector := &summaryCollector{summary: RunSummary{ErrorCategories: map[string]int{}}}
			(Runner{Queue: queue}).applyDecision(context.Background(), collector, QueueMessage{ReceiptHandle: "r"}, test.decision)
			summary := collector.snapshot()
			if summary.QueueErrors != 1 || summary.ErrorCategories[test.want] != 1 || summary.Fatal {
				t.Fatalf("summary = %#v", summary)
			}
		})
	}
}

func TestRunnerReceiveFailureExitsAsOperationalFatal(t *testing.T) {
	queue := newRunnerQueue(nil)
	queue.receiveError = errors.New("SQS unavailable")
	summary := (Runner{Queue: queue, Handler: handlerFunc(func(context.Context, QueueMessage) Decision { return Decision{} }),
		Concurrency: 4, ReceiveBatch: 10, ReceiveWait: 20 * time.Second, Visibility: time.Minute,
		DrainTimeout: time.Millisecond}).Run(context.Background())
	if !summary.Fatal || summary.ExitCode != 1 || summary.ErrorCategories["queue_receive_failure"] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

type handlerFunc func(context.Context, QueueMessage) Decision

func (function handlerFunc) Handle(ctx context.Context, message QueueMessage) Decision {
	return function(ctx, message)
}

type blockingHandler struct {
	started chan struct{}
	release chan struct{}
}

func (handler *blockingHandler) Handle(context.Context, QueueMessage) Decision {
	handler.started <- struct{}{}
	<-handler.release
	return Decision{Action: ActionDelete}
}

type countingHandler struct {
	active  atomic.Int32
	maximum atomic.Int32
	release chan struct{}
}

func (handler *countingHandler) Handle(ctx context.Context, _ QueueMessage) Decision {
	active := handler.active.Add(1)
	for {
		maximum := handler.maximum.Load()
		if active <= maximum || handler.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-handler.release:
	case <-ctx.Done():
	}
	handler.active.Add(-1)
	return Decision{Action: ActionChangeVisibility, Visibility: time.Minute}
}

type runnerQueue struct {
	mu              sync.Mutex
	initial         []QueueMessage
	delivered       bool
	receives        atomic.Int32
	changes         atomic.Int32
	deletes         atomic.Int32
	receiveError    error
	deleteError     error
	changeError     error
	receiveWait     chan time.Duration
	receiveMax      chan int
	changedMessages chan QueueMessage
}

func newRunnerQueue(messages []QueueMessage) *runnerQueue { return &runnerQueue{initial: messages} }
func (queue *runnerQueue) Receive(ctx context.Context, max int, wait time.Duration, _ time.Duration) ([]QueueMessage, error) {
	queue.receives.Add(1)
	if queue.receiveMax != nil {
		queue.receiveMax <- max
	}
	if queue.receiveWait != nil {
		queue.receiveWait <- wait
	}
	queue.mu.Lock()
	if !queue.delivered {
		queue.delivered = true
		receiveError := queue.receiveError
		count := min(max, len(queue.initial))
		messages := append([]QueueMessage(nil), queue.initial[:count]...)
		queue.initial = queue.initial[count:]
		queue.mu.Unlock()
		return messages, receiveError
	}
	if len(queue.initial) != 0 {
		count := min(max, len(queue.initial))
		messages := append([]QueueMessage(nil), queue.initial[:count]...)
		queue.initial = queue.initial[count:]
		queue.mu.Unlock()
		return messages, nil
	}
	queue.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}
func (queue *runnerQueue) Delete(context.Context, QueueMessage) error {
	queue.deletes.Add(1)
	return queue.deleteError
}
func (queue *runnerQueue) ChangeVisibility(_ context.Context, message QueueMessage, _ time.Duration) error {
	queue.changes.Add(1)
	if queue.changedMessages != nil {
		queue.changedMessages <- message
	}
	return queue.changeError
}
