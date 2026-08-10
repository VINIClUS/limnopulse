package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRenewalGuardExtendsDynamoAndSQSBeforeExpiry(t *testing.T) {
	store := &renewStore{}
	queue := &renewQueue{}
	guard := RenewalGuard{
		Store: store, Queue: queue, Interval: time.Millisecond, LeaseTTL: time.Minute,
		Visibility: time.Minute, Now: time.Now,
	}
	ctx, stop := guard.Protect(context.Background(), QueueMessage{ReceiptHandle: "receipt_1"}, DeliveryRecord{
		Revision: 3, LeaseOwner: "worker_1", LeaseEpoch: 2,
	})
	deadline := time.After(100 * time.Millisecond)
	for store.count() == 0 || queue.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("guard did not renew both leases")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("protected context error = %v", ctx.Err())
	}
}

func TestRenewalGuardCancelsWhenEitherRenewalFails(t *testing.T) {
	for _, test := range []struct {
		name     string
		storeErr error
		queueErr error
	}{
		{"Dynamo", errors.New("lease lost"), nil},
		{"SQS", nil, errors.New("visibility lost")},
	} {
		t.Run(test.name, func(t *testing.T) {
			guard := RenewalGuard{Store: &renewStore{err: test.storeErr}, Queue: &renewQueue{err: test.queueErr},
				Interval: time.Millisecond, LeaseTTL: time.Minute, Visibility: time.Minute, Now: time.Now}
			ctx, stop := guard.Protect(context.Background(), QueueMessage{ReceiptHandle: "receipt"}, DeliveryRecord{
				Revision: 3, LeaseOwner: "worker", LeaseEpoch: 1,
			})
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
				t.Fatal("renewal failure did not cancel work")
			}
			if err := stop(); err == nil {
				t.Fatal("stop error = nil")
			}
		})
	}
}

type renewStore struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (store *renewStore) Renew(context.Context, DeliveryRecord, time.Time) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	return store.err
}
func (store *renewStore) count() int { store.mu.Lock(); defer store.mu.Unlock(); return store.calls }

type renewQueue struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (queue *renewQueue) Receive(context.Context, int, time.Duration, time.Duration) ([]QueueMessage, error) {
	return nil, nil
}
func (queue *renewQueue) Delete(context.Context, QueueMessage) error { return nil }
func (queue *renewQueue) ChangeVisibility(context.Context, QueueMessage, time.Duration) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.calls++
	return queue.err
}
func (queue *renewQueue) count() int { queue.mu.Lock(); defer queue.mu.Unlock(); return queue.calls }
