package relay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
)

type fakeStore struct {
	mu      sync.Mutex
	queries []DueRequest
	pages   map[int][]DuePage
}

func (store *fakeStore) QueryDue(_ context.Context, request DueRequest) (DuePage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.queries = append(store.queries, request)
	pages := store.pages[request.Bucket]
	if len(pages) == 0 {
		return DuePage{}, nil
	}
	page := pages[0]
	store.pages[request.Bucket] = pages[1:]
	return page, nil
}

func (*fakeStore) Reload(context.Context, Candidate, time.Time) (Work, bool, error) {
	return Work{}, false, nil
}

func (*fakeStore) Claim(context.Context, Work, LeaseRequest) (Work, bool, error) {
	return Work{}, false, nil
}

func (*fakeStore) ExpandIntent(context.Context, Work, ExpandRequest) (WorkResult, error) {
	return WorkResult{}, nil
}

func (*fakeStore) ExpandDependency(context.Context, Work, ExpandRequest) (WorkResult, error) {
	return WorkResult{}, nil
}

func (*fakeStore) MarkQueued(context.Context, Work, QueuedResult) error { return nil }

func (*fakeStore) Reschedule(context.Context, Work, time.Time) error { return nil }

type fakePublisher struct{}

func (fakePublisher) Publish(context.Context, PublishRequest) (string, error) {
	return "message-id", nil
}

func TestRunQueriesEachOwnedBucketOnceAndTerminatesWithoutPolling(t *testing.T) {
	store := &fakeStore{pages: make(map[int][]DuePage)}
	start := time.Date(2026, 7, 16, 12, 34, 56, 0, time.UTC)
	runner := Runner{
		Store:     store,
		Publisher: fakePublisher{},
		Clock:     func() time.Time { return start },
		IDFactory: func() string { return "run_test" },
	}
	config := relayconfig.RunConfig{
		ShardCount: 1, QueryParallelism: 4, WorkParallelism: 8, MaxWork: 250,
		FanoutPageSize: 20, GlobalDeadline: 45 * time.Second,
		SoftDeadline: 40 * time.Second, ItemTimeout: 10 * time.Second,
		LeaseTTL: 20 * time.Second,
	}

	summary := runner.Run(context.Background(), config)

	if summary.ExitCode != ExitSuccess || summary.Result != "success" || !summary.ScopeCompleted {
		t.Fatalf("summary = %#v", summary)
	}
	if !summary.RelayTime.Equal(start) || summary.RunID != "run_test" {
		t.Fatalf("captured run identity = %#v", summary)
	}
	if len(store.queries) != 64 {
		t.Fatalf("QueryDue calls = %d, want exactly one per owned bucket", len(store.queries))
	}
	seen := make(map[int]int)
	for _, request := range store.queries {
		seen[request.Bucket]++
		if !request.DueThrough.Equal(start) || request.PageSize < 1 || request.NextToken != "" {
			t.Fatalf("query request = %#v", request)
		}
	}
	for bucket := 0; bucket < 64; bucket++ {
		if seen[bucket] != 1 {
			t.Fatalf("bucket %d query count = %d", bucket, seen[bucket])
		}
	}
}

func TestRunConsumesSetupTimeFromGlobalAndSoftDeadlineBudgets(t *testing.T) {
	start := time.Date(2026, 7, 16, 12, 34, 56, 0, time.UTC)
	budgetStartedAt := start.Add(-40 * time.Second)
	store := &fakeStore{pages: make(map[int][]DuePage)}
	runner := Runner{
		Store: store, Publisher: fakePublisher{}, Clock: func() time.Time { return start },
		IDFactory: func() string { return "run_test" },
	}
	config := relayconfig.RunConfig{
		BudgetStartedAt: &budgetStartedAt, ShardCount: 1, QueryParallelism: 4,
		WorkParallelism: 8, MaxWork: 250, FanoutPageSize: 20,
		GlobalDeadline: 45 * time.Second, SoftDeadline: 40 * time.Second,
		ItemTimeout: 10 * time.Second, LeaseTTL: 20 * time.Second,
	}

	summary := runner.Run(context.Background(), config)
	if summary.ExitCode != ExitPartial || !summary.DeadlineReached || summary.WorkRemaining != 1 ||
		len(store.queries) != 0 || summary.Duration != 40*time.Second {
		t.Fatalf("summary = %#v, queries = %#v", summary, store.queries)
	}
}

type publicationStore struct {
	mu               sync.Mutex
	candidate        Candidate
	work             Work
	served           bool
	reloads          int
	claims           int
	claimRequests    []LeaseRequest
	queued           []QueuedResult
	rescheduled      []time.Time
	queryCount       int
	onQuery          func(int)
	dependencyResult WorkResult
	dependencyErr    error
	markQueuedErr    error
}

func (store *publicationStore) QueryDue(_ context.Context, request DueRequest) (DuePage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.queryCount++
	if store.onQuery != nil {
		store.onQuery(store.queryCount)
	}
	if request.Bucket == store.candidateBucket() && !store.served {
		store.served = true
		return DuePage{Candidates: []Candidate{store.candidate}}, nil
	}
	return DuePage{}, nil
}

func TestRunPaginatesBucketsAndRotatesTheFirstBucketDeterministically(t *testing.T) {
	start := time.Date(2026, 7, 16, 13, 7, 0, 0, time.UTC)
	buckets, err := notifications.OwnedRelayBuckets(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	first := rotateBuckets(buckets, start)[0]
	store := &fakeStore{pages: map[int][]DuePage{
		first: {{NextToken: "page_2"}, {}},
	}}
	runner := Runner{
		Store: store, Publisher: fakePublisher{}, Clock: func() time.Time { return start },
		IDFactory: func() string { return "run_1" },
	}
	config := relayconfig.RunConfig{
		ShardCount: 1, QueryParallelism: 1, WorkParallelism: 1, MaxWork: 250,
		FanoutPageSize: 20, GlobalDeadline: 45 * time.Second,
		SoftDeadline: 40 * time.Second, ItemTimeout: 10 * time.Second, LeaseTTL: 20 * time.Second,
	}

	summary := runner.Run(context.Background(), config)
	if summary.ExitCode != ExitSuccess {
		t.Fatalf("summary = %#v", summary)
	}
	if len(store.queries) != 65 || store.queries[0].Bucket != first ||
		store.queries[0].NextToken != "" || store.queries[64].Bucket != first ||
		store.queries[64].NextToken != "page_2" {
		t.Fatalf("paginated query order = %#v", store.queries)
	}
}

func TestRunStopsBeforeReloadOrLeaseAtSoftDeadline(t *testing.T) {
	start := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	deliveryID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, "tnt_1", deliveryID, start.Add(-time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{
		PK: "NOTIFICATION_OUTBOX#outbox_1", SK: "DELIVERY#" + deliveryID,
		RelayPK: index.PartitionKey, RelaySK: index.SortKey,
		Kind: notifications.WorkKindDelivery, AvailableAt: start.Add(-time.Minute),
	}
	var deadline atomic.Bool
	store := &publicationStore{candidate: candidate, work: Work{Candidate: candidate}}
	store.onQuery = func(count int) {
		if count == notifications.RelayBucketCount {
			deadline.Store(true)
		}
	}
	clock := func() time.Time {
		if deadline.Load() {
			return start.Add(40 * time.Second)
		}
		return start
	}
	runner := Runner{Store: store, Publisher: &publicationPublisher{}, Clock: clock}
	config := relayconfig.RunConfig{
		ShardCount: 1, QueryParallelism: 1, WorkParallelism: 1, MaxWork: 250,
		FanoutPageSize: 20, GlobalDeadline: 45 * time.Second,
		SoftDeadline: 40 * time.Second, ItemTimeout: 10 * time.Second, LeaseTTL: 20 * time.Second,
	}

	summary := runner.Run(context.Background(), config)
	if summary.ExitCode != ExitPartial || !summary.DeadlineReached || summary.WorkRemaining != 1 ||
		store.reloads != 0 || store.claims != 0 {
		t.Fatalf("summary = %#v, reloads = %d, claims = %d", summary, store.reloads, store.claims)
	}
}

func TestRunNeverClaimsWithLeaseTimestampAtOrAfterSoftDeadline(t *testing.T) {
	start := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	deliveryID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, "tnt_1", deliveryID, start.Add(-time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{
		PK: "NOTIFICATION_OUTBOX#outbox_1", SK: "DELIVERY#" + deliveryID,
		RelayPK: index.PartitionKey, RelaySK: index.SortKey,
		Kind: notifications.WorkKindDelivery, AvailableAt: start.Add(-time.Minute),
	}
	store := &publicationStore{candidate: candidate, work: Work{Candidate: candidate}}
	var clockCalls atomic.Int32
	clock := func() time.Time {
		if clockCalls.Add(1) >= 5 {
			return start.Add(40 * time.Second)
		}
		return start
	}
	runner := Runner{
		Store: store, Publisher: &publicationPublisher{}, Clock: clock,
		IDFactory: func() string { return "run_1" },
	}
	config := relayconfig.RunConfig{
		Shard: index.Bucket, ShardCount: notifications.RelayBucketCount,
		QueryParallelism: 1, WorkParallelism: 1, MaxWork: 250, FanoutPageSize: 20,
		GlobalDeadline: 45 * time.Second, SoftDeadline: 40 * time.Second,
		ItemTimeout: 10 * time.Second, LeaseTTL: 20 * time.Second,
	}

	summary := runner.Run(context.Background(), config)
	if store.reloads != 1 || store.claims != 1 || len(store.claimRequests) != 1 ||
		!store.claimRequests[0].Now.Equal(start) || !store.claimRequests[0].Now.Before(start.Add(40*time.Second)) {
		t.Fatalf("summary = %#v, reloads = %d, claims = %d", summary, store.reloads, store.claims)
	}
}

func TestCandidatePriorityFavorsDeliveryWithoutStarvingIntentOrDependency(t *testing.T) {
	candidates := make([]Candidate, 0, 12)
	for index := range 10 {
		candidates = append(candidates, Candidate{SK: string(rune('a' + index)), Kind: notifications.WorkKindDelivery})
	}
	candidates = append(candidates,
		Candidate{SK: "intent", Kind: notifications.WorkKindIntent},
		Candidate{SK: "dependency", Kind: notifications.WorkKindDependency},
	)
	ordered := prioritizeCandidates(candidates)
	wantKinds := []notifications.WorkKind{
		notifications.WorkKindDelivery, notifications.WorkKindDelivery,
		notifications.WorkKindIntent, notifications.WorkKindDelivery,
		notifications.WorkKindDelivery, notifications.WorkKindDependency,
	}
	for index, want := range wantKinds {
		if ordered[index].Kind != want {
			t.Fatalf("priority at %d = %s, want %s; order = %#v", index, ordered[index].Kind, want, ordered)
		}
	}
}

func (store *publicationStore) candidateBucket() int {
	return int(store.candidate.RelayPK[len(store.candidate.RelayPK)-2]-'0')*10 +
		int(store.candidate.RelayPK[len(store.candidate.RelayPK)-1]-'0')
}

func (store *publicationStore) Reload(_ context.Context, candidate Candidate, _ time.Time) (Work, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.reloads++
	if candidate != store.candidate {
		return Work{}, false, errors.New("unexpected candidate")
	}
	return store.work, true, nil
}

func (store *publicationStore) Claim(_ context.Context, work Work, lease LeaseRequest) (Work, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.claims++
	store.claimRequests = append(store.claimRequests, lease)
	work.LeaseOwner = lease.Owner
	work.LeaseEpoch = 2
	return work, true, nil
}

func (*publicationStore) ExpandIntent(context.Context, Work, ExpandRequest) (WorkResult, error) {
	return WorkResult{}, errors.New("unexpected intent")
}

func (store *publicationStore) ExpandDependency(context.Context, Work, ExpandRequest) (WorkResult, error) {
	return store.dependencyResult, store.dependencyErr
}

func (store *publicationStore) MarkQueued(_ context.Context, _ Work, result QueuedResult) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.queued = append(store.queued, result)
	return store.markQueuedErr
}

func (store *publicationStore) Reschedule(_ context.Context, _ Work, next time.Time) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.rescheduled = append(store.rescheduled, next)
	return nil
}

type publicationPublisher struct {
	requests []PublishRequest
	message  string
	err      error
}

func (publisher *publicationPublisher) Publish(_ context.Context, request PublishRequest) (string, error) {
	publisher.requests = append(publisher.requests, request)
	return publisher.message, publisher.err
}

func TestRunConfirmsQueuedOnlyAfterSQSConfirmationAndReindexesAmbiguity(t *testing.T) {
	start := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	deliveryID, err := notifications.NewDeliveryID(
		"event_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, "tnt_1", deliveryID, start.Add(-time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{
		PK: "NOTIFICATION_OUTBOX#outbox_1", SK: "DELIVERY#" + deliveryID,
		RelayPK: index.PartitionKey, RelaySK: index.SortKey,
		Kind: notifications.WorkKindDelivery, AvailableAt: start.Add(-time.Minute),
	}
	work := Work{
		Candidate: candidate, TenantID: "tnt_1", ItemID: deliveryID,
		OutboxID: "outbox_1", DeliveryID: deliveryID, EventID: "event_1", RuleID: "rule_1",
		NotificationKind: notifications.NotificationKindOpening, Channel: notifications.ChannelEmail,
		State: "pending", Revision: 1,
	}
	tests := []struct {
		name              string
		message           string
		publishErr        error
		markQueuedErr     error
		wantExit          int
		wantQueued        int
		wantRescheduled   int
		wantPublishedJobs int
		wantSQSErrors     int
	}{
		{name: "confirmed", message: "message_1", wantExit: ExitSuccess, wantQueued: 1, wantPublishedJobs: 1},
		{name: "SQS success then Dynamo ambiguity", message: "message_1", markQueuedErr: errors.New("commit unconfirmed"), wantExit: ExitPartial, wantQueued: 1, wantPublishedJobs: 1},
		{name: "ambiguous", publishErr: errors.New("unconfirmed"), wantExit: ExitPartial, wantRescheduled: 1, wantSQSErrors: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &publicationStore{candidate: candidate, work: work, markQueuedErr: test.markQueuedErr}
			publisher := &publicationPublisher{message: test.message, err: test.publishErr}
			runner := Runner{
				Store: store, Publisher: publisher, Clock: func() time.Time { return start },
				IDFactory: func() string { return "run_1" },
			}
			config := relayconfig.RunConfig{
				ShardCount: 1, QueryParallelism: 4, WorkParallelism: 1, MaxWork: 250,
				FanoutPageSize: 20, GlobalDeadline: 45 * time.Second,
				SoftDeadline: 40 * time.Second, ItemTimeout: 10 * time.Second, LeaseTTL: 20 * time.Second,
			}

			summary := runner.Run(context.Background(), config)
			if summary.ExitCode != test.wantExit || len(store.queued) != test.wantQueued ||
				len(store.rescheduled) != test.wantRescheduled || summary.PublishedJobs != test.wantPublishedJobs ||
				summary.SQSErrors != test.wantSQSErrors || len(publisher.requests) != 1 {
				t.Fatalf("summary = %#v, queued = %#v, rescheduled = %#v, published = %#v",
					summary, store.queued, store.rescheduled, publisher.requests)
			}
			if test.wantRescheduled == 1 && !store.rescheduled[0].Equal(start.Add(time.Minute)) {
				t.Fatalf("ambiguity rescheduled at %s", store.rescheduled[0])
			}
			if len(publisher.requests) == 1 && publisher.requests[0].Job.DeliveryID != deliveryID {
				t.Fatalf("job = %#v", publisher.requests[0].Job)
			}
		})
	}
}

func TestRunTreatsExpectedDependencyWaitAsHandledWork(t *testing.T) {
	start := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	next := start.Add(time.Minute)
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDependency, "tnt_1", "recovery_outbox", start.Add(-time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{
		PK: "TENANT#tnt_1", SK: "NOTIFICATION_OUTBOX#recovery_outbox",
		RelayPK: index.PartitionKey, RelaySK: index.SortKey,
		Kind: notifications.WorkKindDependency, AvailableAt: start.Add(-time.Minute),
	}
	work := Work{
		Candidate: candidate, TenantID: "tnt_1", ItemID: "recovery_outbox",
		OutboxID: "recovery_outbox", EventID: "event_1", RuleID: "rule_1",
		DependsOnOutboxID: "opening_outbox", NotificationKind: notifications.NotificationKindRecovery,
		Channel: notifications.ChannelEmail, State: "pending",
	}
	store := &publicationStore{
		candidate: candidate, work: work, dependencyErr: &RetryAtError{At: next},
	}
	runner := Runner{
		Store: store, Publisher: &publicationPublisher{}, Clock: func() time.Time { return start },
		IDFactory: func() string { return "run_1" },
	}
	config := relayconfig.RunConfig{
		ShardCount: 1, QueryParallelism: 4, WorkParallelism: 1, MaxWork: 250,
		FanoutPageSize: 20, GlobalDeadline: 45 * time.Second,
		SoftDeadline: 40 * time.Second, ItemTimeout: 10 * time.Second, LeaseTTL: 20 * time.Second,
	}

	summary := runner.Run(context.Background(), config)
	if summary.ExitCode != ExitSuccess || summary.WorkErrors != 0 ||
		summary.WorkProcessed != 1 || summary.DependenciesProcessed != 1 ||
		len(store.rescheduled) != 1 || !store.rescheduled[0].Equal(next) {
		t.Fatalf("summary = %#v, rescheduled = %#v", summary, store.rescheduled)
	}
}
