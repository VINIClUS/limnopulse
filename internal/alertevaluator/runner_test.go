package alertevaluator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu         sync.Mutex
	candidates []Candidate
	work       map[string]Work
	states     map[string]VersionedState
	commits    []CommitRequest
	queryErr   error
	commitErr  error
}

func (store *fakeStore) QueryDue(_ context.Context, request DueRequest) (DuePage, error) {
	if store.queryErr != nil {
		return DuePage{}, store.queryErr
	}
	var items []Candidate
	for _, candidate := range store.candidates {
		if candidate.Bucket == request.Bucket {
			items = append(items, candidate)
		}
	}
	return DuePage{Candidates: items}, nil
}

func (store *fakeStore) Claim(_ context.Context, candidate Candidate, _ LeaseRequest) (Work, error) {
	work, ok := store.work[candidate.RuleID]
	if !ok {
		return Work{}, ErrLeaseConflict
	}
	return work, nil
}

func (store *fakeStore) LoadState(_ context.Context, work Work) (VersionedState, error) {
	return store.states[work.Rule.RuleID], nil
}

func (store *fakeStore) Commit(_ context.Context, request CommitRequest) error {
	if store.commitErr != nil {
		return store.commitErr
	}
	store.mu.Lock()
	store.commits = append(store.commits, request)
	store.mu.Unlock()
	return nil
}

type concurrencyStore struct {
	fakeStore
	queryStarted chan struct{}
	queryRelease chan struct{}
}

func (store *concurrencyStore) QueryDue(ctx context.Context, request DueRequest) (DuePage, error) {
	store.queryStarted <- struct{}{}
	select {
	case <-store.queryRelease:
		return DuePage{}, nil
	case <-ctx.Done():
		return DuePage{}, ctx.Err()
	}
}

func TestRunnerLimitsConcurrentEvaluationQueries(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	store := &concurrencyStore{
		queryStarted: make(chan struct{}, EvaluationBucketCount), queryRelease: make(chan struct{}),
	}
	config := runnableConfig(evaluationTime)
	config.QueryParallelism = 3
	done := make(chan RunSummary, 1)
	go func() {
		done <- (Runner{Store: store, Windows: fakeWindowReader{}}).Run(context.Background(), config)
	}()
	for range 3 {
		<-store.queryStarted
	}
	select {
	case <-store.queryStarted:
		t.Fatal("more than query-parallelism queries started")
	case <-time.After(25 * time.Millisecond):
	}
	close(store.queryRelease)
	if summary := <-done; summary.ExitCode != ExitSuccess {
		t.Fatalf("summary = %#v", summary)
	}
}

type fakeWindowReader struct {
	result WindowResult
	err    error
}

func (reader fakeWindowReader) Evaluate(_ context.Context, _ EvaluationRule, _ WindowSpec) (WindowResult, error) {
	return reader.result, reader.err
}

func runnableConfig(evaluationTime time.Time) RunConfig {
	config := defaultRunConfig()
	config.EvaluationTime = &evaluationTime
	config.Shard = 0
	config.ShardCount = 1
	return config
}

func runnableWork(slot time.Time) Work {
	return Work{
		PK: "TENANT#tnt_1", SK: "ALERT_RULE#rule_1",
		Rule: EvaluationRule{
			Rule:   Rule{TenantID: "tnt_1", RuleID: "rule_1", Version: 1, EvaluationRevision: 1, Duration: time.Minute, Cooldown: 30 * time.Minute, Channels: []Channel{ChannelEmail}},
			PondID: "pond_1", Metric: "do_mg_l", Operator: "<", Threshold: 5, Aggregation: AggregationMin, Window: time.Minute,
		},
		NextEvaluationAt: slot,
		LeaseOwner:       "run_1", LeaseEpoch: 1,
	}
}

func TestRunnerProcessesOneRuleAndCommitsStableSlot(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
	work := runnableWork(slot)
	store := &fakeStore{
		candidates: []Candidate{{PK: work.PK, SK: work.SK, RuleID: "rule_1", Bucket: EvaluationBucket("tnt_1", "rule_1")}},
		work:       map[string]Work{"rule_1": work},
		states:     map[string]VersionedState{},
	}
	runner := Runner{Store: store, Windows: fakeWindowReader{result: WindowResult{Quality: QualitySufficient, Value: 4.2}}}

	summary := runner.Run(context.Background(), runnableConfig(evaluationTime))

	if summary.ExitCode != ExitSuccess || !summary.ScopeCompleted || summary.RulesEvaluated != 1 || summary.IncidentsFired != 1 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(store.commits) != 1 || !store.commits[0].Slot.Equal(slot) || store.commits[0].Decision.Transition != TransitionOpened {
		t.Fatalf("commits = %#v", store.commits)
	}
}

func TestRunnerCoalescesMissedSlotsAndRestartsPending(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 4, 0, 0, time.UTC)
	slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
	work := runnableWork(slot.Add(-3 * time.Minute))
	work.Rule.Duration = 3 * time.Minute
	store := &fakeStore{
		candidates: []Candidate{{PK: work.PK, SK: work.SK, RuleID: "rule_1", Bucket: EvaluationBucket("tnt_1", "rule_1")}},
		work:       map[string]Work{"rule_1": work},
		states:     map[string]VersionedState{"rule_1": {State: State{Mode: ModePending, ConfirmedSlots: 1, PendingSince: slot.Add(-4 * time.Minute), LastBreachSlot: slot.Add(-4 * time.Minute)}, Revision: 2}},
	}
	runner := Runner{Store: store, Windows: fakeWindowReader{result: WindowResult{Quality: QualitySufficient, Value: 4.2}}}

	summary := runner.Run(context.Background(), runnableConfig(evaluationTime))

	if summary.MissedSlots != 3 || summary.CoalescedRules != 1 {
		t.Fatalf("summary = %#v", summary)
	}
	if store.commits[0].Decision.Next.ConfirmedSlots != 1 {
		t.Fatalf("pending was not restarted: %#v", store.commits[0])
	}
}

func TestRunnerRecordsIsolatedWindowErrorAndReturnsPartial(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
	work := runnableWork(slot)
	store := &fakeStore{
		candidates: []Candidate{{PK: work.PK, SK: work.SK, RuleID: "rule_1", Bucket: EvaluationBucket("tnt_1", "rule_1")}},
		work:       map[string]Work{"rule_1": work}, states: map[string]VersionedState{},
	}
	runner := Runner{Store: store, Windows: fakeWindowReader{err: errors.New("influx timeout")}}

	summary := runner.Run(context.Background(), runnableConfig(evaluationTime))

	if summary.ExitCode != ExitPartial || summary.RulesWithError != 1 || !summary.ScopeCompleted {
		t.Fatalf("summary = %#v", summary)
	}
	if len(store.commits) != 1 || store.commits[0].Evaluation.Quality != QualityQueryError {
		t.Fatalf("error commit = %#v", store.commits)
	}
}

func TestRunnerReturnsFatalWhenDiscoveryFails(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	runner := Runner{Store: &fakeStore{queryErr: errors.New("dynamo unavailable")}, Windows: fakeWindowReader{}}

	summary := runner.Run(context.Background(), runnableConfig(evaluationTime))

	if summary.ExitCode != ExitFatal || summary.ScopeCompleted || !summary.RetryRecommended {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunnerEscalatesSystemicWindowFailures(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
	store := &fakeStore{work: map[string]Work{}, states: map[string]VersionedState{}}
	for _, ruleID := range []string{"rule_1", "rule_2", "rule_3"} {
		work := runnableWork(slot)
		work.SK = "ALERT_RULE#" + ruleID
		work.Rule.RuleID = ruleID
		store.work[ruleID] = work
		store.candidates = append(store.candidates, Candidate{
			PK: work.PK, SK: work.SK, RuleID: ruleID,
			Bucket: EvaluationBucket(work.Rule.TenantID, ruleID),
		})
	}
	runner := Runner{Store: store, Windows: fakeWindowReader{err: errors.New("influx unavailable")}}

	summary := runner.Run(context.Background(), runnableConfig(evaluationTime))

	if summary.ExitCode != ExitFatal || summary.ScopeCompleted || summary.RulesWithError != 3 {
		t.Fatalf("summary = %#v", summary)
	}
}
