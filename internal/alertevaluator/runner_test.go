package alertevaluator

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	loadErr    error
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
	if store.loadErr != nil {
		return VersionedState{}, store.loadErr
	}
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
	runner := Runner{Store: store, Windows: fakeWindowReader{result: WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: 4.2}}}

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
	runner := Runner{Store: store, Windows: fakeWindowReader{result: WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: 4.2}}}

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

func runSingleRule(t *testing.T, work func(time.Time) Work, reader WindowReader, state VersionedState) (RunSummary, CommitRequest) {
	t.Helper()
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
	rule := work(slot)
	store := &fakeStore{
		candidates: []Candidate{{PK: rule.PK, SK: rule.SK, RuleID: rule.Rule.RuleID, Bucket: EvaluationBucket(rule.Rule.TenantID, rule.Rule.RuleID)}},
		work:       map[string]Work{rule.Rule.RuleID: rule},
		states:     map[string]VersionedState{rule.Rule.RuleID: state},
	}
	summary := (Runner{Store: store, Windows: reader}).Run(context.Background(), runnableConfig(evaluationTime))
	if len(store.commits) != 1 {
		t.Fatalf("commits = %#v, summary = %#v", store.commits, summary)
	}
	return summary, store.commits[0]
}

func TestRunnerTreatsSufficientZeroAsPresentAndBreached(t *testing.T) {
	summary, commit := runSingleRule(t, runnableWork,
		fakeWindowReader{result: WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: 0}}, VersionedState{})

	value, ok := commit.Evaluation.Value.Get()
	if !ok || value != 0 || !commit.Evaluation.Breached || commit.Evaluation.Quality != QualitySufficient {
		t.Fatalf("evaluation = %#v, want present breached 0 for operator < 5", commit.Evaluation)
	}
	if commit.Decision.Transition != TransitionOpened || summary.IncidentsFired != 1 {
		t.Fatalf("present zero did not open: decision = %#v, summary = %#v", commit.Decision, summary)
	}
}

func TestRunnerComparesPresentZeroAgainstThreshold(t *testing.T) {
	work := func(slot time.Time) Work {
		rule := runnableWork(slot)
		rule.Rule.Threshold = -1
		return rule
	}
	_, commit := runSingleRule(t, work,
		fakeWindowReader{result: WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: 0}}, VersionedState{})

	if value, ok := commit.Evaluation.Value.Get(); !ok || value != 0 || commit.Evaluation.Breached {
		t.Fatalf("evaluation = %#v, want present clean 0 for operator < -1", commit.Evaluation)
	}
}

func TestRunnerDropsStaleWindowAggregate(t *testing.T) {
	active := VersionedState{State: State{Mode: ModeActive, ActiveEventID: "alert_1", ActiveStatus: StatusOpen, LastValue: Measured(4.2)}, Revision: 3}
	// The raw stale aggregate 1 breaches "< 5"; it must not count.
	_, commit := runSingleRule(t, runnableWork,
		fakeWindowReader{result: WindowResult{Quality: QualityStaleData, Value: 1}}, active)

	if commit.Evaluation.Value.IsPresent() || commit.Evaluation.Breached || commit.Evaluation.Quality != QualityStaleData {
		t.Fatalf("stale evaluation = %#v, want absent and not breached", commit.Evaluation)
	}
	if commit.Decision.Transition != TransitionNone || commit.Decision.Next.Mode != ModeActive {
		t.Fatalf("stale evaluation moved the incident: %#v", commit.Decision)
	}
	if value, ok := commit.Decision.Next.LastValue.Get(); !ok || value != 4.2 {
		t.Fatalf("stale evaluation replaced last value: %v", commit.Decision.Next.LastValue)
	}
}

func TestRunnerDropsWindowValueOnQueryError(t *testing.T) {
	// The reader also returns a sufficient breaching window; the error wins.
	reader := fakeWindowReader{result: WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: 1}, err: errors.New("influx timeout")}
	work := func(slot time.Time) Work {
		rule := runnableWork(slot)
		rule.Rule.Duration = 2 * time.Minute
		return rule
	}
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
	pending := VersionedState{State: State{
		Mode: ModePending, ConfirmedSlots: 1, PendingSince: slot.Add(-time.Minute), LastBreachSlot: slot.Add(-time.Minute),
	}, Revision: 2}
	summary, commit := runSingleRule(t, work, reader, pending)

	if commit.Evaluation.Quality != QualityQueryError || commit.Evaluation.Value.IsPresent() || commit.Evaluation.Breached {
		t.Fatalf("query error evaluation = %#v, want absent query_error", commit.Evaluation)
	}
	if commit.Decision.Transition != TransitionNone || commit.Decision.Next.ConfirmedSlots != 1 || summary.RulesWithError != 1 {
		t.Fatalf("query error decision = %#v, summary = %#v", commit.Decision, summary)
	}
}

func TestRunnerKeepsMissedSlotsOnIndeterminateEvaluations(t *testing.T) {
	for name, reader := range map[string]fakeWindowReader{
		"query error": {err: errors.New("influx timeout")},
		"stale":       {result: WindowResult{Quality: QualityStaleData, Value: 1}},
		"downgraded":  {result: WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: math.NaN()}},
	} {
		evaluationTime := time.Date(2026, 7, 15, 12, 4, 0, 0, time.UTC)
		slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
		work := runnableWork(slot.Add(-3 * time.Minute))
		work.Rule.Duration = 3 * time.Minute
		store := &fakeStore{
			candidates: []Candidate{{PK: work.PK, SK: work.SK, RuleID: "rule_1", Bucket: EvaluationBucket("tnt_1", "rule_1")}},
			work:       map[string]Work{"rule_1": work},
			states: map[string]VersionedState{"rule_1": {State: State{
				Mode: ModePending, ConfirmedSlots: 1, PendingSince: slot.Add(-4 * time.Minute), LastBreachSlot: slot.Add(-4 * time.Minute),
			}, Revision: 2}},
		}

		(Runner{Store: store, Windows: reader}).Run(context.Background(), runnableConfig(evaluationTime))

		if len(store.commits) != 1 || store.commits[0].Evaluation.MissedSlots != 3 {
			t.Fatalf("%s: commits = %#v", name, store.commits)
		}
		if next := store.commits[0].Decision.Next; next.Mode != ModeHealthy || next.ConfirmedSlots != 0 {
			t.Fatalf("%s: coalesced gap did not reset pending: %#v", name, next)
		}
	}
}

func TestRunnerDowngradesSufficientWindowWithoutUsableValue(t *testing.T) {
	// Before the value became optional these aggregates reached Decide as plain
	// numbers: NaN and ±Inf made json.Marshal fail the commit on every run, and
	// 1e200 and 5e-324 lie outside DynamoDB's number range.
	active := VersionedState{State: State{
		Mode: ModeActive, ActiveEventID: "alert_1", ActiveStatus: StatusOpen,
		OpeningOutboxes: map[Channel]string{ChannelEmail: "outbox_open_email"}, LastValue: Measured(4.2),
	}, Revision: 3}
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1e200, -1e26, 5e-324} {
		_, commit := runSingleRule(t, runnableWork,
			fakeWindowReader{result: WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: invalid}}, active)

		if commit.Evaluation.Quality != QualityInsufficientData || commit.Evaluation.Value.IsPresent() || commit.Evaluation.Breached {
			t.Fatalf("value %v: evaluation = %#v, want absent insufficient_data", invalid, commit.Evaluation)
		}
		if commit.Decision.Transition != TransitionNone || commit.Decision.Next.Mode != ModeActive {
			t.Fatalf("value %v: decision = %#v, want the active incident untouched", invalid, commit.Decision)
		}
		if commit.Decision.Next.LastQuality != QualityInsufficientData {
			t.Fatalf("value %v: recorded quality = %s", invalid, commit.Decision.Next.LastQuality)
		}
	}
}

func TestRunnerNeverOpensOnSufficientWindowWithoutPoints(t *testing.T) {
	// An unset Value reads as 0, which breaches "< 5" and would open.
	summary, commit := runSingleRule(t, runnableWork,
		fakeWindowReader{result: WindowResult{Quality: QualitySufficient}}, VersionedState{})

	if commit.Evaluation.Quality != QualityInsufficientData || commit.Evaluation.Value.IsPresent() || commit.Evaluation.Breached {
		t.Fatalf("evaluation = %#v, want absent insufficient_data", commit.Evaluation)
	}
	if commit.Decision.Transition != TransitionNone || summary.IncidentsFired != 0 {
		t.Fatalf("bare sufficient window moved the rule: decision = %#v, summary = %#v", commit.Decision, summary)
	}
}

type blockingWindowReader struct{}

func (blockingWindowReader) Evaluate(ctx context.Context, _ EvaluationRule, _ WindowSpec) (WindowResult, error) {
	select {
	case <-ctx.Done():
		return WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: 3}, ctx.Err()
	case <-time.After(2 * time.Second):
		// Fails the test instead of hanging when PerRuleTimeout is not applied.
		return WindowResult{Quality: QualitySufficient, ValidPointCount: 1, Value: 3}, nil
	}
}

func TestRunnerTreatsPerRuleTimeoutAsAbsentQueryError(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
	work := runnableWork(slot)
	store := &fakeStore{
		candidates: []Candidate{{PK: work.PK, SK: work.SK, RuleID: "rule_1", Bucket: EvaluationBucket("tnt_1", "rule_1")}},
		work:       map[string]Work{"rule_1": work}, states: map[string]VersionedState{},
	}
	config := runnableConfig(evaluationTime)
	config.PerRuleTimeout = 10 * time.Millisecond

	summary := (Runner{Store: store, Windows: blockingWindowReader{}}).Run(context.Background(), config)

	if len(store.commits) != 1 || summary.ErrorCategories["window_query"] != 1 {
		t.Fatalf("commits = %#v, summary = %#v", store.commits, summary)
	}
	evaluation := store.commits[0].Evaluation
	if evaluation.Quality != QualityQueryError || evaluation.Value.IsPresent() || evaluation.Breached {
		t.Fatalf("timed out evaluation = %#v, want absent query_error", evaluation)
	}
	if store.commits[0].Decision.Transition != TransitionNone {
		t.Fatalf("timed out decision = %#v", store.commits[0].Decision)
	}
}

func TestRunnerReportsUnsupportedStateSchemaSeparately(t *testing.T) {
	for _, test := range []struct {
		err          error
		wantCategory string
	}{
		{fmt.Errorf("decode alert evaluation state JSON: %w", ErrUnsupportedStateSchema), "state_schema"},
		{errors.New("dynamo unavailable"), "state_read"},
	} {
		evaluationTime := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
		slot := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second)
		work := runnableWork(slot)
		store := &fakeStore{
			candidates: []Candidate{{PK: work.PK, SK: work.SK, RuleID: "rule_1", Bucket: EvaluationBucket("tnt_1", "rule_1")}},
			work:       map[string]Work{"rule_1": work}, loadErr: test.err,
		}

		summary := (Runner{Store: store, Windows: fakeWindowReader{}}).Run(context.Background(), runnableConfig(evaluationTime))

		if summary.ErrorCategories[test.wantCategory] != 1 || len(summary.ErrorCategories) != 1 ||
			summary.ExitCode != ExitFatal || len(store.commits) != 0 {
			t.Fatalf("%v: summary = %#v, commits = %d", test.err, summary, len(store.commits))
		}
	}
}
