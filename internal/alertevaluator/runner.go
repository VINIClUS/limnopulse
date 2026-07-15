package alertevaluator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

const EvaluationCadence = time.Minute

const (
	ExitSuccess = 0
	ExitFatal   = 1
	ExitPartial = 2
)

type RunSummary struct {
	RunID                string         `json:"run_id"`
	EvaluationTime       time.Time      `json:"evaluation_time"`
	Shard                int            `json:"shard"`
	ShardCount           int            `json:"shard_count"`
	Duration             time.Duration  `json:"duration"`
	Result               string         `json:"result"`
	ExitCode             int            `json:"exit_code"`
	ScopeCompleted       bool           `json:"scope_completed"`
	RetryRecommended     bool           `json:"retry_recommended"`
	RulesDiscovered      int            `json:"rules_discovered"`
	RulesEvaluated       int            `json:"rules_evaluated"`
	RulesSkipped         int            `json:"rules_skipped"`
	RulesWithError       int            `json:"rules_with_error"`
	IncidentsFired       int            `json:"incidents_fired"`
	IncidentsRecovered   int            `json:"incidents_recovered"`
	IncidentsSuppressed  int            `json:"incidents_suppressed"`
	MissedSlots          int            `json:"missed_slots"`
	CoalescedRules       int            `json:"coalesced_rules"`
	RulesRemaining       int            `json:"rules_remaining"`
	CapReached           bool           `json:"cap_reached"`
	DeadlineReached      bool           `json:"deadline_reached"`
	ErrorCategories      map[string]int `json:"error_categories,omitempty"`
	TelemetryExportError string         `json:"telemetry_export_error,omitempty"`
}

type Runner struct {
	Store     WorkStore
	Windows   WindowReader
	Clock     func() time.Time
	IDFactory func() string
}

func (runner Runner) Run(parent context.Context, config RunConfig) RunSummary {
	clock := runner.Clock
	if clock == nil {
		clock = time.Now
	}
	startedAt := clock().UTC()
	evaluationTime := startedAt
	if config.EvaluationTime != nil {
		evaluationTime = config.EvaluationTime.UTC()
	}
	runID := newRunID()
	if runner.IDFactory != nil {
		runID = runner.IDFactory()
	}
	summary := RunSummary{
		RunID: runID, EvaluationTime: evaluationTime, Shard: config.Shard,
		ShardCount: config.ShardCount, ScopeCompleted: true,
		ErrorCategories: map[string]int{},
	}
	ctx, cancel := context.WithTimeout(parent, config.GlobalDeadline)
	defer cancel()
	softDeadline := startedAt.Add(config.GlobalDeadline - config.DrainGrace)
	slot := LatestCompleteSlot(evaluationTime, EvaluationCadence, config.AllowedLateness)

	candidates, complete, err := runner.discover(ctx, config, slot, softDeadline, clock)
	if err != nil {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.ErrorCategories["discovery"]++
		return finishSummary(summary, ExitFatal, startedAt, clock())
	}
	summary.RulesDiscovered = len(candidates)
	if !complete {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.CapReached = len(candidates) >= config.MaxRules
		summary.RulesRemaining = 1
	}

	results := make(chan candidateResult, len(candidates))
	jobs := make(chan Candidate, len(candidates))
	workerCount := min(config.EvaluationParallelism, len(candidates))
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for candidate := range jobs {
				results <- runner.processCandidate(ctx, candidate, config, slot, softDeadline, runID, clock)
			}
		}()
	}
	for _, candidate := range candidates {
		jobs <- candidate
	}
	close(jobs)
	workers.Wait()
	close(results)
	for result := range results {
		summary.RulesEvaluated += result.evaluated
		summary.RulesSkipped += result.skipped
		summary.RulesWithError += result.withError
		summary.IncidentsFired += result.fired
		summary.IncidentsRecovered += result.recovered
		summary.IncidentsSuppressed += result.suppressed
		summary.MissedSlots += result.missedSlots
		summary.CoalescedRules += result.coalesced
		summary.RulesRemaining += result.remaining
		if result.deadline {
			summary.DeadlineReached = true
		}
		if result.category != "" {
			summary.ErrorCategories[result.category]++
		}
		if result.fatal || result.deadline {
			summary.ScopeCompleted = false
			summary.RetryRecommended = true
		}
	}

	exitCode := ExitSuccess
	if !summary.ScopeCompleted {
		exitCode = ExitFatal
	} else if summary.RulesWithError > 0 {
		exitCode = ExitPartial
		summary.RetryRecommended = true
	}
	return finishSummary(summary, exitCode, startedAt, clock())
}

type candidateResult struct {
	evaluated   int
	skipped     int
	withError   int
	fired       int
	recovered   int
	suppressed  int
	missedSlots int
	coalesced   int
	remaining   int
	category    string
	fatal       bool
	deadline    bool
}

func (runner Runner) processCandidate(ctx context.Context, candidate Candidate, config RunConfig, slot, softDeadline time.Time, runID string, clock func() time.Time) candidateResult {
	if !clock().Before(softDeadline) {
		return candidateResult{remaining: 1, deadline: true}
	}
	lease := LeaseRequest{Owner: runID, Now: clock().UTC(), DueThrough: slot}
	lease.ExpiresAt = lease.Now.Add(config.LeaseTTL)
	work, err := runner.Store.Claim(ctx, candidate, lease)
	if errors.Is(err, ErrLeaseConflict) {
		return candidateResult{skipped: 1}
	}
	if err != nil {
		return candidateResult{fatal: true, category: "claim"}
	}
	state, err := runner.Store.LoadState(ctx, work)
	if err != nil {
		return candidateResult{fatal: true, category: "state_read"}
	}
	missed := missedSlots(work.NextEvaluationAt, slot)
	result := candidateResult{missedSlots: missed}
	if missed > 0 {
		result.coalesced = 1
	}
	windowSpec := WindowSpec{
		Start: slot.Add(-work.Rule.Window), End: slot, DeviceID: work.Rule.DeviceID,
		ExpectedSampleInterval: config.ExpectedSampleInterval,
		EvaluationCadence:      EvaluationCadence, MinimumCoverageRatio: config.MinimumCoverageRatio,
		MinimumPoints: config.MinimumPoints, MaximumSampleAge: config.MaximumSampleAge,
		Aggregation: work.Rule.Aggregation,
	}
	ruleCtx, cancel := context.WithTimeout(ctx, config.PerRuleTimeout)
	window, windowErr := runner.Windows.Evaluate(ruleCtx, work.Rule, windowSpec)
	cancel()
	evaluation := Evaluation{Slot: slot, Quality: window.Quality, Value: window.Value, MissedSlots: missed}
	if windowErr != nil {
		evaluation.Quality = QualityQueryError
		result.withError = 1
		result.category = "window_query"
	} else if window.Quality == QualitySufficient {
		evaluation.Breached = compare(work.Rule.Operator, window.Value, work.Rule.Threshold)
	}
	decision := Decide(work.Rule.Rule, state.State, evaluation, EvaluationCadence)
	if err := runner.Store.Commit(ctx, CommitRequest{
		Work: work, PreviousState: state, Evaluation: evaluation, Decision: decision,
		Slot: slot, NextDue: slot.Add(EvaluationCadence),
	}); err != nil {
		return candidateResult{fatal: true, category: "commit", missedSlots: missed, coalesced: result.coalesced}
	}
	result.evaluated = 1
	switch decision.Transition {
	case TransitionOpened:
		result.fired = 1
	case TransitionSuppressed:
		result.fired = 1
		result.suppressed = 1
	case TransitionRecovered:
		result.recovered = 1
	}
	return result
}

func (runner Runner) discover(ctx context.Context, config RunConfig, slot, softDeadline time.Time, clock func() time.Time) ([]Candidate, bool, error) {
	buckets := OwnedBuckets(config.Shard, config.ShardCount)
	tokens := make(map[int]string, len(buckets))
	done := make(map[int]bool, len(buckets))
	candidates := make([]Candidate, 0, config.MaxRules)
	for len(done) < len(buckets) {
		progress := false
		pending := make([]int, 0, len(buckets)-len(done))
		for _, bucket := range buckets {
			if !done[bucket] {
				pending = append(pending, bucket)
			}
		}
		for offset := 0; offset < len(pending); offset += config.QueryParallelism {
			if len(candidates) >= config.MaxRules || !clock().Before(softDeadline) {
				return candidates, false, nil
			}
			end := min(offset+config.QueryParallelism, len(pending))
			results := make([]dueQueryResult, end-offset)
			var queries sync.WaitGroup
			for index, bucket := range pending[offset:end] {
				queries.Add(1)
				go func(index, bucket int) {
					defer queries.Done()
					results[index].bucket = bucket
					results[index].page, results[index].err = runner.Store.QueryDue(ctx, DueRequest{
						Bucket: bucket, DueThrough: slot, PageSize: config.PageSize, NextToken: tokens[bucket],
					})
				}(index, bucket)
			}
			queries.Wait()
			for _, result := range results {
				if result.err != nil {
					return nil, false, result.err
				}
				progress = true
				remaining := config.MaxRules - len(candidates)
				if len(result.page.Candidates) > remaining {
					candidates = append(candidates, result.page.Candidates[:remaining]...)
					return candidates, false, nil
				}
				candidates = append(candidates, result.page.Candidates...)
				if result.page.NextToken == "" {
					done[result.bucket] = true
				} else {
					tokens[result.bucket] = result.page.NextToken
				}
			}
		}
		if !progress {
			break
		}
	}
	return candidates, true, nil
}

type dueQueryResult struct {
	bucket int
	page   DuePage
	err    error
}

func missedSlots(nextDue, slot time.Time) int {
	if nextDue.IsZero() || !slot.After(nextDue) {
		return 0
	}
	return int(slot.Sub(nextDue) / EvaluationCadence)
}

func compare(operator string, value, threshold float64) bool {
	switch operator {
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	default:
		return false
	}
}

func finishSummary(summary RunSummary, exitCode int, startedAt, finishedAt time.Time) RunSummary {
	summary.ExitCode = exitCode
	summary.Duration = finishedAt.Sub(startedAt)
	switch exitCode {
	case ExitSuccess:
		summary.Result = "success"
	case ExitPartial:
		summary.Result = "partial_failure"
	case ExitFatal:
		summary.Result = "fatal_failure"
	}
	return summary
}

func newRunID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "run_" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	return "run_" + hex.EncodeToString(value[:])
}
