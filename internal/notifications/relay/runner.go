package relay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
)

const (
	ExitSuccess = 0
	ExitFatal   = 1
	ExitPartial = 2
)

type RunSummary struct {
	RunID                       string         `json:"run_id"`
	RelayTime                   time.Time      `json:"relay_time"`
	Shard                       int            `json:"shard"`
	ShardCount                  int            `json:"shard_count"`
	Duration                    time.Duration  `json:"duration"`
	Result                      string         `json:"result"`
	ExitCode                    int            `json:"exit_code"`
	ScopeCompleted              bool           `json:"scope_completed"`
	RetryRecommended            bool           `json:"retry_recommended"`
	Backlog                     int            `json:"backlog"`
	OldestBacklogAgeSeconds     int64          `json:"oldest_backlog_age_seconds"`
	WorkProcessed               int            `json:"work_processed"`
	WorkSkipped                 int            `json:"work_skipped"`
	WorkErrors                  int            `json:"work_errors"`
	IntentsProcessed            int            `json:"intents_processed"`
	DependenciesProcessed       int            `json:"dependencies_processed"`
	DeliveriesProcessed         int            `json:"deliveries_processed"`
	DeliveriesCreated           int            `json:"deliveries_created"`
	DeliveriesCancelled         int            `json:"deliveries_cancelled"`
	RecipientsExamined          int            `json:"recipients_examined"`
	RecipientsFiltered          int            `json:"recipients_filtered"`
	RecipientFilteredBySeverity int            `json:"recipient_filtered_by_severity"`
	PublishedJobs               int            `json:"published_jobs"`
	SQSErrors                   int            `json:"sqs_errors"`
	WorkRemaining               int            `json:"work_remaining"`
	CapReached                  bool           `json:"cap_reached"`
	DeadlineReached             bool           `json:"deadline_reached"`
	ErrorCategories             map[string]int `json:"error_categories,omitempty"`
	TelemetryExportError        string         `json:"telemetry_export_error,omitempty"`
}

type Runner struct {
	Store     WorkStore
	Publisher Publisher
	Clock     func() time.Time
	IDFactory func() string
}

func (runner Runner) Run(parent context.Context, config relayconfig.RunConfig) RunSummary {
	clock := runner.Clock
	if clock == nil {
		clock = time.Now
	}
	runnerStartedAt := clock().UTC()
	budgetStartedAt := runnerStartedAt
	if config.BudgetStartedAt != nil && !config.BudgetStartedAt.After(runnerStartedAt) {
		budgetStartedAt = config.BudgetStartedAt.UTC()
	}
	relayTime := runnerStartedAt
	if config.RelayTime != nil {
		relayTime = config.RelayTime.UTC()
	}
	runID := newRunID()
	if runner.IDFactory != nil {
		runID = runner.IDFactory()
	}
	summary := RunSummary{
		RunID: runID, RelayTime: relayTime, Shard: config.Shard,
		ShardCount: config.ShardCount, ScopeCompleted: true,
		ErrorCategories: make(map[string]int),
	}
	if runner.Store == nil {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.ErrorCategories["configuration"]++
		return finishSummary(summary, ExitFatal, budgetStartedAt, clock().UTC())
	}
	remainingGlobal := config.GlobalDeadline - runnerStartedAt.Sub(budgetStartedAt)
	if remainingGlobal <= 0 {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.DeadlineReached = true
		summary.WorkRemaining = 1
		return finishSummary(summary, ExitPartial, budgetStartedAt, clock().UTC())
	}
	ctx, cancel := context.WithTimeout(parent, remainingGlobal)
	defer cancel()
	softDeadline := budgetStartedAt.Add(config.SoftDeadline)
	buckets, err := notifications.OwnedRelayBuckets(config.Shard, config.ShardCount)
	if err != nil {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.ErrorCategories["configuration"]++
		return finishSummary(summary, ExitFatal, budgetStartedAt, clock().UTC())
	}
	buckets = rotateBuckets(buckets, relayTime)
	discovery, err := runner.discover(ctx, buckets, config, relayTime, softDeadline, clock)
	if err != nil {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.ErrorCategories["discovery"]++
		return finishSummary(summary, ExitFatal, budgetStartedAt, clock().UTC())
	}
	candidates := discovery.candidates
	summary.Backlog = len(candidates)
	for _, candidate := range candidates {
		age := relayTime.Sub(candidate.AvailableAt)
		if seconds := int64(age / time.Second); seconds > summary.OldestBacklogAgeSeconds {
			summary.OldestBacklogAgeSeconds = seconds
		}
	}
	ordered := prioritizeCandidates(candidates)
	if len(ordered) > config.MaxWork {
		ordered = ordered[:config.MaxWork]
		discovery.complete = false
		discovery.capReached = true
	}
	if !discovery.complete {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.CapReached = discovery.capReached
		summary.DeadlineReached = discovery.deadlineReached
		summary.WorkRemaining = 1
	}
	results := make(chan candidateResult, len(ordered))
	jobs := make(chan Candidate, len(ordered))
	workerCount := min(config.WorkParallelism, len(ordered))
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for candidate := range jobs {
				results <- runner.processCandidate(
					ctx, candidate, config, relayTime, softDeadline, runID, clock,
				)
			}
		}()
	}
	for _, candidate := range ordered {
		jobs <- candidate
	}
	close(jobs)
	workers.Wait()
	close(results)
	for result := range results {
		summary.WorkProcessed += result.processed
		summary.WorkSkipped += result.skipped
		summary.WorkErrors += result.withError
		summary.IntentsProcessed += result.intents
		summary.DependenciesProcessed += result.dependencies
		summary.DeliveriesProcessed += result.deliveries
		summary.DeliveriesCreated += result.fanout.DeliveriesCreated
		summary.DeliveriesCancelled += result.fanout.DeliveriesCancelled
		summary.RecipientsExamined += result.fanout.RecipientsExamined
		summary.RecipientsFiltered += result.fanout.RecipientsFiltered
		summary.RecipientFilteredBySeverity += result.fanout.FilteredBySeverity
		summary.PublishedJobs += result.published
		summary.SQSErrors += result.sqsErrors
		summary.WorkRemaining += result.remaining
		if result.deadline {
			summary.DeadlineReached = true
			summary.ScopeCompleted = false
		}
		if result.category != "" {
			summary.ErrorCategories[result.category]++
		}
		if result.withError > 0 || result.remaining > 0 {
			summary.RetryRecommended = true
		}
	}
	exitCode := ExitSuccess
	if !summary.ScopeCompleted || summary.WorkErrors > 0 {
		exitCode = ExitPartial
	}
	return finishSummary(summary, exitCode, budgetStartedAt, clock().UTC())
}

func (runner Runner) discover(
	ctx context.Context,
	buckets []int,
	config relayconfig.RunConfig,
	relayTime time.Time,
	softDeadline time.Time,
	clock func() time.Time,
) (discoveryResult, error) {
	result := discoveryResult{complete: true, candidates: make([]Candidate, 0, 3*config.MaxWork)}
	for _, kind := range []notifications.WorkKind{
		notifications.WorkKindDelivery,
		notifications.WorkKindIntent,
		notifications.WorkKindDependency,
	} {
		lane, err := runner.discoverLane(
			ctx, buckets, kind, config, relayTime, softDeadline, clock,
		)
		if err != nil {
			return discoveryResult{}, err
		}
		result.candidates = append(result.candidates, lane.candidates...)
		if !lane.complete {
			result.complete = false
		}
		result.capReached = result.capReached || lane.capReached
		result.deadlineReached = result.deadlineReached || lane.deadlineReached
	}
	return result, nil
}

func (runner Runner) discoverLane(
	ctx context.Context,
	buckets []int,
	kind notifications.WorkKind,
	config relayconfig.RunConfig,
	relayTime time.Time,
	softDeadline time.Time,
	clock func() time.Time,
) (laneDiscoveryResult, error) {
	tokens := make(map[int]string, len(buckets))
	done := make(map[int]bool, len(buckets))
	candidates := make([]Candidate, 0, config.MaxWork)
	for len(done) < len(buckets) {
		pending := make([]int, 0, len(buckets)-len(done))
		for _, bucket := range buckets {
			if !done[bucket] {
				pending = append(pending, bucket)
			}
		}
		for offset := 0; offset < len(pending); offset += config.QueryParallelism {
			if !clock().Before(softDeadline) {
				return laneDiscoveryResult{
					candidates: candidates, deadlineReached: true,
				}, nil
			}
			if len(candidates) >= config.MaxWork {
				return laneDiscoveryResult{candidates: candidates, capReached: true}, nil
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
						Bucket: bucket, Kind: kind, DueThrough: relayTime,
						PageSize: discoveryPageSize(config.MaxWork, len(buckets)), NextToken: tokens[bucket],
					})
				}(index, bucket)
			}
			queries.Wait()
			truncated := false
			for _, result := range results {
				if result.err != nil {
					return laneDiscoveryResult{}, result.err
				}
				remaining := config.MaxWork - len(candidates)
				if len(result.page.Candidates) > remaining {
					candidates = append(candidates, result.page.Candidates[:remaining]...)
					truncated = true
				} else {
					candidates = append(candidates, result.page.Candidates...)
				}
				if result.page.NextToken == "" {
					done[result.bucket] = true
					continue
				}
				if result.page.NextToken == tokens[result.bucket] {
					return laneDiscoveryResult{}, context.Canceled
				}
				tokens[result.bucket] = result.page.NextToken
			}
			if truncated || (len(candidates) >= config.MaxWork &&
				(end < len(pending) || len(done) < len(buckets))) {
				return laneDiscoveryResult{candidates: candidates, capReached: true}, nil
			}
		}
	}
	return laneDiscoveryResult{candidates: candidates, complete: true}, nil
}

type discoveryResult struct {
	candidates      []Candidate
	complete        bool
	capReached      bool
	deadlineReached bool
}

type laneDiscoveryResult struct {
	candidates      []Candidate
	complete        bool
	capReached      bool
	deadlineReached bool
}

type candidateResult struct {
	processed    int
	skipped      int
	withError    int
	intents      int
	dependencies int
	deliveries   int
	published    int
	sqsErrors    int
	remaining    int
	deadline     bool
	category     string
	fanout       WorkResult
}

func (runner Runner) processCandidate(
	ctx context.Context,
	candidate Candidate,
	config relayconfig.RunConfig,
	relayTime time.Time,
	softDeadline time.Time,
	runID string,
	clock func() time.Time,
) candidateResult {
	if !clock().Before(softDeadline) {
		return candidateResult{remaining: 1, deadline: true}
	}
	itemCtx, cancel := context.WithTimeout(ctx, config.ItemTimeout)
	defer cancel()
	work, current, err := runner.Store.Reload(itemCtx, candidate, relayTime)
	if err != nil {
		return candidateResult{withError: 1, category: "reload"}
	}
	if !current {
		return candidateResult{skipped: 1}
	}
	now := clock().UTC()
	if !now.Before(softDeadline) {
		return candidateResult{remaining: 1, deadline: true}
	}
	claimed, acquired, err := runner.Store.Claim(itemCtx, work, LeaseRequest{
		Owner: runID, Now: now, ExpiresAt: now.Add(config.LeaseTTL), DueThrough: relayTime,
	})
	if err != nil {
		return candidateResult{withError: 1, category: "claim"}
	}
	if !acquired {
		return candidateResult{skipped: 1}
	}
	switch claimed.Kind {
	case notifications.WorkKindIntent:
		fanout, expandErr := runner.Store.ExpandIntent(itemCtx, claimed, ExpandRequest{
			RelayTime: relayTime, PageSize: config.FanoutPageSize,
		})
		if expandErr != nil {
			return runner.retryResult(ctx, claimed, relayTime, expandErr, "intent")
		}
		return candidateResult{processed: 1, intents: 1, fanout: fanout}
	case notifications.WorkKindDependency:
		fanout, expandErr := runner.Store.ExpandDependency(itemCtx, claimed, ExpandRequest{
			RelayTime: relayTime, PageSize: config.FanoutPageSize,
		})
		if expandErr != nil {
			return runner.retryResult(ctx, claimed, relayTime, expandErr, "dependency")
		}
		return candidateResult{processed: 1, dependencies: 1, fanout: fanout}
	case notifications.WorkKindDelivery:
		job, jobErr := notifications.NewDeliveryJob(
			claimed.TenantID, claimed.OutboxID, claimed.DeliveryID, claimed.EventID,
			claimed.NotificationKind, claimed.Channel,
		)
		if jobErr != nil {
			return runner.retryResult(ctx, claimed, relayTime, jobErr, "job")
		}
		if runner.Publisher == nil {
			return runner.retryResult(ctx, claimed, relayTime, context.Canceled, "sqs")
		}
		messageID, publishErr := runner.Publisher.Publish(itemCtx, PublishRequest{
			Job: job, Traceparent: claimed.Traceparent,
		})
		if publishErr != nil {
			result := runner.retryResult(ctx, claimed, relayTime, publishErr, "sqs")
			result.sqsErrors = 1
			return result
		}
		if markErr := runner.Store.MarkQueued(itemCtx, claimed, QueuedResult{
			QueuedAt: relayTime, MessageID: messageID,
		}); markErr != nil {
			return candidateResult{
				withError: 1, published: 1, category: "publication_commit",
			}
		}
		return candidateResult{processed: 1, deliveries: 1, published: 1}
	default:
		return runner.retryResult(ctx, claimed, relayTime, claimed.Kind.Validate(), "work_kind")
	}
}

func (runner Runner) retryResult(
	ctx context.Context,
	work Work,
	relayTime time.Time,
	cause error,
	category string,
) candidateResult {
	next := relayTime.Add(time.Minute)
	retryAt, expectedWait := RetryAt(cause)
	if expectedWait {
		next = retryAt
	}
	if err := runner.Store.Reschedule(ctx, work, next); err != nil {
		return candidateResult{withError: 1, category: "reschedule"}
	}
	if expectedWait {
		result := candidateResult{processed: 1}
		if category == "dependency" {
			result.dependencies = 1
		}
		if category == "intent" {
			result.intents = 1
		}
		return result
	}
	return candidateResult{withError: 1, category: category}
}

func prioritizeCandidates(candidates []Candidate) []Candidate {
	queues := map[notifications.WorkKind][]Candidate{
		notifications.WorkKindDelivery:   {},
		notifications.WorkKindIntent:     {},
		notifications.WorkKindDependency: {},
	}
	for _, candidate := range candidates {
		queues[candidate.Kind] = append(queues[candidate.Kind], candidate)
	}
	pattern := []notifications.WorkKind{
		notifications.WorkKindDelivery, notifications.WorkKindDelivery,
		notifications.WorkKindIntent, notifications.WorkKindDelivery,
		notifications.WorkKindDelivery, notifications.WorkKindDependency,
	}
	ordered := make([]Candidate, 0, len(candidates))
	for len(ordered) < len(candidates) {
		progress := false
		for _, kind := range pattern {
			if len(queues[kind]) == 0 {
				continue
			}
			ordered = append(ordered, queues[kind][0])
			queues[kind] = queues[kind][1:]
			progress = true
		}
		if !progress {
			break
		}
	}
	return ordered
}

func discoveryPageSize(maxWork, bucketCount int) int {
	if bucketCount < 1 {
		return 1
	}
	pageSize := maxWork / bucketCount
	if pageSize < 1 {
		return 1
	}
	return min(pageSize, 25)
}

type dueQueryResult struct {
	bucket int
	page   DuePage
	err    error
}

func rotateBuckets(buckets []int, relayTime time.Time) []int {
	if len(buckets) < 2 {
		return buckets
	}
	offset := int((relayTime.Unix() / 60) % int64(len(buckets)))
	if offset < 0 {
		offset += len(buckets)
	}
	rotated := make([]int, 0, len(buckets))
	rotated = append(rotated, buckets[offset:]...)
	rotated = append(rotated, buckets[:offset]...)
	return rotated
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
