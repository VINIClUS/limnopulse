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
	if config.RelayTime != nil && relayTime.After(runnerStartedAt) {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.ErrorCategories["configuration"]++
		return finishSummary(summary, ExitFatal, budgetStartedAt, clock().UTC())
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
	discoveryCutoff := softDeadline.Add(-config.ItemTimeout)
	discovery := discoveryResult{complete: false, deadlineReached: true}
	remainingDiscovery := discoveryCutoff.Sub(clock().UTC())
	if remainingDiscovery > 0 {
		discoveryCtx, cancelDiscovery := context.WithTimeout(ctx, remainingDiscovery)
		discovery, err = runner.discover(
			discoveryCtx, buckets, config, relayTime, discoveryCutoff, clock,
		)
		cancelDiscovery()
	}
	if err != nil {
		summary.ScopeCompleted = false
		summary.RetryRecommended = true
		summary.ErrorCategories["discovery"]++
		return finishSummary(summary, ExitFatal, budgetStartedAt, clock().UTC())
	}
	candidates := discovery.candidates
	summary.Backlog = len(candidates)
	summary.WorkSkipped += discovery.skipped
	for _, candidate := range candidates {
		age := relayTime.Sub(candidate.AvailableAt)
		if seconds := int64(age / time.Second); seconds > summary.OldestBacklogAgeSeconds {
			summary.OldestBacklogAgeSeconds = seconds
		}
	}
	ordered := prioritizeCandidates(candidates, relayTime)
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
	discoveryCutoff time.Time,
	clock func() time.Time,
) (discoveryResult, error) {
	lanes := make(map[notifications.WorkKind]*discoveryLane, 3)
	for _, kind := range []notifications.WorkKind{
		notifications.WorkKindDelivery,
		notifications.WorkKindIntent,
		notifications.WorkKindDependency,
	} {
		lane := &discoveryLane{
			kind: kind, candidates: make([]Candidate, 0, config.MaxWork),
			cursors: make([]*discoveryCursor, 0, len(buckets)),
		}
		for _, bucket := range buckets {
			lane.cursors = append(lane.cursors, &discoveryCursor{kind: kind, bucket: bucket})
		}
		lanes[kind] = lane
	}
	deadlineReached := false
	minuteSlot := relayTime.Unix() / 60
	for round := int64(0); discoveryHasActiveLane(lanes, config.MaxWork); round++ {
		if discoveryStopped(ctx, discoveryCutoff, clock) {
			deadlineReached = true
			break
		}
		baseKinds := rotateDiscoveryKinds([]notifications.WorkKind{
			notifications.WorkKindDelivery,
			notifications.WorkKindIntent,
			notifications.WorkKindDependency,
		}, minuteSlot+round)
		for offset := 0; offset < len(baseKinds); offset += config.QueryParallelism {
			if discoveryStopped(ctx, discoveryCutoff, clock) {
				deadlineReached = true
				break
			}
			end := min(offset+config.QueryParallelism, len(baseKinds))
			_, stopped, err := runner.queryDiscoveryWave(
				ctx, lanes, baseKinds[offset:end], config, relayTime, discoveryCutoff, clock,
			)
			if err != nil {
				return discoveryResult{}, err
			}
			if stopped {
				deadlineReached = true
				break
			}
		}
		if deadlineReached {
			break
		}
		remainingDeliveryWeight := 3
		for remainingDeliveryWeight > 0 && lanes[notifications.WorkKindDelivery].active(config.MaxWork) {
			if discoveryStopped(ctx, discoveryCutoff, clock) {
				deadlineReached = true
				break
			}
			weight := min(remainingDeliveryWeight, config.QueryParallelism)
			kinds := make([]notifications.WorkKind, weight)
			for index := range kinds {
				kinds[index] = notifications.WorkKindDelivery
			}
			queried, stopped, err := runner.queryDiscoveryWave(
				ctx, lanes, kinds, config, relayTime, discoveryCutoff, clock,
			)
			if err != nil {
				return discoveryResult{}, err
			}
			if stopped {
				deadlineReached = true
				break
			}
			if queried == 0 {
				break
			}
			remainingDeliveryWeight -= queried
		}
		if deadlineReached {
			break
		}
	}
	result := discoveryResult{
		complete: !deadlineReached, deadlineReached: deadlineReached,
		candidates: make([]Candidate, 0, 3*config.MaxWork),
	}
	for _, kind := range []notifications.WorkKind{
		notifications.WorkKindDelivery,
		notifications.WorkKindIntent,
		notifications.WorkKindDependency,
	} {
		lane := lanes[kind]
		result.candidates = append(result.candidates, lane.candidates...)
		result.skipped += lane.skipped
		if lane.capReached {
			result.capReached = true
			result.complete = false
		}
	}
	return result, nil
}

type discoveryResult struct {
	candidates      []Candidate
	skipped         int
	complete        bool
	capReached      bool
	deadlineReached bool
}

type discoveryCursor struct {
	kind      notifications.WorkKind
	bucket    int
	nextToken string
	done      bool
	inFlight  bool
}

type discoveryLane struct {
	kind       notifications.WorkKind
	cursors    []*discoveryCursor
	nextCursor int
	candidates []Candidate
	skipped    int
	capReached bool
}

func (lane *discoveryLane) active(maxWork int) bool {
	if lane.capReached || len(lane.candidates) >= maxWork {
		return false
	}
	for _, cursor := range lane.cursors {
		if !cursor.done {
			return true
		}
	}
	return false
}

func (lane *discoveryLane) takeCursor(maxWork int) *discoveryCursor {
	if !lane.active(maxWork) {
		return nil
	}
	for offset := range len(lane.cursors) {
		index := (lane.nextCursor + offset) % len(lane.cursors)
		cursor := lane.cursors[index]
		if cursor.done || cursor.inFlight {
			continue
		}
		cursor.inFlight = true
		lane.nextCursor = (index + 1) % len(lane.cursors)
		return cursor
	}
	return nil
}

func (lane *discoveryLane) hasRemainingCursor() bool {
	for _, cursor := range lane.cursors {
		if !cursor.done {
			return true
		}
	}
	return false
}

func discoveryHasActiveLane(lanes map[notifications.WorkKind]*discoveryLane, maxWork int) bool {
	for _, lane := range lanes {
		if lane.active(maxWork) {
			return true
		}
	}
	return false
}

func discoveryStopped(ctx context.Context, cutoff time.Time, clock func() time.Time) bool {
	return ctx.Err() != nil || !clock().Before(cutoff)
}

func rotateDiscoveryKinds(kinds []notifications.WorkKind, slot int64) []notifications.WorkKind {
	offset := int(slot % int64(len(kinds)))
	if offset < 0 {
		offset += len(kinds)
	}
	rotated := make([]notifications.WorkKind, 0, len(kinds))
	rotated = append(rotated, kinds[offset:]...)
	rotated = append(rotated, kinds[:offset]...)
	return rotated
}

func (runner Runner) queryDiscoveryWave(
	ctx context.Context,
	lanes map[notifications.WorkKind]*discoveryLane,
	kinds []notifications.WorkKind,
	config relayconfig.RunConfig,
	relayTime time.Time,
	discoveryCutoff time.Time,
	clock func() time.Time,
) (int, bool, error) {
	queries := make([]dueQueryResult, 0, len(kinds))
	for _, kind := range kinds {
		cursor := lanes[kind].takeCursor(config.MaxWork)
		if cursor == nil {
			continue
		}
		queries = append(queries, dueQueryResult{cursor: cursor})
	}
	var wait sync.WaitGroup
	for index := range queries {
		wait.Add(1)
		go func(result *dueQueryResult) {
			defer wait.Done()
			result.page, result.err = runner.Store.QueryDue(ctx, DueRequest{
				Bucket: result.cursor.bucket, Kind: result.cursor.kind, DueThrough: relayTime,
				PageSize:  discoveryPageSize(config.MaxWork, len(lanes[result.cursor.kind].cursors)),
				NextToken: result.cursor.nextToken,
			})
		}(&queries[index])
	}
	wait.Wait()
	stopped := false
	for index := range queries {
		result := &queries[index]
		lane := lanes[result.cursor.kind]
		result.cursor.inFlight = false
		if result.err != nil {
			if ctx.Err() != nil {
				stopped = true
				continue
			}
			return 0, false, result.err
		}
		candidates, skipped := runner.filterDisabledTelegramCandidates(
			ctx, result.page.Candidates, config, relayTime, discoveryCutoff, clock,
		)
		lane.skipped += skipped
		remaining := config.MaxWork - len(lane.candidates)
		if len(candidates) > remaining {
			lane.candidates = append(lane.candidates, candidates[:remaining]...)
			lane.capReached = true
		} else {
			lane.candidates = append(lane.candidates, candidates...)
		}
		if result.page.NextToken == "" {
			result.cursor.done = true
		} else {
			if result.page.NextToken == result.cursor.nextToken {
				return 0, false, context.Canceled
			}
			result.cursor.nextToken = result.page.NextToken
		}
	}
	for _, lane := range lanes {
		if len(lane.candidates) >= config.MaxWork && lane.hasRemainingCursor() {
			lane.capReached = true
		}
	}
	return len(queries), stopped, nil
}

// filterDisabledTelegramCandidates reloads only when Telegram is disabled so
// candidates of that channel cannot consume a discovery lane or the global
// work cap. The work is reloaded again before claim, preserving the existing
// conditional lease boundary while letting a paginated Query advance past a
// disabled Telegram backlog.
func (runner Runner) filterDisabledTelegramCandidates(
	ctx context.Context,
	candidates []Candidate,
	config relayconfig.RunConfig,
	relayTime time.Time,
	discoveryCutoff time.Time,
	clock func() time.Time,
) ([]Candidate, int) {
	if config.TelegramDeliveryEnabled || !config.TelegramDeliveryConfigured || len(candidates) == 0 {
		return candidates, 0
	}
	eligible := make([]Candidate, 0, len(candidates))
	skipped := 0
	for index, candidate := range candidates {
		if discoveryStopped(ctx, discoveryCutoff, clock) {
			return append(eligible, candidates[index:]...), skipped
		}
		itemCtx, cancel := context.WithTimeout(ctx, config.ItemTimeout)
		work, current, err := runner.Store.Reload(itemCtx, candidate, relayTime)
		cancel()
		if err != nil {
			// Keep invalid or unavailable items in the normal path so their
			// failure remains observable in the run summary and retry logic.
			eligible = append(eligible, candidate)
			continue
		}
		if !current || work.Channel == notifications.ChannelTelegram {
			skipped++
			continue
		}
		eligible = append(eligible, candidate)
	}
	return eligible, skipped
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
	if work.Channel == notifications.ChannelTelegram && !config.TelegramDeliveryEnabled {
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

func prioritizeCandidates(candidates []Candidate, relayTime time.Time) []Candidate {
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
	offset := int((relayTime.Unix() / 60) % int64(len(pattern)))
	if offset < 0 {
		offset += len(pattern)
	}
	pattern = rotateDiscoveryKinds(pattern, int64(offset))
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
	cursor *discoveryCursor
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
