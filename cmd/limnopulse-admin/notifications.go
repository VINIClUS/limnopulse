package main

import (
	"context"
	"flag"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	notificationdynamo "github.com/VINIClUS/limnopulse/internal/notifications/dynamo"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

const (
	maxPageSize            = 1<<31 - 1
	defaultBackfillTimeout = 5 * time.Minute
	maxBackfillTimeout     = 24 * time.Hour
)

// backfillRelay writes the relay fields of legacy outboxes; backfill-telegram also moves
// deferred Telegram outboxes onto the v2 relay.
func backfillRelay(
	ctx context.Context,
	name string,
	targetChannel notifications.Channel,
	args []string,
	deps dependencies,
	load workercmd.AWSLoader,
) int {
	fs := flag.NewFlagSet("limnopulse-admin "+name, flag.ContinueOnError)
	var sources []tenantSource
	fs.Var(tenantSourceFlagValue{Kind: tenantSourceFlag, Sources: &sources}, "tenant", "explicit tenant ID; repeatable")
	fs.Var(tenantSourceFlagValue{Kind: tenantSourceFile, Sources: &sources}, "tenant-file", "file containing tenant IDs; repeatable")
	apply := fs.Bool("apply", false, "write relay fields; default is dry-run")
	pageSize := fs.Int("page-size", 25, "DynamoDB query page size")
	maxRows := fs.Int("max-rows", notificationdynamo.DefaultBackfillMaxRows, "maximum migration candidates per run")
	timeout := fs.Duration("timeout", defaultBackfillTimeout, "total backfill deadline")
	if err := parse(fs, args); err != nil {
		return workercmd.Fail(deps.Output, err, "configuration")
	}
	if err := checkBounds(
		bound{*pageSize >= 1 && *pageSize <= maxPageSize, "page-size"},
		bound{*maxRows >= 1, "max-rows"},
		bound{*timeout > 0 && *timeout <= maxBackfillTimeout, "timeout"},
	); err != nil {
		return workercmd.Fail(deps.Output, err, "configuration")
	}
	tenants, err := resolveTenants(sources)
	if err != nil {
		return workercmd.Fail(deps.Output, workerproc.Configuration(err), "tenant_scope")
	}
	runCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client, table, code := connect(runCtx, deps, load)
	if code != workerproc.ExitSuccess {
		return code
	}
	summary, err := (notificationdynamo.Store{Table: table, Client: client}).BackfillRelay(
		runCtx,
		notificationdynamo.BackfillOptions{
			Tenants: tenants, Apply: *apply, PageSize: *pageSize, MaxRows: *maxRows,
			TargetChannel: targetChannel,
		},
	)
	result := commandResult{Summary: &summary, RetryRecommended: true}
	switch {
	case err != nil:
		result.Result, result.ExitCode = "fatal_failure", workerproc.ExitFailure
		result.ErrorCategories = map[string]int{"query_failure": 1}
	case summary.LimitReached || summary.DeadlineReached:
		result.Result, result.ExitCode = "incomplete", workerproc.ExitFailure
		result.ErrorCategories = summaryErrorCategories(summary)
	case summary.RowFailures > 0:
		result.Result, result.ExitCode, result.ScopeCompleted = "partial_failure", workerproc.ExitPartial, true
		result.ErrorCategories = summaryErrorCategories(summary)
	default:
		result.Result, result.ScopeCompleted, result.RetryRecommended = "success", true, false
	}
	workercmd.WriteJSON(deps.Output, result)
	return result.ExitCode
}

func summaryErrorCategories(summary notificationdynamo.BackfillSummary) map[string]int {
	categories := make(map[string]int)
	for category, count := range map[string]int{
		"schema_conflict":   summary.SchemaConflicts,
		"concurrent_change": summary.ConcurrentChanges,
		"decode_failure":    summary.DecodeFailures,
		"update_failure":    summary.UpdateFailures,
	} {
		if count > 0 {
			categories[category] = count
		}
	}
	if summary.LimitReached {
		categories["row_limit_reached"] = 1
	}
	if summary.DeadlineReached {
		categories["deadline_reached"] = 1
	}
	return categories
}
