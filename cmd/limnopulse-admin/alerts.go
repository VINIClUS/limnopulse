package main

import (
	"context"
	"flag"
	"time"

	alertdynamo "github.com/VINIClUS/limnopulse/internal/alertevaluator/dynamo"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

// backfillSchedule writes the AlertEvaluationByDue attributes of rules that predate them.
func backfillSchedule(ctx context.Context, args []string, deps dependencies, load workercmd.AWSLoader) int {
	fs := flag.NewFlagSet("limnopulse-admin backfill-schedule", flag.ContinueOnError)
	var sources []tenantSource
	fs.Var(tenantSourceFlagValue{Kind: tenantSourceFlag, Sources: &sources}, "tenant", "explicit tenant ID; repeatable")
	evaluationTime := fs.String("evaluation-time", "", "stable RFC3339 schedule time; default now")
	apply := fs.Bool("apply", false, "write schedules; default is dry-run")
	pageSize := fs.Int("page-size", 25, "DynamoDB query page size")
	if err := workerproc.ParseFlags(fs, args); err != nil || len(fs.Args()) != 0 || *pageSize < 1 || *pageSize > maxPageSize {
		return writeFailure(deps.Output, "configuration", workerproc.ExitConfiguration)
	}
	var at time.Time
	if *evaluationTime != "" {
		parsed, err := time.Parse(time.RFC3339Nano, *evaluationTime)
		if err != nil {
			return writeFailure(deps.Output, "configuration", workerproc.ExitConfiguration)
		}
		at = parsed.UTC()
	}
	tenants, err := resolveTenants(sources)
	if err != nil {
		return writeFailure(deps.Output, "tenant_scope", workerproc.ExitConfiguration)
	}
	client, table, code := connect(ctx, deps, load)
	if code != workerproc.ExitSuccess {
		return code
	}
	summary, err := (alertdynamo.Store{Table: table, Client: client}).BackfillSchedule(ctx, alertdynamo.BackfillOptions{
		Tenants: tenants, EvaluationTime: at, Apply: *apply, PageSize: *pageSize,
	})
	return writeAlertResult(deps, summary, err, true)
}

// backfillActiveAlertIndex writes the sparse active-alert GSI keys of open and acknowledged
// alert events.
func backfillActiveAlertIndex(ctx context.Context, args []string, deps dependencies, load workercmd.AWSLoader) int {
	fs := flag.NewFlagSet("limnopulse-admin backfill-active-alert-index", flag.ContinueOnError)
	var sources []tenantSource
	for _, name := range []string{"tenant", "tenant-id"} {
		fs.Var(tenantSourceFlagValue{Kind: tenantSourceFlag, Sources: &sources}, name, "explicit tenant ID; repeatable")
	}
	apply := fs.Bool("apply", false, "write the active-alert projection; default is dry-run")
	pageSize := fs.Int("page-size", 25, "DynamoDB query page size")
	limit := fs.Int("limit", 0, "maximum pending projections; zero means unlimited")
	if err := workerproc.ParseFlags(fs, args); err != nil || len(fs.Args()) != 0 || *pageSize < 1 || *pageSize > maxPageSize || *limit < 0 {
		return writeFailure(deps.Output, "configuration", workerproc.ExitConfiguration)
	}
	tenants, err := resolveTenants(sources)
	if err != nil {
		return writeFailure(deps.Output, "tenant_scope", workerproc.ExitConfiguration)
	}
	client, table, code := connect(ctx, deps, load)
	if code != workerproc.ExitSuccess {
		return code
	}
	summary, err := (alertdynamo.Store{Table: table, Client: client}).BackfillActiveAlertIndex(ctx, alertdynamo.BackfillOptions{
		Tenants: tenants, Apply: *apply, PageSize: *pageSize, Limit: *limit,
	})
	// A --limit batch can stop before the scope ends; the next dry-run reports what remains.
	return writeAlertResult(deps, summary, err, *limit == 0)
}

func writeAlertResult(deps dependencies, summary alertdynamo.BackfillSummary, err error, scopeCompleted bool) int {
	result := commandResult{Result: "success", ScopeCompleted: scopeCompleted, Summary: &summary}
	if err != nil {
		result = commandResult{
			Result: "fatal_failure", ExitCode: workerproc.ExitFailure, RetryRecommended: true,
			ErrorCategories: map[string]int{"query_failure": 1}, Summary: &summary,
		}
	}
	workercmd.WriteJSON(deps.Output, result)
	return result.ExitCode
}
