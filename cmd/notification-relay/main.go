// Command notification-relay is the ADR-020 notification-relay role: one bounded relay run
// (outbox → channel SQS lanes) per invocation, started by its systemd timer. It has no loop
// and no sleep; overlapping runs stay safe through the relay's leases and fencing.
//
// Exit codes: 0 success, 1 runtime failure, 2 partial run, 78 configuration error.
package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	relaycompose "github.com/VINIClUS/limnopulse/internal/notifications/relay/compose"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

type dependencies struct {
	Output    io.Writer
	LookupEnv func(string) (string, bool)
	RunRelay  func(context.Context, relayconfig.RunConfig, workercmd.AWSLoader) (relay.RunSummary, error)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runMain(ctx, os.Args[1:], dependencies{Output: os.Stdout, LookupEnv: os.LookupEnv, RunRelay: relaycompose.Run})
	stop()
	os.Exit(code)
}

func runMain(ctx context.Context, args []string, deps dependencies) int {
	budgetStartedAt := time.Now().UTC()
	config, err := relayconfig.Load(args, deps.LookupEnv)
	if err != nil {
		return workercmd.Fail(deps.Output, workerproc.Configuration(err), "configuration")
	}
	load, _, err := workercmd.AWSLoaderFromEnv(deps.LookupEnv)
	if err != nil {
		return workercmd.Fail(deps.Output, err, "credentials")
	}
	config.BudgetStartedAt = &budgetStartedAt
	runCtx, cancel := context.WithDeadline(ctx, budgetStartedAt.Add(config.GlobalDeadline))
	defer cancel()
	summary, err := deps.RunRelay(runCtx, config, load)
	if err != nil {
		return workercmd.Fail(deps.Output, err, "aws_configuration")
	}
	// The runner reports a relay time in the future as a configuration error too.
	if summary.ExitCode == relay.ExitFatal && summary.ErrorCategories["configuration"] > 0 {
		summary.ExitCode = workerproc.ExitConfiguration
		summary.RetryRecommended = false
	}
	workercmd.WriteJSON(deps.Output, summary)
	return summary.ExitCode
}
