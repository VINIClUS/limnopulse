// Command email-worker is the ADR-020 email-worker role, SES consumer 1: SQS email jobs →
// SES. It runs continuously under workerproc.Continuous; the SES-feedback consumer is its own
// process, cmd/ses-feedback-worker.
//
// Exit codes: 0 after SIGTERM/SIGINT, 1 on a runtime failure or a stalled loop, 78 on a
// configuration error.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workercompose "github.com/VINIClUS/limnopulse/internal/notifications/worker/compose"
	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker/consumer"
	workerses "github.com/VINIClUS/limnopulse/internal/notifications/worker/ses"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

// stallAfter is this role's stall_after_seconds in ops/workers/roles.yaml.
const stallAfter = 90 * time.Second

type dependencies struct {
	Output     io.Writer
	LookupEnv  func(string) (string, bool)
	Supervisor workerproc.Continuous
	Run        func(context.Context, workerconfig.RunConfig, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error)
}

func main() {
	os.Exit(runMain(context.Background(), os.Args[1:], dependencies{
		Output: os.Stdout, LookupEnv: os.LookupEnv,
		Supervisor: workerproc.Continuous{StallAfter: stallAfter}, Run: workercompose.Run,
	}))
}

func runMain(ctx context.Context, args []string, deps dependencies) int {
	return consumer.Role[workerconfig.RunConfig]{
		Load: func(args []string, lookup func(string) (string, bool)) (workerconfig.RunConfig, error) {
			config, err := workerconfig.LoadEmail(args, lookup)
			if err != nil {
				return workerconfig.RunConfig{}, err
			}
			return config, checkRole(config)
		},
		Run: deps.Run,
	}.Main(ctx, args, deps.Output, deps.LookupEnv, deps.Supervisor)
}

// checkRole rejects at load what would otherwise fail only at runtime and restart forever.
// The SES source is checked here, not in workerconfig, which the feedback worker shares.
func checkRole(config workerconfig.RunConfig) error {
	if config.EmailSenderMode == "aws" && workerses.ValidateSource(config.SESFromEmail) != nil {
		return errors.New("SES_FROM_EMAIL is invalid")
	}
	// Every send waits for a limiter token, then makes one provider call. When both outlast
	// the stall limit, a loop whose slots are all waiting reports no progress and stalls.
	if 1/config.MaxSendRate+config.ProviderTimeout.Seconds() >= stallAfter.Seconds() {
		return fmt.Errorf("NOTIFICATION_MAX_SEND_RATE is too low for the %s stall limit", stallAfter)
	}
	return nil
}
