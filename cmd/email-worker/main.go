// Command email-worker is the ADR-020 email-worker role, SES consumer 1: SQS email jobs →
// SES. It runs continuously under workerproc.Continuous; the SES-feedback consumer is its own
// process, cmd/ses-feedback-worker.
//
// Exit codes: 0 after SIGTERM/SIGINT, 1 on a runtime failure or a stalled loop, 78 on a
// configuration error.
package main

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workercompose "github.com/VINIClUS/limnopulse/internal/notifications/worker/compose"
	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker/consumer"
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
			return workerconfig.LoadEmail(args, lookup)
		},
		Run: deps.Run,
	}.Main(ctx, args, deps.Output, deps.LookupEnv, deps.Supervisor)
}
