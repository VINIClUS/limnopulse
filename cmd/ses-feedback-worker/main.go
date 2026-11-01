// Command ses-feedback-worker is the ADR-020 ses-feedback-worker role, SES consumer 2: SQS SES
// events → delivery state. It runs continuously under workerproc.Continuous as a single
// instance; the email-jobs consumer is its own process, cmd/email-worker.
//
// Exit codes: 0 after SIGTERM/SIGINT, 1 on a runtime failure or a stalled loop, 78 on a
// configuration error.
package main

import (
	"context"
	"io"
	"os"
	"time"

	feedbackcompose "github.com/VINIClUS/limnopulse/internal/notifications/feedback/compose"
	feedbackconfig "github.com/VINIClUS/limnopulse/internal/notifications/feedback/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
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
	Run        func(context.Context, feedbackconfig.RunConfig, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error)
}

func main() {
	os.Exit(runMain(context.Background(), os.Args[1:], dependencies{
		Output: os.Stdout, LookupEnv: os.LookupEnv,
		Supervisor: workerproc.Continuous{StallAfter: stallAfter}, Run: feedbackcompose.Run,
	}))
}

func runMain(ctx context.Context, args []string, deps dependencies) int {
	return consumer.Role[feedbackconfig.RunConfig]{
		Load: func(args []string, lookup func(string) (string, bool)) (feedbackconfig.RunConfig, error) {
			return feedbackconfig.Load(args, lookup)
		},
		Run: deps.Run,
	}.Main(ctx, args, deps.Output, deps.LookupEnv, deps.Supervisor)
}
