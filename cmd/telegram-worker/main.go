// Command telegram-worker is the ADR-020 telegram-worker role: SQS Telegram jobs → Bot API,
// behind the shared Redis rate limiter. It runs continuously under workerproc.Continuous.
// Under systemd the Redis password is the redis-password credential, never the env file.
//
// Exit codes: 0 after SIGTERM/SIGINT, 1 on a runtime failure or a stalled loop, 78 on a
// configuration error.
package main

import (
	"context"
	"io"
	"os"
	"time"

	telegramcompose "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/compose"
	telegramworkerconfig "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/config"
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
	Run        func(context.Context, telegramworkerconfig.RunConfig, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error)
}

func main() {
	os.Exit(runMain(context.Background(), os.Args[1:], dependencies{
		Output: os.Stdout, LookupEnv: os.LookupEnv,
		Supervisor: workerproc.Continuous{StallAfter: stallAfter}, Run: telegramcompose.Run,
	}))
}

func runMain(ctx context.Context, args []string, deps dependencies) int {
	return consumer.Role[telegramworkerconfig.RunConfig]{
		Load: func(args []string, lookup func(string) (string, bool)) (telegramworkerconfig.RunConfig, error) {
			return telegramworkerconfig.Load(args, lookup)
		},
		Run:    deps.Run,
		Secret: &consumer.Secret{Credential: workerproc.CredentialRedisPassword, Key: "REDIS_PASSWORD"},
	}.Main(ctx, args, deps.Output, deps.LookupEnv, deps.Supervisor)
}
