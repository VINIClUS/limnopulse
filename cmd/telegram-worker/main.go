// Command telegram-worker is the ADR-020 telegram-worker role: SQS Telegram jobs → Bot API,
// behind the shared Redis rate limiter. It runs continuously under workerproc.Continuous.
// Under systemd the Redis password is the redis-password credential, never the env file.
//
// Exit codes: 0 after SIGTERM/SIGINT, 1 on a runtime failure or a stalled loop, 78 on a
// configuration error.
package main

import (
	"context"
	"fmt"
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
			config, err := telegramworkerconfig.Load(args, lookup)
			if err != nil {
				return config, err
			}
			return config, checkRole(config)
		},
		Run:    deps.Run,
		Secret: &consumer.Secret{Credential: workerproc.CredentialRedisPassword, Key: "REDIS_PASSWORD"},
	}.Main(ctx, args, deps.Output, deps.LookupEnv, deps.Supervisor)
}

// checkRole rejects at load a rate whose limiter wait would outlast the stall limit. The
// processor sleeps out a RateLimitError in-process, and the Redis limiter asks for the longer
// of the two buckets' waits, so a slow bucket would read as a stalled loop and restart forever.
func checkRole(config telegramworkerconfig.RunConfig) error {
	for _, rate := range []struct {
		name  string
		value float64
	}{
		{"TELEGRAM_GLOBAL_RATE", config.GlobalRate}, {"TELEGRAM_DESTINATION_RATE", config.DestinationRate},
	} {
		if 1/rate.value+config.ProviderTimeout.Seconds() >= stallAfter.Seconds() {
			return fmt.Errorf("%s is too low for the %s stall limit", rate.name, stallAfter)
		}
	}
	return nil
}
