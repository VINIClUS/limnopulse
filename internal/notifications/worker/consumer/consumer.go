// Package consumer runs one notification SQS consumer as its own ADR-020 process:
// workerproc.Continuous supervises it, and the run ends with one JSON summary line.
package consumer

import (
	"context"
	"errors"
	"io"
	"maps"
	"sync"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

// Role is one consumer entrypoint: the loader of its configuration and the composition that
// runs it. Main is the whole process.
type Role[C any] struct {
	Load func(args []string, lookup func(string) (string, bool)) (C, error)
	Run  func(context.Context, C, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error)
	// Secret, when set, is an env key whose value comes from a credential under systemd.
	Secret *Secret
}

// Secret names the LoadCredential= credential that replaces an env key.
type Secret struct {
	Credential string
	Key        string
}

// Main reads the role's credentials and configuration, then supervises it. A missing
// credential, a bad flag or a bad setting exits 78 before anything connects.
func (role Role[C]) Main(
	ctx context.Context,
	args []string,
	output io.Writer,
	lookup func(string) (string, bool),
	supervisor workerproc.Continuous,
) int {
	load, creds, err := workercmd.AWSLoaderFromEnv(lookup)
	if err != nil {
		return workercmd.Fail(output, err, "credentials")
	}
	if role.Secret != nil {
		lookup, err = workercmd.SecretLookup(lookup, creds, role.Secret.Credential, role.Secret.Key)
		if err != nil {
			return workercmd.Fail(output, err, "credentials")
		}
	}
	config, err := role.Load(args, lookup)
	if err != nil {
		return workercmd.Fail(output, workerproc.Configuration(err), "configuration")
	}
	return Run(ctx, supervisor, output, func(ctx context.Context, loop worker.Loop) (worker.RunSummary, error) {
		return role.Run(ctx, config, load, loop)
	})
}

// Consume composes one consumer and runs it until ctx is cancelled. An error means it could
// not start; a workerproc.Configuration error then exits 78.
type Consume func(context.Context, worker.Loop) (worker.RunSummary, error)

var errConsumerFailed = errors.New("consumer stopped on a fatal error")

// Run supervises consume and writes its summary to output. It returns the process exit code:
// 0 only after a stop signal, 1 for a fatal run or a stall, 78 for a configuration error.
func Run(ctx context.Context, supervisor workerproc.Continuous, output io.Writer, consume Consume) int {
	final := &finalSummary{}
	outcome := supervisor.Run(ctx, func(ctx context.Context, loop workerproc.Loop) error {
		summary, err := consume(ctx, loop)
		if err != nil {
			return err
		}
		final.set(summary)
		if summary.ExitCode != workerproc.ExitSuccess {
			return errConsumerFailed
		}
		return nil
	})
	return final.write(output, outcome)
}

// finalSummary holds the consumer's summary until the supervisor decides the outcome. After a
// stall the supervisor stops waiting, so a summary that arrives later is dropped.
type finalSummary struct {
	mu      sync.Mutex
	summary *worker.RunSummary
	written bool
}

func (final *finalSummary) set(summary worker.RunSummary) {
	final.mu.Lock()
	defer final.mu.Unlock()
	if !final.written {
		final.summary = &summary
	}
}

func (final *finalSummary) write(output io.Writer, outcome workerproc.Outcome) int {
	final.mu.Lock()
	defer final.mu.Unlock()
	final.written = true
	// Without a summary the consumer stopped before it ran, or never returned after a stall.
	summary := worker.RunSummary{Result: "success", Graceful: true}
	if final.summary != nil {
		summary = *final.summary
	}
	summary.ExitCode = outcome.ExitCode
	if outcome.ExitCode != workerproc.ExitSuccess {
		summary.Result = "fatal_failure"
		summary.Fatal = true
		summary.Graceful = false
		if final.summary == nil || outcome.Reason == workerproc.ReasonStalled ||
			outcome.Reason == workerproc.ReasonUnexpectedExit {
			categories := make(map[string]int, len(summary.ErrorCategories)+1)
			maps.Copy(categories, summary.ErrorCategories)
			categories[category(outcome.Reason)]++
			summary.ErrorCategories = categories
		}
	}
	workercmd.WriteJSON(output, summary)
	return outcome.ExitCode
}

func category(reason string) string {
	if reason == workerproc.ReasonFailure {
		return "startup_failure"
	}
	return reason
}
