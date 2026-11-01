package workerproc

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	ErrStopSignal = errors.New("stop signal received")
	ErrStalled    = errors.New("loop stopped making progress")
)

const defaultStallExitGrace = 5 * time.Second

// Outcome reasons, for the role's run summary.
const (
	ReasonStopped        = "stopped"
	ReasonStalled        = "stalled"
	ReasonUnexpectedExit = "unexpected_exit"
	ReasonConfiguration  = "configuration"
	ReasonFailure        = "failure"
)

// Loop is what a continuous worker reports to its supervisor.
type Loop interface {
	// Ready sends READY=1 once start-up has finished; it also counts as progress.
	Ready() error
	// Progress records that the loop advanced: a receive turn returned or a message
	// finished. Only fresh progress feeds the systemd watchdog.
	Progress()
}

// Continuous supervises one continuous worker inside its process (ADR-020): it sends
// READY=1 and STOPPING=1, feeds the systemd watchdog only while the loop makes progress,
// and ends the process with exit 1 when progress stops for StallAfter. The worker exits
// 0 only after a stop signal.
type Continuous struct {
	// StallAfter is the longest legitimate gap between two progress marks. It must stay
	// below the unit's WatchdogSec so this clean exit normally wins over SIGABRT.
	StallAfter time.Duration
	// StallExitGrace bounds the wait for run to return after a stall; zero means 5 s.
	StallExitGrace time.Duration
	Lookup         LookupEnv
	// Signals replaces SIGTERM/SIGINT delivery in tests.
	Signals <-chan os.Signal
	Now     func() time.Time
}

type Outcome struct {
	ExitCode int
	Reason   string
	Err      error
}

// Run starts run and supervises it. run receives a context cancelled with cause
// ErrStopSignal on SIGTERM/SIGINT or ErrStalled on a stall, and must drain and return
// once it is cancelled.
func (supervisor Continuous) Run(parent context.Context, run func(context.Context, Loop) error) Outcome {
	if supervisor.StallAfter <= 0 {
		return configurationOutcome(errors.New("stall threshold must be positive"))
	}
	now := supervisor.Now
	if now == nil {
		now = time.Now
	}
	notifier, err := NotifierFromEnv(supervisor.Lookup)
	if err != nil {
		return configurationOutcome(err)
	}
	watchdog, err := WatchdogTimeout(supervisor.Lookup, os.Getpid())
	if err != nil {
		return configurationOutcome(err)
	}
	if watchdog > 0 && watchdog <= supervisor.StallAfter {
		return configurationOutcome(errors.New("WatchdogSec must exceed the stall threshold"))
	}
	signals := supervisor.Signals
	if signals == nil {
		delivered := make(chan os.Signal, 1)
		signal.Notify(delivered, syscall.SIGTERM, os.Interrupt)
		defer signal.Stop(delivered)
		signals = delivered
	}
	grace := supervisor.StallExitGrace
	if grace <= 0 {
		grace = defaultStallExitGrace
	}

	// Only the supervisor cancels the worker: a parent cancellation goes through stop()
	// first, so the drain is always classified as a stop and announced with STOPPING=1.
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	defer cancel(nil)
	loop := &loopState{notifier: notifier, now: now, start: now()}
	loop.Progress()
	done := make(chan error, 1)
	go func() { done <- run(ctx, loop) }()

	check := supervisor.StallAfter / 4
	if watchdog > 0 && watchdog/2 < check {
		check = watchdog / 2
	}
	ticker := time.NewTicker(max(check, time.Millisecond))
	defer ticker.Stop()
	stopping := false
	stop := func() {
		if !stopping {
			stopping = true
			_ = notifier.Stopping()
			cancel(ErrStopSignal)
		}
	}
	var lastPing time.Time
	parentDone := parent.Done()
	for {
		select {
		case err := <-done:
			return runOutcome(err, stopping)
		case <-signals:
			stop()
		case <-parentDone:
			parentDone = nil
			stop()
		case <-ticker.C:
			// Shutdown has its own drain budget and the unit's TimeoutStopSec.
			if stopping {
				continue
			}
			current := now()
			if current.Sub(loop.last()) >= supervisor.StallAfter {
				cancel(ErrStalled)
				select {
				case <-done:
				case <-time.After(grace):
				}
				return Outcome{ExitCode: ExitFailure, Reason: ReasonStalled, Err: ErrStalled}
			}
			if watchdog > 0 && current.Sub(lastPing) >= watchdog/2 {
				// A failed ping is left to the systemd watchdog itself.
				_ = notifier.Watchdog()
				lastPing = current
			}
		}
	}
}

func runOutcome(err error, stopping bool) Outcome {
	switch {
	case IsConfiguration(err):
		return Outcome{ExitCode: ExitConfiguration, Reason: ReasonConfiguration, Err: err}
	case stopping && (err == nil || errors.Is(err, ErrStopSignal) || errors.Is(err, context.Canceled)):
		// A worker that drains and returns its context's error still stopped cleanly.
		return Outcome{ExitCode: ExitSuccess, Reason: ReasonStopped}
	case err != nil:
		return Outcome{ExitCode: ExitFailure, Reason: ReasonFailure, Err: err}
	default:
		return Outcome{
			ExitCode: ExitFailure, Reason: ReasonUnexpectedExit,
			Err: errors.New("continuous worker returned without a stop signal"),
		}
	}
}

func configurationOutcome(err error) Outcome {
	return Outcome{ExitCode: ExitConfiguration, Reason: ReasonConfiguration, Err: Configuration(err)}
}

type loopState struct {
	notifier *Notifier
	now      func() time.Time
	// start keeps time.Now's monotonic reading; progress is stored as the elapsed time
	// since start so wall-clock steps never shift stall detection.
	start     time.Time
	elapsed   atomic.Int64
	readyOnce sync.Once
	readyErr  error
}

func (loop *loopState) Ready() error {
	loop.readyOnce.Do(func() { loop.readyErr = loop.notifier.Ready() })
	loop.Progress()
	return loop.readyErr
}

func (loop *loopState) Progress() {
	loop.elapsed.Store(int64(loop.now().Sub(loop.start)))
}

func (loop *loopState) last() time.Time {
	return loop.start.Add(time.Duration(loop.elapsed.Load()))
}
