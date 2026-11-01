package workerproc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func runAsync(supervisor Continuous, parent context.Context, run func(context.Context, Loop) error) <-chan Outcome {
	outcome := make(chan Outcome, 1)
	go func() { outcome <- supervisor.Run(parent, run) }()
	return outcome
}

func waitOutcome(t *testing.T, outcome <-chan Outcome) Outcome {
	t.Helper()
	select {
	case got := <-outcome:
		return got
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not return")
		return Outcome{}
	}
}

func TestStopSignalExitsZeroAfterReadyAndStopping(t *testing.T) {
	path, messages := listenNotify(t)
	signals := make(chan os.Signal, 1)
	causes := make(chan error, 1)
	outcome := runAsync(Continuous{
		StallAfter: time.Second,
		Lookup:     env(map[string]string{"NOTIFY_SOCKET": path}),
		Signals:    signals,
	}, context.Background(), func(ctx context.Context, loop Loop) error {
		if err := loop.Ready(); err != nil {
			return err
		}
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return nil
	})
	expectMessage(t, messages, "READY=1")
	signals <- syscall.SIGTERM
	got := waitOutcome(t, outcome)
	if got.ExitCode != ExitSuccess || got.Reason != ReasonStopped {
		t.Fatalf("got %+v", got)
	}
	if cause := <-causes; !errors.Is(cause, ErrStopSignal) {
		t.Fatalf("run saw cause %v", cause)
	}
	expectMessage(t, messages, "STOPPING=1")
}

func TestReturningTheContextErrorAfterAStopExitsZero(t *testing.T) {
	results := map[string]func(context.Context) error{
		"ctx.Err":       func(ctx context.Context) error { return ctx.Err() },
		"context.Cause": func(ctx context.Context) error { return context.Cause(ctx) },
		"wrapped":       func(ctx context.Context) error { return fmt.Errorf("receive: %w", ctx.Err()) },
	}
	for name, result := range results {
		signals := make(chan os.Signal, 1)
		outcome := runAsync(Continuous{
			StallAfter: time.Second,
			Lookup:     env(nil),
			Signals:    signals,
		}, context.Background(), func(ctx context.Context, loop Loop) error {
			<-ctx.Done()
			return result(ctx)
		})
		signals <- syscall.SIGTERM
		if got := waitOutcome(t, outcome); got.ExitCode != ExitSuccess || got.Reason != ReasonStopped {
			t.Errorf("%s: got %+v", name, got)
		}
	}
}

func TestFreshProgressFeedsTheWatchdog(t *testing.T) {
	path, messages := listenNotify(t)
	signals := make(chan os.Signal, 1)
	outcome := runAsync(Continuous{
		StallAfter: 200 * time.Millisecond,
		Lookup:     env(map[string]string{"NOTIFY_SOCKET": path, "WATCHDOG_USEC": "400000"}),
		Signals:    signals,
	}, context.Background(), func(ctx context.Context, loop Loop) error {
		if err := loop.Ready(); err != nil {
			return err
		}
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				loop.Progress()
			}
		}
	})
	expectMessage(t, messages, "WATCHDOG=1")
	expectMessage(t, messages, "WATCHDOG=1")
	// Running well past StallAfter must not stall a loop that keeps progressing.
	time.Sleep(500 * time.Millisecond)
	signals <- syscall.SIGTERM
	if got := waitOutcome(t, outcome); got.ExitCode != ExitSuccess {
		t.Fatalf("got %+v", got)
	}
}

func TestStalledLoopExitsOneEvenWhenRunIgnoresCancellation(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	started := time.Now()
	got := waitOutcome(t, runAsync(Continuous{
		StallAfter:     100 * time.Millisecond,
		StallExitGrace: 50 * time.Millisecond,
		Lookup:         env(nil),
		Signals:        make(chan os.Signal),
	}, context.Background(), func(ctx context.Context, loop Loop) error {
		_ = loop.Ready()
		<-release
		return nil
	}))
	if got.ExitCode != ExitFailure || got.Reason != ReasonStalled || !errors.Is(got.Err, ErrStalled) {
		t.Fatalf("got %+v", got)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("stall exit took %v", elapsed)
	}
}

func TestStalledLoopSeesTheStallCause(t *testing.T) {
	causes := make(chan error, 1)
	got := waitOutcome(t, runAsync(Continuous{
		StallAfter: 100 * time.Millisecond,
		Lookup:     env(nil),
		Signals:    make(chan os.Signal),
	}, context.Background(), func(ctx context.Context, loop Loop) error {
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return nil
	}))
	if got.ExitCode != ExitFailure || got.Reason != ReasonStalled {
		t.Fatalf("got %+v", got)
	}
	if cause := <-causes; !errors.Is(cause, ErrStalled) {
		t.Fatalf("run saw cause %v", cause)
	}
}

func TestStopSignalDisarmsStallDetectionDuringDrain(t *testing.T) {
	signals := make(chan os.Signal, 1)
	outcome := runAsync(Continuous{
		StallAfter: 100 * time.Millisecond,
		Lookup:     env(nil),
		Signals:    signals,
	}, context.Background(), func(ctx context.Context, loop Loop) error {
		_ = loop.Ready()
		<-ctx.Done()
		time.Sleep(400 * time.Millisecond)
		return nil
	})
	signals <- syscall.SIGINT
	if got := waitOutcome(t, outcome); got.ExitCode != ExitSuccess || got.Reason != ReasonStopped {
		t.Fatalf("a drain longer than StallAfter must still stop cleanly, got %+v", got)
	}
}

func TestParentCancellationIsAStop(t *testing.T) {
	path, messages := listenNotify(t)
	// A worker that returns the instant its context ends must still be classified as a
	// stop; repeat to catch a select that sees the worker's return before the cancellation.
	for attempt := range 200 {
		parent, cancel := context.WithCancel(context.Background())
		causes := make(chan error, 1)
		outcome := runAsync(Continuous{
			StallAfter: time.Second,
			Lookup:     env(map[string]string{"NOTIFY_SOCKET": path}),
			Signals:    make(chan os.Signal),
		}, parent, func(ctx context.Context, loop Loop) error {
			<-ctx.Done()
			causes <- context.Cause(ctx)
			return nil
		})
		cancel()
		if got := waitOutcome(t, outcome); got.ExitCode != ExitSuccess || got.Reason != ReasonStopped {
			t.Fatalf("attempt %d: got %+v", attempt, got)
		}
		if cause := <-causes; !errors.Is(cause, ErrStopSignal) {
			t.Fatalf("attempt %d: run saw cause %v", attempt, cause)
		}
		expectMessage(t, messages, "STOPPING=1")
	}
}

func TestProgressKeepsTheMonotonicClockReading(t *testing.T) {
	loop := &loopState{notifier: &Notifier{}, now: time.Now, start: time.Now()}
	loop.Progress()
	// time.Time prints its monotonic reading as "m=..."; a wall-clock-only timestamp
	// would let a clock step delay or force a stall.
	if last := loop.last().String(); !strings.Contains(last, " m=") {
		t.Fatalf("progress timestamp lost its monotonic reading: %s", last)
	}
	if age := time.Since(loop.last()); age < 0 || age > time.Second {
		t.Fatalf("unexpected progress age %v", age)
	}
}

func TestRunResultsMapToSharedExitCodes(t *testing.T) {
	cases := []struct {
		name       string
		result     error
		wantCode   int
		wantReason string
	}{
		{"return without stop", nil, ExitFailure, ReasonUnexpectedExit},
		{"configuration error", Configuration(errors.New("missing queue url")), ExitConfiguration, ReasonConfiguration},
		{"runtime failure", errors.New("receive failed"), ExitFailure, ReasonFailure},
	}
	for _, tc := range cases {
		got := waitOutcome(t, runAsync(Continuous{
			StallAfter: time.Second,
			Lookup:     env(nil),
			Signals:    make(chan os.Signal),
		}, context.Background(), func(context.Context, Loop) error { return tc.result }))
		if got.ExitCode != tc.wantCode || got.Reason != tc.wantReason {
			t.Errorf("%s: got %+v", tc.name, got)
		}
	}
}

func TestInvalidSupervisionIsAConfigurationError(t *testing.T) {
	cases := []struct {
		name       string
		supervisor Continuous
	}{
		{"no stall threshold", Continuous{Lookup: env(nil)}},
		{"relative notify socket", Continuous{StallAfter: time.Second, Lookup: env(map[string]string{"NOTIFY_SOCKET": "notify"})}},
		{"watchdog shorter than stall", Continuous{StallAfter: time.Second, Lookup: env(map[string]string{"WATCHDOG_USEC": "500000"})}},
	}
	for _, tc := range cases {
		ran := false
		got := tc.supervisor.Run(context.Background(), func(context.Context, Loop) error {
			ran = true
			return nil
		})
		if got.ExitCode != ExitConfiguration || ran {
			t.Errorf("%s: got %+v, ran=%v", tc.name, got, ran)
		}
	}
}
