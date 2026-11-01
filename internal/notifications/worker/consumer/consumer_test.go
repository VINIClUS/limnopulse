package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

func newSupervisor(stallAfter time.Duration, signals chan os.Signal) workerproc.Continuous {
	return workerproc.Continuous{
		StallAfter: stallAfter, StallExitGrace: time.Second,
		Lookup:  func(string) (string, bool) { return "", false },
		Signals: signals,
	}
}

func runAsync(supervisor workerproc.Continuous, output *bytes.Buffer, consume Consume) <-chan int {
	done := make(chan int, 1)
	go func() { done <- Run(context.Background(), supervisor, output, consume) }()
	return done
}

func waitExit(t *testing.T, done <-chan int) int {
	t.Helper()
	select {
	case code := <-done:
		return code
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not exit")
		return -1
	}
}

func decode(t *testing.T, output *bytes.Buffer) worker.RunSummary {
	t.Helper()
	var summary worker.RunSummary
	if err := json.Unmarshal(output.Bytes(), &summary); err != nil {
		t.Fatalf("decode %q: %v", output.String(), err)
	}
	return summary
}

func TestRunExitsZeroAfterAStopAndWritesTheConsumerSummary(t *testing.T) {
	signals := make(chan os.Signal, 1)
	ready := make(chan struct{})
	var output bytes.Buffer
	done := runAsync(newSupervisor(time.Second, signals), &output, func(ctx context.Context, loop worker.Loop) (worker.RunSummary, error) {
		if err := loop.Ready(); err != nil {
			return worker.RunSummary{}, err
		}
		loop.Progress()
		close(ready)
		<-ctx.Done()
		return worker.RunSummary{Result: "success", Graceful: true, MessagesReceived: 3}, nil
	})
	<-ready
	signals <- syscall.SIGTERM
	if code := waitExit(t, done); code != workerproc.ExitSuccess {
		t.Fatalf("exit = %d, output = %s", code, output.String())
	}
	if summary := decode(t, &output); summary.Result != "success" || summary.ExitCode != 0 ||
		summary.MessagesReceived != 3 || summary.Fatal {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunExitsOneWithTheSummaryOfAFatalConsumer(t *testing.T) {
	var output bytes.Buffer
	done := runAsync(newSupervisor(time.Second, make(chan os.Signal)), &output, func(context.Context, worker.Loop) (worker.RunSummary, error) {
		return worker.RunSummary{
			Result: "fatal_failure", ExitCode: 1, Fatal: true,
			ErrorCategories: map[string]int{"queue_receive_failure": 1},
		}, nil
	})
	if code := waitExit(t, done); code != workerproc.ExitFailure {
		t.Fatalf("exit = %d", code)
	}
	summary := decode(t, &output)
	if summary.ExitCode != 1 || summary.ErrorCategories["queue_receive_failure"] != 1 || len(summary.ErrorCategories) != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunExitsOneWhenTheLoopStalls(t *testing.T) {
	var output bytes.Buffer
	done := runAsync(newSupervisor(100*time.Millisecond, make(chan os.Signal)), &output, func(ctx context.Context, _ worker.Loop) (worker.RunSummary, error) {
		<-ctx.Done()
		if !errors.Is(context.Cause(ctx), workerproc.ErrStalled) {
			return worker.RunSummary{}, fmt.Errorf("cancelled by %v", context.Cause(ctx))
		}
		return worker.RunSummary{Result: "success", Graceful: true}, nil
	})
	if code := waitExit(t, done); code != workerproc.ExitFailure {
		t.Fatalf("exit = %d", code)
	}
	summary := decode(t, &output)
	if summary.Result != "fatal_failure" || !summary.Fatal || summary.Graceful ||
		summary.ErrorCategories[workerproc.ReasonStalled] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunMapsStartupErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		wantCode int
		category string
	}{
		{name: "configuration", err: workerproc.Configuration(errors.New("limiter burst")), wantCode: 78, category: "configuration"},
		{name: "runtime", err: errors.New("redis unreachable"), wantCode: 1, category: "startup_failure"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			done := runAsync(newSupervisor(time.Second, make(chan os.Signal)), &output, func(context.Context, worker.Loop) (worker.RunSummary, error) {
				return worker.RunSummary{}, test.err
			})
			if code := waitExit(t, done); code != test.wantCode {
				t.Fatalf("exit = %d", code)
			}
			if summary := decode(t, &output); summary.ExitCode != test.wantCode || summary.ErrorCategories[test.category] != 1 {
				t.Fatalf("summary = %#v", summary)
			}
		})
	}
}

func TestRunExitsZeroWhenStoppedBeforeTheConsumerStarted(t *testing.T) {
	signals := make(chan os.Signal, 1)
	waiting := make(chan struct{})
	var output bytes.Buffer
	done := runAsync(newSupervisor(time.Second, signals), &output, func(ctx context.Context, _ worker.Loop) (worker.RunSummary, error) {
		close(waiting)
		<-ctx.Done()
		return worker.RunSummary{}, fmt.Errorf("connect rate limiter: %w", ctx.Err())
	})
	<-waiting
	signals <- syscall.SIGINT
	if code := waitExit(t, done); code != workerproc.ExitSuccess {
		t.Fatalf("exit = %d", code)
	}
	if summary := decode(t, &output); summary.Result != "success" || summary.ExitCode != 0 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunExitsOneWhenTheConsumerReturnsWithoutAStop(t *testing.T) {
	var output bytes.Buffer
	done := runAsync(newSupervisor(time.Second, make(chan os.Signal)), &output, func(context.Context, worker.Loop) (worker.RunSummary, error) {
		return worker.RunSummary{Result: "success", Graceful: true}, nil
	})
	if code := waitExit(t, done); code != workerproc.ExitFailure {
		t.Fatalf("exit = %d", code)
	}
	if summary := decode(t, &output); summary.ExitCode != 1 || summary.ErrorCategories[workerproc.ReasonUnexpectedExit] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunRefusesAnInvalidSupervisorWithoutStartingTheConsumer(t *testing.T) {
	var output bytes.Buffer
	started := false
	code := Run(context.Background(), workerproc.Continuous{}, &output, func(context.Context, worker.Loop) (worker.RunSummary, error) {
		started = true
		return worker.RunSummary{}, nil
	})
	if code != workerproc.ExitConfiguration || started {
		t.Fatalf("exit = %d, started = %t", code, started)
	}
	if summary := decode(t, &output); summary.ErrorCategories["configuration"] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

type roleConfig struct{ setting, secret string }

func roleEnv(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func testRole(loads, runs *int) Role[roleConfig] {
	return Role[roleConfig]{
		Load: func(args []string, lookup func(string) (string, bool)) (roleConfig, error) {
			*loads++
			setting, _ := lookup("SETTING")
			secret, _ := lookup("SECRET")
			if setting == "" || len(args) != 0 {
				return roleConfig{}, errors.New("SETTING is required")
			}
			return roleConfig{setting: setting, secret: secret}, nil
		},
		Run: func(ctx context.Context, config roleConfig, load workercmd.AWSLoader, loop worker.Loop) (worker.RunSummary, error) {
			*runs++
			if load == nil || config.setting != "on" {
				return worker.RunSummary{}, fmt.Errorf("config = %#v", config)
			}
			if err := loop.Ready(); err != nil {
				return worker.RunSummary{}, err
			}
			<-ctx.Done()
			return worker.RunSummary{Result: "success", Graceful: true}, nil
		},
	}
}

func TestRoleMainLoadsItsConfigurationAndRunsUntilStopped(t *testing.T) {
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	loads, runs := 0, 0
	var output bytes.Buffer
	code := testRole(&loads, &runs).Main(context.Background(), nil, &output,
		roleEnv(map[string]string{"SETTING": "on"}), newSupervisor(time.Second, signals))
	if code != workerproc.ExitSuccess || loads != 1 || runs != 1 {
		t.Fatalf("exit=%d loads=%d runs=%d output=%s", code, loads, runs, output.String())
	}
}

func TestRoleMainExits78BeforeRunningOnABadSettingOrFlag(t *testing.T) {
	for name, args := range map[string][]string{"missing setting": nil, "extra argument": {"extra"}} {
		loads, runs := 0, 0
		var output bytes.Buffer
		environment := map[string]string{}
		if args != nil {
			environment["SETTING"] = "on"
		}
		code := testRole(&loads, &runs).Main(context.Background(), args, &output, roleEnv(environment),
			newSupervisor(time.Second, make(chan os.Signal)))
		if code != workerproc.ExitConfiguration || loads != 1 || runs != 0 {
			t.Fatalf("%s: exit=%d loads=%d runs=%d", name, code, loads, runs)
		}
	}
}

func TestRoleMainExits78WithoutCredentialsUnderSystemd(t *testing.T) {
	loads, runs := 0, 0
	var output bytes.Buffer
	code := testRole(&loads, &runs).Main(context.Background(), nil, &output,
		roleEnv(map[string]string{"SETTING": "on", workerproc.SupervisorEnv: "systemd"}),
		newSupervisor(time.Second, make(chan os.Signal)))
	if code != workerproc.ExitConfiguration || loads != 0 || runs != 0 {
		t.Fatalf("exit=%d loads=%d runs=%d", code, loads, runs)
	}
}

func TestRoleMainServesTheSecretFromItsCredentialOnly(t *testing.T) {
	dir := t.TempDir()
	for name, value := range map[string]string{
		workerproc.CredentialAWSAccessKeyID: "AKID", workerproc.CredentialAWSSecretAccessKey: "secret",
		workerproc.CredentialRedisPassword: "credential-password",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var loaded roleConfig
	role := Role[roleConfig]{
		Load: func(_ []string, lookup func(string) (string, bool)) (roleConfig, error) {
			loaded.secret, _ = lookup("SECRET")
			return loaded, nil
		},
		Run: func(context.Context, roleConfig, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error) {
			return worker.RunSummary{}, errors.New("stop here")
		},
		Secret: &Secret{Credential: workerproc.CredentialRedisPassword, Key: "SECRET"},
	}
	var output bytes.Buffer
	role.Main(context.Background(), nil, &output, roleEnv(map[string]string{"CREDENTIALS_DIRECTORY": dir}),
		newSupervisor(time.Second, make(chan os.Signal)))
	if loaded.secret != "credential-password" {
		t.Fatalf("secret = %q", loaded.secret)
	}
	loaded = roleConfig{}
	output.Reset()
	code := role.Main(context.Background(), nil, &output,
		roleEnv(map[string]string{"CREDENTIALS_DIRECTORY": dir, "SECRET": "env-file-password"}),
		newSupervisor(time.Second, make(chan os.Signal)))
	if code != workerproc.ExitConfiguration || loaded.secret != "" || bytes.Contains(output.Bytes(), []byte("password")) {
		t.Fatalf("exit=%d secret=%q output=%s", code, loaded.secret, output.String())
	}
}
