package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

const privateJobsQueue = "http://sqs:9324/queue/private-jobs"

func environment(extra map[string]string) func(string) (string, bool) {
	values := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "private-domain",
		"SQS_NOTIFICATION_JOBS_URL": privateJobsQueue, "SES_FROM_EMAIL": "private-sender@example.com",
		"NOTIFICATION_EMAIL_SENDER_MODE": "success",
	}
	for key, value := range extra {
		values[key] = value
	}
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func supervisor(signals chan os.Signal) workerproc.Continuous {
	return workerproc.Continuous{
		StallAfter: time.Second, Lookup: func(string) (string, bool) { return "", false }, Signals: signals,
	}
}

func TestEmailWorkerRunsOnlyTheEmailJobsConsumerUntilStopped(t *testing.T) {
	signals := make(chan os.Signal, 1)
	var captured workerconfig.RunConfig
	var output bytes.Buffer
	code := runMain(context.Background(), []string{"--send-concurrency=3"}, dependencies{
		Output: &output, LookupEnv: environment(nil), Supervisor: supervisor(signals),
		Run: func(ctx context.Context, config workerconfig.RunConfig, load workercmd.AWSLoader, loop worker.Loop) (worker.RunSummary, error) {
			captured = config
			if err := loop.Ready(); err != nil || load == nil {
				t.Errorf("ready=%v load=%t", err, load != nil)
			}
			signals <- syscall.SIGTERM
			<-ctx.Done()
			return worker.RunSummary{Result: "success", Graceful: true, MessagesReceived: 2}, nil
		},
	})
	if code != 0 || captured.SQSQueueURL != privateJobsQueue || captured.SendConcurrency != 3 ||
		captured.SQSFeedbackURL != "" || captured.FeedbackConcurrency != 0 {
		t.Fatalf("exit=%d config=%#v output=%s", code, captured, output.String())
	}
	var summary worker.RunSummary
	if err := json.Unmarshal(output.Bytes(), &summary); err != nil || summary.MessagesReceived != 2 {
		t.Fatalf("summary=%#v err=%v", summary, err)
	}
	for _, private := range []string{privateJobsQueue, "private-domain", "private-sender"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("summary leaked %q: %s", private, output.String())
		}
	}
}

func TestEmailWorkerExits78OnConfigurationErrors(t *testing.T) {
	for name, test := range map[string]struct {
		args   []string
		lookup func(string) (string, bool)
	}{
		"feedback flag":   {args: []string{"--feedback-concurrency=2"}, lookup: environment(nil)},
		"bad flag":        {args: []string{"--send-concurrency=x"}, lookup: environment(nil)},
		"missing sender":  {lookup: environment(map[string]string{"SES_FROM_EMAIL": ""})},
		"no credentials":  {lookup: environment(map[string]string{workerproc.SupervisorEnv: "systemd"})},
		"unknown APP_ENV": {lookup: environment(map[string]string{"APP_ENV": "dev"})},
	} {
		t.Run(name, func(t *testing.T) {
			runs := 0
			var output bytes.Buffer
			code := runMain(context.Background(), test.args, dependencies{
				Output: &output, LookupEnv: test.lookup, Supervisor: supervisor(make(chan os.Signal)),
				Run: func(context.Context, workerconfig.RunConfig, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error) {
					runs++
					return worker.RunSummary{}, nil
				},
			})
			if code != workerproc.ExitConfiguration || runs != 0 {
				t.Fatalf("exit=%d runs=%d output=%s", code, runs, output.String())
			}
		})
	}
}

func TestEmailWorkerExitsOneWhenTheConsumerFails(t *testing.T) {
	var output bytes.Buffer
	code := runMain(context.Background(), nil, dependencies{
		Output: &output, LookupEnv: environment(nil), Supervisor: supervisor(make(chan os.Signal)),
		Run: func(context.Context, workerconfig.RunConfig, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error) {
			return worker.RunSummary{Result: "fatal_failure", ExitCode: 1, Fatal: true}, nil
		},
	})
	if code != workerproc.ExitFailure {
		t.Fatalf("exit=%d output=%s", code, output.String())
	}
}
