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

	feedbackconfig "github.com/VINIClUS/limnopulse/internal/notifications/feedback/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

const privateEventsQueue = "http://sqs:9324/queue/private-ses-events"

func environment(extra map[string]string) func(string) (string, bool) {
	// No email settings: the feedback consumer needs none of them.
	values := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "private-domain",
		"SQS_SES_EVENTS_URL": privateEventsQueue,
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

func TestSESFeedbackWorkerRunsOnlyTheFeedbackConsumerUntilStopped(t *testing.T) {
	signals := make(chan os.Signal, 1)
	var captured feedbackconfig.RunConfig
	var output bytes.Buffer
	code := runMain(context.Background(), []string{"--feedback-concurrency=3"}, dependencies{
		Output: &output, LookupEnv: environment(nil), Supervisor: supervisor(signals),
		Run: func(ctx context.Context, config feedbackconfig.RunConfig, load workercmd.AWSLoader, loop worker.Loop) (worker.RunSummary, error) {
			captured = config
			if err := loop.Ready(); err != nil || load == nil {
				t.Errorf("ready=%v load=%t", err, load != nil)
			}
			signals <- syscall.SIGTERM
			<-ctx.Done()
			return worker.RunSummary{Result: "success", Graceful: true,
				FeedbackMetrics: worker.FeedbackMetricsSnapshot{Applied: 4}}, nil
		},
	})
	if code != 0 || captured.SQSQueueURL != privateEventsQueue || captured.Concurrency != 3 {
		t.Fatalf("exit=%d config=%#v output=%s", code, captured, output.String())
	}
	var summary worker.RunSummary
	if err := json.Unmarshal(output.Bytes(), &summary); err != nil || summary.FeedbackMetrics.Applied != 4 {
		t.Fatalf("summary=%#v err=%v", summary, err)
	}
	if strings.Contains(output.String(), privateEventsQueue) || strings.Contains(output.String(), "private-domain") {
		t.Fatalf("summary leaked a queue or table: %s", output.String())
	}
}

func TestSESFeedbackWorkerExits78OnConfigurationErrors(t *testing.T) {
	for name, test := range map[string]struct {
		args   []string
		lookup func(string) (string, bool)
	}{
		"email flag":     {args: []string{"--send-concurrency=2"}, lookup: environment(nil)},
		"missing queue":  {lookup: environment(map[string]string{"SQS_SES_EVENTS_URL": ""})},
		"no credentials": {lookup: environment(map[string]string{workerproc.SupervisorEnv: "systemd"})},
	} {
		t.Run(name, func(t *testing.T) {
			runs := 0
			var output bytes.Buffer
			code := runMain(context.Background(), test.args, dependencies{
				Output: &output, LookupEnv: test.lookup, Supervisor: supervisor(make(chan os.Signal)),
				Run: func(context.Context, feedbackconfig.RunConfig, workercmd.AWSLoader, worker.Loop) (worker.RunSummary, error) {
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
