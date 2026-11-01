package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
)

const privateQueue = "http://sqs:9324/queue/private-jobs"

func relayEnvironment(extra map[string]string) func(string) (string, bool) {
	values := map[string]string{
		"AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "private-domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb:8000", "SQS_NOTIFICATION_JOBS_URL": privateQueue,
	}
	for key, value := range extra {
		values[key] = value
	}
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func decodeLine(t *testing.T, output *bytes.Buffer) map[string]any {
	t.Helper()
	var line map[string]any
	if err := json.Unmarshal(output.Bytes(), &line); err != nil {
		t.Fatalf("decode %q: %v", output.String(), err)
	}
	return line
}

func TestRelayRunsOneBoundedRunAndWritesNoPIISummary(t *testing.T) {
	var captured relayconfig.RunConfig
	var capturedDeadline time.Time
	var output bytes.Buffer
	exitCode := runMain(context.Background(), []string{
		"--relay-time=2026-07-16T13:00:00Z", "--shard=1", "--shard-count=2", "--max-work=77",
	}, dependencies{
		Output: &output, LookupEnv: relayEnvironment(nil),
		RunRelay: func(ctx context.Context, config relayconfig.RunConfig, load workercmd.AWSLoader) (relay.RunSummary, error) {
			if load == nil {
				t.Fatal("no AWS loader")
			}
			captured = config
			capturedDeadline, _ = ctx.Deadline()
			return relay.RunSummary{
				RunID: "run_1", RelayTime: *config.RelayTime, Shard: config.Shard, ShardCount: config.ShardCount,
				Result: "success", ExitCode: relay.ExitSuccess, ScopeCompleted: true,
			}, nil
		},
	})
	if exitCode != relay.ExitSuccess || captured.Shard != 1 || captured.ShardCount != 2 || captured.MaxWork != 77 {
		t.Fatalf("exit=%d config=%#v output=%s", exitCode, captured, output.String())
	}
	if captured.BudgetStartedAt == nil || capturedDeadline.After(captured.BudgetStartedAt.Add(45*time.Second)) ||
		capturedDeadline.Before(captured.BudgetStartedAt.Add(44*time.Second)) {
		t.Fatalf("budget start=%v deadline=%s", captured.BudgetStartedAt, capturedDeadline)
	}
	for _, private := range []string{privateQueue, "private-domain", "dynamodb:8000"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("relay summary leaked %q: %s", private, output.String())
		}
	}
	if line := decodeLine(t, &output); line["result"] != "success" || line["run_id"] != "run_1" {
		t.Fatalf("summary = %v", line)
	}
}

func TestRelayConfigurationErrorsExit78BeforeAnyWork(t *testing.T) {
	for name, test := range map[string]struct {
		args   []string
		lookup func(string) (string, bool)
	}{
		"missing environment": {lookup: func(string) (string, bool) { return "", false }},
		"invalid flag value":  {args: []string{"--max-work=0"}, lookup: relayEnvironment(nil)},
		"unknown flag":        {args: []string{"--bogus"}, lookup: relayEnvironment(nil)},
		"help":                {args: []string{"-h"}, lookup: relayEnvironment(nil)},
		"positional":          {args: []string{"relay"}, lookup: relayEnvironment(nil)},
		"credentials":         {lookup: relayEnvironment(map[string]string{"LIMNOPULSE_SUPERVISOR": "systemd"})},
	} {
		t.Run(name, func(t *testing.T) {
			runs := 0
			var output bytes.Buffer
			exitCode := runMain(context.Background(), test.args, dependencies{
				Output: &output, LookupEnv: test.lookup,
				RunRelay: func(context.Context, relayconfig.RunConfig, workercmd.AWSLoader) (relay.RunSummary, error) {
					runs++
					return relay.RunSummary{}, nil
				},
			})
			if exitCode != 78 || runs != 0 {
				t.Fatalf("exit=%d runs=%d output=%s", exitCode, runs, output.String())
			}
			if line := decodeLine(t, &output); line["exit_code"] != float64(78) || line["retry_recommended"] != false {
				t.Fatalf("line = %v", line)
			}
		})
	}
}

func TestRelayKeepsItsRunExitCodes(t *testing.T) {
	for name, test := range map[string]struct {
		summary relay.RunSummary
		err     error
		want    int
	}{
		"partial":           {summary: relay.RunSummary{Result: "partial_failure", ExitCode: relay.ExitPartial}, want: 2},
		"fatal":             {summary: relay.RunSummary{ExitCode: relay.ExitFatal, ErrorCategories: map[string]int{"discovery": 1}}, want: 1},
		"future relay time": {summary: relay.RunSummary{ExitCode: relay.ExitFatal, ErrorCategories: map[string]int{"configuration": 1}}, want: 78},
		"composition":       {err: errors.New("private-domain unreachable"), want: 1},
	} {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			exitCode := runMain(context.Background(), nil, dependencies{
				Output: &output, LookupEnv: relayEnvironment(nil),
				RunRelay: func(context.Context, relayconfig.RunConfig, workercmd.AWSLoader) (relay.RunSummary, error) {
					return test.summary, test.err
				},
			})
			if exitCode != test.want || decodeLine(t, &output)["exit_code"] != float64(test.want) ||
				strings.Contains(output.String(), "private-domain") {
				t.Fatalf("exit=%d output=%s", exitCode, output.String())
			}
		})
	}
}

func TestRelayUsesTheLoadCredentialKeys(t *testing.T) {
	dir := t.TempDir()
	for name, value := range map[string]string{"aws-access-key-id": "AKIDRELAY", "aws-secret-access-key": "relay-secret"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	exitCode := runMain(context.Background(), nil, dependencies{
		Output: &output,
		LookupEnv: relayEnvironment(map[string]string{
			"LIMNOPULSE_SUPERVISOR": "systemd", "CREDENTIALS_DIRECTORY": dir,
		}),
		RunRelay: func(ctx context.Context, config relayconfig.RunConfig, load workercmd.AWSLoader) (relay.RunSummary, error) {
			awsConfig, err := load(ctx, config.AWSRegion, config.DynamoDBEndpoint)
			if err != nil {
				return relay.RunSummary{}, err
			}
			value, err := awsConfig.Credentials.Retrieve(ctx)
			if err != nil || value.AccessKeyID != "AKIDRELAY" {
				t.Errorf("credentials = %q, %v", value.AccessKeyID, err)
			}
			return relay.RunSummary{Result: "success"}, nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit=%d output=%s", exitCode, output.String())
	}
}
