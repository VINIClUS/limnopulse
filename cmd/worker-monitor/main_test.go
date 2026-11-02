package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/workermonitor"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

var testTable = filepath.Join("..", "..", "internal", "workermonitor", "testdata", "roles.json")

type stubSystemctl struct {
	states map[string]workermonitor.UnitState
	err    error
	calls  int
}

func (stub *stubSystemctl) Show(_ context.Context, unit string) (workermonitor.UnitState, error) {
	stub.calls++
	if stub.err != nil {
		return workermonitor.UnitState{}, stub.err
	}
	return stub.states[unit], nil
}

type stubCloudWatch struct {
	config aws.Config
	inputs []*cloudwatch.PutMetricDataInput
	err    error
}

func (stub *stubCloudWatch) PutMetricData(_ context.Context, input *cloudwatch.PutMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error) {
	stub.inputs = append(stub.inputs, input)
	if stub.err != nil {
		return nil, stub.err
	}
	return &cloudwatch.PutMetricDataOutput{}, nil
}

type harness struct {
	env        environment
	vars       map[string]string
	stdout     *bytes.Buffer
	systemctl  *stubSystemctl
	cloudwatch *stubCloudWatch
}

// newHarness runs the entrypoint with one enabled, running continuous role and the
// process environment of a local run (no CREDENTIALS_DIRECTORY, not under systemd).
func newHarness(t *testing.T) *harness {
	t.Helper()
	isolateAWS(t, "us-east-1")
	h := &harness{
		vars:   make(map[string]string),
		stdout: &bytes.Buffer{},
		systemctl: &stubSystemctl{states: map[string]workermonitor.UnitState{
			"limnopulse-email-worker.service": {LoadState: "loaded", UnitFileState: "enabled", ActiveState: "active", SubState: "running"},
		}},
		cloudwatch: &stubCloudWatch{},
	}
	h.env = environment{
		lookup: func(key string) (string, bool) {
			value, ok := h.vars[key]
			return value, ok
		},
		stdout:    h.stdout,
		systemctl: h.systemctl,
		publisher: func(config aws.Config) workermonitor.MetricPublisher {
			h.cloudwatch.config = config
			return h.cloudwatch
		},
		now: func() time.Time { return time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC) },
	}
	return h
}

// isolateAWS keeps LoadDefaultConfig off the runner's own AWS files and metadata service.
func isolateAWS(t *testing.T, region string) {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"config", "credentials"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", region)
	t.Setenv("AWS_DEFAULT_REGION", "")
}

type summaryLine struct {
	Result    string                 `json:"result"`
	Error     string                 `json:"error"`
	Published int                    `json:"published"`
	Roles     []workermonitor.Report `json:"roles"`
}

func (h *harness) summary(t *testing.T) summaryLine {
	t.Helper()
	var line summaryLine
	if err := json.Unmarshal(h.stdout.Bytes(), &line); err != nil {
		t.Fatalf("summary %q is not JSON: %v", h.stdout.String(), err)
	}
	return line
}

func TestRunMainPublishesUnitHealthAndExitsZero(t *testing.T) {
	h := newHarness(t)
	if code := runMain([]string{"-roles", testTable}, h.env); code != workerproc.ExitSuccess {
		t.Fatalf("exit %d, want 0; summary %s", code, h.stdout.String())
	}
	line := h.summary(t)
	if line.Result != "success" || line.Published != 1 || len(h.cloudwatch.inputs) != 1 {
		t.Fatalf("summary %+v after %d PutMetricData calls", line, len(h.cloudwatch.inputs))
	}
	if h.cloudwatch.config.Region != "us-east-1" {
		t.Fatalf("CloudWatch region = %q", h.cloudwatch.config.Region)
	}
}

func TestRunMainUsesLoadCredentialKeysUnderSystemd(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	keys := map[string]string{
		workerproc.CredentialAWSAccessKeyID:     "test-key-id",
		workerproc.CredentialAWSSecretAccessKey: "test-key-value",
	}
	for name, value := range keys {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h.vars["CREDENTIALS_DIRECTORY"] = dir
	h.vars[workerproc.SupervisorEnv] = "systemd"
	if code := runMain([]string{"-roles", testTable}, h.env); code != workerproc.ExitSuccess {
		t.Fatalf("exit %d, want 0; summary %s", code, h.stdout.String())
	}
	credentials, err := h.cloudwatch.config.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if credentials.AccessKeyID != "test-key-id" {
		t.Fatalf("CloudWatch access key id = %q, want the LoadCredential= one", credentials.AccessKeyID)
	}
}

func TestRunMainExitsConfigurationBeforeAnyQuery(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, h *harness) []string
	}{
		{"unknown flag", func(*testing.T, *harness) []string { return []string{"-roles", testTable, "-interval", "60"} }},
		{"extra argument", func(*testing.T, *harness) []string { return []string{"-roles", testTable, "now"} }},
		{"missing role table", func(t *testing.T, _ *harness) []string {
			return []string{"-roles", filepath.Join(t.TempDir(), "roles.json")}
		}},
		{"invalid role table", func(t *testing.T, _ *harness) []string {
			path := filepath.Join(t.TempDir(), "roles.json")
			if err := os.WriteFile(path, []byte(`{"schema_version": 1}`), 0o600); err != nil {
				t.Fatal(err)
			}
			return []string{"-roles", path}
		}},
		{"systemd without a credentials directory", func(_ *testing.T, h *harness) []string {
			h.vars[workerproc.SupervisorEnv] = "systemd"
			return []string{"-roles", testTable}
		}},
		{"credentials directory without keys", func(t *testing.T, h *harness) []string {
			h.vars["CREDENTIALS_DIRECTORY"] = t.TempDir()
			return []string{"-roles", testTable}
		}},
		{"no region", func(t *testing.T, _ *harness) []string {
			isolateAWS(t, "")
			return []string{"-roles", testTable}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			args := tc.setup(t, h)
			if code := runMain(args, h.env); code != workerproc.ExitConfiguration {
				t.Fatalf("exit %d, want 78; summary %s", code, h.stdout.String())
			}
			if line := h.summary(t); line.Result != "configuration_error" || line.Error == "" {
				t.Fatalf("summary %+v", line)
			}
			if h.systemctl.calls != 0 || len(h.cloudwatch.inputs) != 0 {
				t.Fatalf("%d systemctl queries and %d publishes after a configuration error", h.systemctl.calls, len(h.cloudwatch.inputs))
			}
		})
	}
}

func TestRunMainHelpExitsZero(t *testing.T) {
	h := newHarness(t)
	if code := runMain([]string{"-h"}, h.env); code != workerproc.ExitSuccess {
		t.Fatalf("exit %d, want 0", code)
	}
	if h.stdout.Len() != 0 || h.systemctl.calls != 0 {
		t.Fatalf("help wrote %q and ran %d queries", h.stdout.String(), h.systemctl.calls)
	}
}

func TestRunMainExitsOneWhenAQueryFails(t *testing.T) {
	h := newHarness(t)
	h.systemctl.err = errors.New("Failed to connect to bus")
	if code := runMain([]string{"-roles", testTable}, h.env); code != workerproc.ExitFailure {
		t.Fatalf("exit %d, want 1", code)
	}
	if line := h.summary(t); line.Result != "failure" || line.Published != 0 || len(h.cloudwatch.inputs) != 0 {
		t.Fatalf("summary %+v after %d publishes", line, len(h.cloudwatch.inputs))
	}
}

func TestRunMainExitsOneWhenThePublishFails(t *testing.T) {
	h := newHarness(t)
	h.cloudwatch.err = errors.New("AccessDenied")
	if code := runMain([]string{"-roles", testTable}, h.env); code != workerproc.ExitFailure {
		t.Fatalf("exit %d, want 1", code)
	}
	if line := h.summary(t); line.Result != "failure" || line.Published != 0 || len(line.Roles) == 0 {
		t.Fatalf("summary %+v", line)
	}
}

func TestDefaultEnvironmentUsesSystemctlAndCloudWatch(t *testing.T) {
	env := defaultEnvironment()
	systemctl, ok := env.systemctl.(workermonitor.ExecSystemctl)
	if !ok || systemctl.Path != workermonitor.SystemctlPath {
		t.Fatalf("systemctl = %#v", env.systemctl)
	}
	if _, ok := env.publisher(aws.Config{Region: "us-east-1"}).(*cloudwatch.Client); !ok {
		t.Fatal("publisher is not a CloudWatch client")
	}
	if env.now == nil || env.lookup == nil || env.stdout != os.Stdout {
		t.Fatal("default environment is incomplete")
	}
}
