package workermonitor

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

var testNow = time.Date(2026, 11, 1, 12, 0, 30, 0, time.UTC)

// fakeSystemctl answers from a map; a unit it does not know reads as not installed, the
// way `systemctl show` reports LoadState=not-found with an empty UnitFileState.
type fakeSystemctl struct {
	states  map[string]UnitState
	err     error
	queried []string
}

func (fake *fakeSystemctl) Show(_ context.Context, unit string) (UnitState, error) {
	fake.queried = append(fake.queried, unit)
	if fake.err != nil {
		return UnitState{}, fake.err
	}
	if state, ok := fake.states[unit]; ok {
		return state, nil
	}
	return UnitState{LoadState: "not-found", ActiveState: "inactive", SubState: "dead"}, nil
}

type fakeCloudWatch struct {
	inputs []*cloudwatch.PutMetricDataInput
	err    error
}

func (fake *fakeCloudWatch) PutMetricData(_ context.Context, input *cloudwatch.PutMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error) {
	fake.inputs = append(fake.inputs, input)
	if fake.err != nil {
		return nil, fake.err
	}
	return &cloudwatch.PutMetricDataOutput{}, nil
}

// published maps each role in the single PutMetricData call to its value.
func (fake *fakeCloudWatch) published(t *testing.T) map[string]float64 {
	t.Helper()
	values := make(map[string]float64)
	if len(fake.inputs) == 0 {
		return values
	}
	if len(fake.inputs) > 1 {
		t.Fatalf("PutMetricData called %d times, want once", len(fake.inputs))
	}
	for _, datum := range fake.inputs[0].MetricData {
		if len(datum.Dimensions) != 1 {
			t.Fatalf("datum dimensions = %+v, want only the role", datum.Dimensions)
		}
		values[aws.ToString(datum.Dimensions[0].Value)] = aws.ToFloat64(datum.Value)
	}
	return values
}

func continuousService(active, sub string) UnitState {
	return UnitState{LoadState: "loaded", UnitFileState: "enabled", ActiveState: active, SubState: sub}
}

func timerUnit(fileState, active string) UnitState {
	return UnitState{LoadState: "loaded", UnitFileState: fileState, ActiveState: active, SubState: "waiting"}
}

// finishedRun is a one-shot service that finished a run with the given exit at the given
// time; systemd leaves it inactive after exit 0 and failed after anything else.
func finishedRun(code, status int, result string, exited time.Time) UnitState {
	state := UnitState{LoadState: "loaded", UnitFileState: "static", ActiveState: "inactive", SubState: "dead",
		Result: result, ExecMainCode: code, ExecMainStatus: status, ExecMainExit: exited}
	if result != resultSuccess {
		state.ActiveState, state.SubState = "failed", "failed"
	}
	return state
}

// runningNow is a one-shot service in the middle of a run: systemd has cleared the
// previous run's Result and ExecMain* properties.
func runningNow() UnitState {
	return UnitState{LoadState: "loaded", UnitFileState: "static", ActiveState: activeStarting, SubState: "start", Result: resultSuccess}
}

const (
	evaluatorService = "limnopulse-alert-evaluator.service"
	evaluatorTimer   = "limnopulse-alert-evaluator.timer"
	emailService     = "limnopulse-email-worker.service"
	monitorService   = "limnopulse-worker-monitor.service"
	monitorTimer     = "limnopulse-worker-monitor.timer"
)

func TestMonitorPublishesUnitHealthForEnabledRolesOnly(t *testing.T) {
	ago := func(seconds int) time.Time { return testNow.Add(-time.Duration(seconds) * time.Second) }
	cases := []struct {
		name   string
		states map[string]UnitState
		want   map[string]float64
	}{
		{
			name:   "nothing installed publishes nothing",
			states: nil,
			want:   map[string]float64{},
		},
		{
			name:   "disabled continuous role",
			states: map[string]UnitState{emailService: {LoadState: "loaded", UnitFileState: "disabled", ActiveState: "inactive", SubState: "dead"}},
			want:   map[string]float64{},
		},
		{
			name:   "disabled continuous role that is still running",
			states: map[string]UnitState{emailService: {LoadState: "loaded", UnitFileState: "disabled", ActiveState: "active", SubState: "running"}},
			want:   map[string]float64{},
		},
		{
			name: "disabled one-shot role with a good last run",
			states: map[string]UnitState{
				evaluatorTimer:   timerUnit("disabled", "inactive"),
				evaluatorService: finishedRun(1, 0, resultSuccess, ago(20)),
			},
			want: map[string]float64{},
		},
		{
			name:   "runtime-enabled timer is not enabled",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled-runtime", "active"), evaluatorService: finishedRun(1, 0, resultSuccess, ago(20))},
			want:   map[string]float64{},
		},
		{
			name:   "healthy continuous role",
			states: map[string]UnitState{emailService: continuousService("active", "running")},
			want:   map[string]float64{"email-worker": 1},
		},
		{
			name:   "inactive continuous role",
			states: map[string]UnitState{emailService: continuousService("inactive", "dead")},
			want:   map[string]float64{"email-worker": 0},
		},
		{
			name:   "failed continuous role",
			states: map[string]UnitState{emailService: continuousService("failed", "failed")},
			want:   map[string]float64{"email-worker": 0},
		},
		{
			name:   "continuous role waiting to restart",
			states: map[string]UnitState{emailService: continuousService("activating", "auto-restart")},
			want:   map[string]float64{"email-worker": 0},
		},
		{
			name:   "continuous role not ready yet",
			states: map[string]UnitState{emailService: continuousService("activating", "start")},
			want:   map[string]float64{"email-worker": 0},
		},
		{
			name:   "healthy one-shot role",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(1, 0, resultSuccess, ago(20))},
			want:   map[string]float64{"alert-evaluator": 1},
		},
		{
			name:   "one-shot run exactly at the stale limit",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(1, 0, resultSuccess, ago(180))},
			want:   map[string]float64{"alert-evaluator": 1},
		},
		{
			name:   "stale one-shot role",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(1, 0, resultSuccess, ago(181))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "one-shot partial run exits 2",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(1, 2, "exit-code", ago(20))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "one-shot runtime failure exits 1",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(1, 1, "exit-code", ago(20))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "one-shot killed by SIGABRT",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(3, 6, "core-dump", ago(20))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "one-shot killed by a signal that reads as status 0",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(2, 0, "signal", ago(20))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "one-shot that failed to start keeps an old exit 0",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: finishedRun(1, 0, "resources", ago(20))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "inactive timer with a good last run",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "inactive"), evaluatorService: finishedRun(1, 0, resultSuccess, ago(20))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "failed timer",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "failed"), evaluatorService: finishedRun(1, 0, resultSuccess, ago(20))},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "one-shot sampled mid-run",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active"), evaluatorService: runningNow()},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "one-shot that never ran since boot",
			states: map[string]UnitState{evaluatorTimer: timerUnit("enabled", "active")},
			want:   map[string]float64{"alert-evaluator": 0},
		},
		{
			name:   "self-report while this run is in progress",
			states: map[string]UnitState{monitorTimer: timerUnit("enabled", "active"), monitorService: runningNow()},
			want:   map[string]float64{SelfRole: 1},
		},
		{
			name:   "self-report with an inactive timer",
			states: map[string]UnitState{monitorTimer: timerUnit("enabled", "inactive"), monitorService: runningNow()},
			want:   map[string]float64{SelfRole: 0},
		},
		{
			name:   "self-report with a disabled timer",
			states: map[string]UnitState{monitorTimer: timerUnit("disabled", "active"), monitorService: runningNow()},
			want:   map[string]float64{},
		},
		{
			name: "mixed roles",
			states: map[string]UnitState{
				monitorTimer:                             timerUnit("enabled", "active"),
				monitorService:                           runningNow(),
				evaluatorTimer:                           timerUnit("enabled", "active"),
				evaluatorService:                         finishedRun(1, 2, "exit-code", ago(20)),
				"limnopulse-notification-relay.timer":    timerUnit("enabled", "active"),
				"limnopulse-notification-relay.service":  finishedRun(1, 0, resultSuccess, ago(70)),
				emailService:                             continuousService("active", "running"),
				"limnopulse-ses-feedback-worker.service": continuousService("failed", "failed"),
				"limnopulse-telegram-worker.service":     {LoadState: "loaded", UnitFileState: "disabled", ActiveState: "inactive", SubState: "dead"},
			},
			want: map[string]float64{
				SelfRole: 1, "alert-evaluator": 0, "notification-relay": 1, "email-worker": 1, "ses-feedback-worker": 0,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := loadTestTable(t)
			systemctl := &fakeSystemctl{states: tc.states}
			publisher := &fakeCloudWatch{}
			summary, err := Monitor{Systemctl: systemctl, Publisher: publisher, Now: func() time.Time { return testNow }}.Run(context.Background(), table)
			if err != nil {
				t.Fatalf("Run error = %v", err)
			}
			got := publisher.published(t)
			if !maps.Equal(got, tc.want) {
				t.Fatalf("published %v, want %v; reports %+v", got, tc.want, summary.Roles)
			}
			if summary.Published != len(tc.want) {
				t.Fatalf("summary published %d, want %d", summary.Published, len(tc.want))
			}
			if len(tc.want) == 0 && len(publisher.inputs) != 0 {
				t.Fatal("PutMetricData called with no enabled role")
			}
			for _, report := range summary.Roles {
				if report.Reason == "" {
					t.Fatalf("report %+v has no reason", report)
				}
			}
		})
	}
}

func TestMonitorNeverQueriesOrPublishesRolesWithoutUnitHealth(t *testing.T) {
	table := loadTestTable(t)
	// Give the unmonitored roles units anyway: unit_health none must still skip them.
	for i := range table.Roles {
		if table.Roles[i].UnitHealth == UnitHealthNone {
			table.Roles[i].Lifecycle = LifecycleContinuous
			table.Roles[i].Units = Units{Service: "limnopulse-" + table.Roles[i].Name + ".service"}
		}
	}
	states := map[string]UnitState{
		"limnopulse-limnopulse-admin.service": continuousService("active", "running"),
		"limnopulse-telemetry-ingest.service": continuousService("active", "running"),
	}
	systemctl := &fakeSystemctl{states: states}
	publisher := &fakeCloudWatch{}
	summary, err := Monitor{Systemctl: systemctl, Publisher: publisher, Now: func() time.Time { return testNow }}.Run(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	for unit := range states {
		if slices.Contains(systemctl.queried, unit) {
			t.Fatalf("queried %s for a role with unit_health none", unit)
		}
	}
	for _, report := range summary.Roles {
		if report.Role == "limnopulse-admin" || report.Role == "telemetry-ingest" {
			t.Fatalf("reported %s, which has unit_health none", report.Role)
		}
	}
	if len(publisher.inputs) != 0 {
		t.Fatalf("published %+v", publisher.inputs)
	}
	want := []string{
		evaluatorService, evaluatorTimer,
		"limnopulse-notification-relay.service", "limnopulse-notification-relay.timer",
		emailService, "limnopulse-ses-feedback-worker.service", "limnopulse-telegram-worker.service",
		monitorService, monitorTimer,
	}
	if !slices.Equal(systemctl.queried, want) {
		t.Fatalf("queried %v, want %v", systemctl.queried, want)
	}
}

func TestMonitorPublishesUnderTheTableNames(t *testing.T) {
	scanned := scanRolesYAML(t)
	health := scanned.blocks["unit_health"]
	table := loadTestTable(t)
	states := map[string]UnitState{emailService: continuousService("active", "running")}
	publisher := &fakeCloudWatch{}
	if _, err := (Monitor{Systemctl: &fakeSystemctl{states: states}, Publisher: publisher, Now: func() time.Time { return testNow }}).Run(context.Background(), table); err != nil {
		t.Fatal(err)
	}
	assertDatum(t, publisher, health["namespace"], health["metric"], health["dimension"], "email-worker", 1)

	// The names are read from the table, never from constants.
	table.UnitHealth.Namespace, table.UnitHealth.Metric, table.UnitHealth.Dimension = "Test/Namespace", "TestMetric", "TestRole"
	publisher = &fakeCloudWatch{}
	if _, err := (Monitor{Systemctl: &fakeSystemctl{states: states}, Publisher: publisher, Now: func() time.Time { return testNow }}).Run(context.Background(), table); err != nil {
		t.Fatal(err)
	}
	assertDatum(t, publisher, "Test/Namespace", "TestMetric", "TestRole", "email-worker", 1)
}

func assertDatum(t *testing.T, publisher *fakeCloudWatch, namespace, metric, dimension, role string, value float64) {
	t.Helper()
	if len(publisher.inputs) != 1 {
		t.Fatalf("PutMetricData called %d times, want once", len(publisher.inputs))
	}
	input := publisher.inputs[0]
	want := types.MetricDatum{
		MetricName: aws.String(metric),
		Dimensions: []types.Dimension{{Name: aws.String(dimension), Value: aws.String(role)}},
		Timestamp:  aws.Time(testNow),
		Unit:       types.StandardUnitNone,
		Value:      aws.Float64(value),
	}
	if aws.ToString(input.Namespace) != namespace || len(input.MetricData) != 1 {
		t.Fatalf("PutMetricData input namespace %q with %d data, want %q with 1", aws.ToString(input.Namespace), len(input.MetricData), namespace)
	}
	got := input.MetricData[0]
	if len(got.Dimensions) != 1 || aws.ToString(got.MetricName) != aws.ToString(want.MetricName) ||
		aws.ToString(got.Dimensions[0].Name) != aws.ToString(want.Dimensions[0].Name) ||
		aws.ToString(got.Dimensions[0].Value) != aws.ToString(want.Dimensions[0].Value) ||
		!aws.ToTime(got.Timestamp).Equal(*want.Timestamp) || got.Unit != want.Unit || aws.ToFloat64(got.Value) != value {
		t.Fatalf("datum = %+v, want %+v", got, want)
	}
}

func TestMonitorPublishesNothingWhenAQueryFails(t *testing.T) {
	publisher := &fakeCloudWatch{}
	systemctl := &fakeSystemctl{err: errors.New("Failed to connect to bus")}
	_, err := Monitor{Systemctl: systemctl, Publisher: publisher, Now: func() time.Time { return testNow }}.Run(context.Background(), loadTestTable(t))
	if err == nil || workerproc.ExitCode(err) != workerproc.ExitFailure {
		t.Fatalf("Run error = %v, want a runtime failure", err)
	}
	if len(publisher.inputs) != 0 {
		t.Fatalf("published %+v after a failed query", publisher.inputs)
	}
}

func TestMonitorReportsAFailedPublishAsARuntimeFailure(t *testing.T) {
	publisher := &fakeCloudWatch{err: errors.New("AccessDenied")}
	states := map[string]UnitState{emailService: continuousService("active", "running")}
	summary, err := Monitor{Systemctl: &fakeSystemctl{states: states}, Publisher: publisher, Now: func() time.Time { return testNow }}.Run(context.Background(), loadTestTable(t))
	if err == nil || workerproc.ExitCode(err) != workerproc.ExitFailure || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("Run error = %v, want a runtime failure", err)
	}
	if summary.Published != 0 || len(publisher.inputs) != 1 {
		t.Fatalf("summary published %d after %d calls, want 0 after 1", summary.Published, len(publisher.inputs))
	}
}
