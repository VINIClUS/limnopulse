package workermonitor

import (
	"fmt"
	"slices"
	"time"
)

const (
	unitFileEnabled = "enabled"
	activeActive    = "active"
	activeStarting  = "activating"
	subRunning      = "running"
	resultSuccess   = "success"
	// ExecMainCode is a siginfo si_code. Only CLD_EXITED makes ExecMainStatus an exit
	// code; after CLD_KILLED or CLD_DUMPED it is the signal number.
	codeExited = 1
)

// Report is one monitored role's verdict. Enabled says whether the role passed the
// publish gate; Healthy is the published value and means nothing when Enabled is false.
type Report struct {
	Role    string `json:"role"`
	Enabled bool   `json:"enabled"`
	Healthy bool   `json:"healthy"`
	Reason  string `json:"reason"`
}

// Evaluate judges every monitored role. states holds the `systemctl show` result for each
// of their service and timer units; a missing unit reads as not installed.
func Evaluate(table Table, states map[string]UnitState, now time.Time) []Report {
	roles := table.Monitored()
	reports := make([]Report, 0, len(roles))
	for _, role := range roles {
		if role.Lifecycle == LifecycleContinuous {
			reports = append(reports, evaluateContinuous(role, states[role.Units.Service]))
			continue
		}
		reports = append(reports, evaluateOneshot(role, table.UnitHealth, states[role.Units.Timer], states[role.Units.Service], now))
	}
	return reports
}

// A continuous role publishes when its service is enabled and is healthy only while the
// service is active/running.
func evaluateContinuous(role Role, service UnitState) Report {
	report := Report{Role: role.Name}
	if service.UnitFileState != unitFileEnabled {
		report.Reason = notEnabled(role.Units.Service, service)
		return report
	}
	report.Enabled = true
	if service.ActiveState == activeActive && service.SubState == subRunning {
		report.Healthy = true
		report.Reason = "service active/running"
		return report
	}
	report.Reason = fmt.Sprintf("service %s/%s", service.ActiveState, service.SubState)
	return report
}

// A one-shot role publishes when its timer is enabled; the service itself is static.
func evaluateOneshot(role Role, health UnitHealth, timer, service UnitState, now time.Time) Report {
	report := Report{Role: role.Name}
	if timer.UnitFileState != unitFileEnabled {
		report.Reason = notEnabled(role.Units.Timer, timer)
		return report
	}
	report.Enabled = true
	report.Healthy, report.Reason = oneshotHealth(role, health, timer, service, now)
	return report
}

// oneshotHealth is healthy only when the timer is active and the last finished run exited
// with a success code no longer than oneshot_stale_after_intervals schedules ago.
//
// systemd clears Result and every ExecMain* property when a run starts, so a run in
// progress hides the previous run's outcome. That counts as unhealthy, so worker-monitor's
// service must be ordered After= the other one-shot services and its timer offset from
// theirs: it then samples them between runs. The monitor
// itself is always mid-run when it looks, so for it the current run is the evidence and
// only its timer is checked; a monitor that stopped running publishes nothing at all.
func oneshotHealth(role Role, health UnitHealth, timer, service UnitState, now time.Time) (bool, string) {
	if timer.ActiveState != activeActive {
		return false, fmt.Sprintf("timer %s/%s", timer.ActiveState, timer.SubState)
	}
	if role.Name == SelfRole {
		return true, "timer active; this run is the monitor's own"
	}
	if service.ExecMainExit.IsZero() {
		if service.ActiveState == activeStarting {
			return false, "a run is in progress; the last run's outcome is not visible"
		}
		return false, "no finished run since boot"
	}
	if service.ExecMainCode != codeExited {
		return false, fmt.Sprintf("last run did not exit (code %d, signal or status %d)", service.ExecMainCode, service.ExecMainStatus)
	}
	if !slices.Contains(health.OneshotSuccessExitCodes, service.ExecMainStatus) {
		return false, fmt.Sprintf("last run exited %d", service.ExecMainStatus)
	}
	if service.Result != resultSuccess {
		return false, fmt.Sprintf("last run result %s", service.Result)
	}
	stale := time.Duration(health.OneshotStaleAfterIntervals) * time.Duration(role.ScheduleSeconds) * time.Second
	age := now.Sub(service.ExecMainExit)
	if age > stale {
		return false, fmt.Sprintf("last run finished %s ago; stale after %s", age.Truncate(time.Second), stale)
	}
	return true, fmt.Sprintf("last run exited %d %s ago", service.ExecMainStatus, age.Truncate(time.Second))
}

func notEnabled(unit string, state UnitState) string {
	if state.UnitFileState == "" {
		return fmt.Sprintf("%s is not installed (load state %q); nothing published", unit, state.LoadState)
	}
	return fmt.Sprintf("%s is %s, not enabled; nothing published", unit, state.UnitFileState)
}
