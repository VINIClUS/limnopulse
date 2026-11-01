package workermonitor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Systemctl reads one unit's state. ExecSystemctl runs `systemctl show`; tests use a fake.
type Systemctl interface {
	Show(ctx context.Context, unit string) (UnitState, error)
}

// UnitState is the part of `systemctl show` the health rules read.
//
//   - UnitFileState is the publish gate: only "enabled" publishes.
//   - ActiveState and SubState: a continuous service must be active/running and a timer
//     must be active.
//   - Result, ExecMainCode, ExecMainStatus and ExecMainExitTimestamp describe a one-shot's
//     last finished run. systemd resets all four when a run starts, so they are empty
//     while a run is in progress.
//   - LoadState only explains an empty UnitFileState (a unit that is not installed).
type UnitState struct {
	LoadState      string
	UnitFileState  string
	ActiveState    string
	SubState       string
	Result         string
	ExecMainCode   int
	ExecMainStatus int
	// ExecMainExit is zero when no run has finished since boot or since the current run
	// started.
	ExecMainExit time.Time
}

// ShowProperties are the properties Show asks for, in the order systemctl receives them.
var ShowProperties = []string{
	"LoadState",
	"UnitFileState",
	"ActiveState",
	"SubState",
	"Result",
	"ExecMainCode",
	"ExecMainStatus",
	"ExecMainExitTimestamp",
}

// systemd prints timestamps as "<English weekday> <date> <time> <zone>". Show sets TZ=UTC,
// so the zone must read UTC literally; LC_ALL=C keeps the weekday English on older
// releases that formatted it with strftime.
const showTimestampLayout = "Mon 2006-01-02 15:04:05 UTC"

// SystemctlPath is absolute so the unit never resolves systemctl through PATH.
const SystemctlPath = "/usr/bin/systemctl"

// ExecSystemctl runs `systemctl show` once per unit.
type ExecSystemctl struct {
	Path string
}

func (systemctl ExecSystemctl) Show(ctx context.Context, unit string) (UnitState, error) {
	cmd := exec.CommandContext(ctx, systemctl.Path, "show", "--no-pager",
		"--property="+strings.Join(ShowProperties, ","), "--", unit)
	cmd.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return UnitState{}, fmt.Errorf("systemctl show %s: %w: %s", unit, err, strings.TrimSpace(stderr.String()))
	}
	state, err := ParseShow(out)
	if err != nil {
		return UnitState{}, fmt.Errorf("systemctl show %s: %w", unit, err)
	}
	return state, nil
}

// ParseShow reads `systemctl show` Key=Value lines. A property that is missing or empty
// keeps its zero value: a timer has no ExecMain* properties, and an unset timestamp
// prints empty.
func ParseShow(out []byte) (UnitState, error) {
	values := make(map[string]string)
	for _, line := range strings.Split(string(out), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			values[key] = strings.TrimSpace(value)
		}
	}
	state := UnitState{
		LoadState:     values["LoadState"],
		UnitFileState: values["UnitFileState"],
		ActiveState:   values["ActiveState"],
		SubState:      values["SubState"],
		Result:        values["Result"],
	}
	var err error
	if state.ExecMainCode, err = parseShowInt(values, "ExecMainCode"); err != nil {
		return UnitState{}, err
	}
	if state.ExecMainStatus, err = parseShowInt(values, "ExecMainStatus"); err != nil {
		return UnitState{}, err
	}
	if state.ExecMainExit, err = parseShowTimestamp(values, "ExecMainExitTimestamp"); err != nil {
		return UnitState{}, err
	}
	return state, nil
}

func parseShowInt(values map[string]string, key string) (int, error) {
	value := values[key]
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not an integer", key, value)
	}
	return parsed, nil
}

func parseShowTimestamp(values map[string]string, key string) (time.Time, error) {
	value := values[key]
	if value == "" || value == "n/a" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(showTimestampLayout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s=%q is not a UTC timestamp", key, value)
	}
	return parsed, nil
}
