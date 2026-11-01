package workermonitor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseShowReadsUnitState(t *testing.T) {
	exited := time.Date(2026, 11, 1, 12, 0, 5, 0, time.UTC)
	cases := []struct {
		name string
		out  string
		want UnitState
	}{
		{
			name: "one-shot service after a partial run",
			out: "LoadState=loaded\nUnitFileState=static\nActiveState=failed\nSubState=failed\nResult=exit-code\n" +
				"ExecMainCode=1\nExecMainStatus=2\nExecMainExitTimestamp=Sun 2026-11-01 12:00:05 UTC\n",
			want: UnitState{LoadState: "loaded", UnitFileState: "static", ActiveState: "failed", SubState: "failed",
				Result: "exit-code", ExecMainCode: 1, ExecMainStatus: 2, ExecMainExit: exited},
		},
		{
			name: "timer without exec properties",
			out:  "LoadState=loaded\nUnitFileState=enabled\nActiveState=active\nSubState=waiting\n",
			want: UnitState{LoadState: "loaded", UnitFileState: "enabled", ActiveState: "active", SubState: "waiting"},
		},
		{
			name: "run in progress with empty exec properties",
			out: "LoadState=loaded\nUnitFileState=static\nActiveState=activating\nSubState=start\nResult=success\n" +
				"ExecMainCode=0\nExecMainStatus=0\nExecMainExitTimestamp=\n",
			want: UnitState{LoadState: "loaded", UnitFileState: "static", ActiveState: "activating", SubState: "start", Result: "success"},
		},
		{
			name: "unit that is not installed",
			out:  "LoadState=not-found\nUnitFileState=\nActiveState=inactive\nSubState=dead\nExecMainExitTimestamp=n/a\n\n",
			want: UnitState{LoadState: "not-found", ActiveState: "inactive", SubState: "dead"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseShow([]byte(tc.out))
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("ParseShow = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseShowRejectsValuesItCannotRead(t *testing.T) {
	cases := map[string]string{
		"code":         "ExecMainCode=exited\n",
		"status":       "ExecMainStatus=2/INVALIDARGUMENT\n",
		"local zone":   "ExecMainExitTimestamp=Sun 2026-11-01 13:00:05 CET\n",
		"unix seconds": "ExecMainExitTimestamp=@1793534405\n",
	}
	for name, out := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseShow([]byte(out)); err == nil {
				t.Fatalf("ParseShow(%q) succeeded, want an error", out)
			}
		})
	}
}

// fakeSystemctlBinary writes a script that stands in for systemctl, so no test runs the
// real one. It fails unless it receives exactly the expected arguments and environment.
func fakeSystemctlBinary(t *testing.T, unit, body string) string {
	t.Helper()
	args := "show --no-pager --property=" + strings.Join(ShowProperties, ",") + " -- " + unit
	script := fmt.Sprintf(`#!/bin/sh
if [ "$*" != '%s' ]; then
	echo "unexpected arguments: $*" >&2
	exit 3
fi
if [ "$TZ" != UTC ] || [ "$LC_ALL" != C ]; then
	echo "unexpected environment: TZ=$TZ LC_ALL=$LC_ALL" >&2
	exit 4
fi
%s
`, args, body)
	path := filepath.Join(t.TempDir(), "systemctl")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExecSystemctlShowsOneUnitInUTC(t *testing.T) {
	t.Setenv("TZ", "America/Sao_Paulo")
	t.Setenv("LC_ALL", "pt_BR.UTF-8")
	unit := "limnopulse-alert-evaluator.service"
	path := fakeSystemctlBinary(t, unit, `printf '%s\n' LoadState=loaded UnitFileState=static ActiveState=inactive SubState=dead \
	Result=success ExecMainCode=1 ExecMainStatus=0 'ExecMainExitTimestamp=Sun 2026-11-01 12:00:05 UTC'`)
	got, err := ExecSystemctl{Path: path}.Show(context.Background(), unit)
	if err != nil {
		t.Fatal(err)
	}
	want := UnitState{LoadState: "loaded", UnitFileState: "static", ActiveState: "inactive", SubState: "dead",
		Result: "success", ExecMainCode: 1, ExecMainExit: time.Date(2026, 11, 1, 12, 0, 5, 0, time.UTC)}
	if got != want {
		t.Fatalf("Show = %+v, want %+v", got, want)
	}
}

func TestExecSystemctlReportsAFailedShow(t *testing.T) {
	unit := "limnopulse-email-worker.service"
	path := fakeSystemctlBinary(t, unit, `echo "Failed to connect to bus: No such file or directory" >&2
exit 1`)
	_, err := ExecSystemctl{Path: path}.Show(context.Background(), unit)
	if err == nil || !strings.Contains(err.Error(), "Failed to connect to bus") {
		t.Fatalf("Show error = %v, want systemctl's stderr", err)
	}
}

func TestExecSystemctlReportsOutputItCannotRead(t *testing.T) {
	unit := "limnopulse-email-worker.service"
	path := fakeSystemctlBinary(t, unit, `echo ExecMainCode=exited`)
	_, err := ExecSystemctl{Path: path}.Show(context.Background(), unit)
	if err == nil || !strings.Contains(err.Error(), unit) {
		t.Fatalf("Show error = %v, want a parse error naming the unit", err)
	}
}

func TestExecSystemctlReportsAMissingBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "systemctl")
	if _, err := (ExecSystemctl{Path: path}).Show(context.Background(), "limnopulse-email-worker.service"); err == nil {
		t.Fatal("Show succeeded without a systemctl binary")
	}
}
