package workerproc

import (
	"errors"
	"fmt"
	"testing"
)

func TestExitCodeMapsTheSharedContract(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, ExitSuccess},
		{"configuration", Configuration(errors.New("missing queue url")), ExitConfiguration},
		{"wrapped configuration", fmt.Errorf("load: %w", Configuration(errors.New("bad flag"))), ExitConfiguration},
		{"runtime failure", errors.New("receive failed"), ExitFailure},
	}
	for _, tc := range cases {
		if got := ExitCode(tc.err); got != tc.want {
			t.Errorf("%s: ExitCode = %d, want %d", tc.name, got, tc.want)
		}
	}
	if ExitSuccess != 0 || ExitFailure != 1 || ExitPartial != 2 || ExitConfiguration != 78 {
		t.Fatal("exit codes drifted from ops/workers/roles.yaml")
	}
}

func TestConfigurationKeepsNilAndDoesNotDoubleWrap(t *testing.T) {
	if Configuration(nil) != nil {
		t.Fatal("Configuration(nil) must stay nil")
	}
	base := errors.New("missing table")
	once := Configuration(base)
	if Configuration(once) != once {
		t.Fatal("an existing configuration error must not be wrapped again")
	}
	if !errors.Is(once, base) || !IsConfiguration(once) {
		t.Fatal("configuration error must unwrap to its cause")
	}
}
