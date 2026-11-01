// Package workerproc is the process contract every ADR-020 worker role shares: exit
// codes, configuration errors, systemd readiness and watchdog notifications, loop
// progress stall detection, and secrets passed through LoadCredential=.
package workerproc

import "errors"

// Exit codes shared by every worker role (ops/workers/roles.yaml exit_codes). Units
// set RestartPreventExitStatus=78, so a configuration error is never restarted.
const (
	ExitSuccess       = 0
	ExitFailure       = 1
	ExitPartial       = 2
	ExitConfiguration = 78
)

// ConfigurationError marks an error the process cannot recover from by restarting.
type ConfigurationError struct {
	Err error
}

func (err *ConfigurationError) Error() string {
	if err.Err == nil {
		return "configuration error"
	}
	return "configuration error: " + err.Err.Error()
}

func (err *ConfigurationError) Unwrap() error {
	return err.Err
}

// Configuration wraps err as a configuration error; nil stays nil.
func Configuration(err error) error {
	if err == nil || IsConfiguration(err) {
		return err
	}
	return &ConfigurationError{Err: err}
}

func IsConfiguration(err error) bool {
	var configuration *ConfigurationError
	return errors.As(err, &configuration)
}

// ExitCode maps a run error to the shared code: nil is 0, a configuration error is 78,
// anything else is 1. One-shot roles that finish a partial run return ExitPartial
// themselves.
func ExitCode(err error) int {
	switch {
	case err == nil:
		return ExitSuccess
	case IsConfiguration(err):
		return ExitConfiguration
	default:
		return ExitFailure
	}
}
