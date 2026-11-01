package workerproc

import (
	"errors"
	"flag"
	"io"
	"testing"
)

func flagSet() (*flag.FlagSet, *string) {
	// ExitOnError would end the test binary if ParseFlags kept it.
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	fs.SetOutput(io.Discard)
	queue := fs.String("queue", "", "queue URL")
	return fs, queue
}

func TestParseFlagsAcceptsKnownFlags(t *testing.T) {
	fs, queue := flagSet()
	if err := ParseFlags(fs, []string{"-queue", "jobs"}); err != nil || *queue != "jobs" {
		t.Fatalf("got %q, %v", *queue, err)
	}
}

func TestParseFlagsReportsABadFlagAsAConfigurationError(t *testing.T) {
	fs, _ := flagSet()
	err := ParseFlags(fs, []string{"-unknown"})
	if !IsConfiguration(err) || ExitCode(err) != ExitConfiguration {
		t.Fatalf("got %v", err)
	}
}

func TestParseFlagsReturnsHelpUnchanged(t *testing.T) {
	fs, _ := flagSet()
	err := ParseFlags(fs, []string{"-h"})
	if !errors.Is(err, flag.ErrHelp) || IsConfiguration(err) {
		t.Fatalf("got %v", err)
	}
}
