package workerflag

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

func flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("email-worker", flag.ExitOnError)
	fs.SetOutput(io.Discard)
	fs.Int("send-concurrency", 4, "concurrent email sends")
	return fs
}

func TestParseReturnsTheUsageForAHelpRequest(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"-send-concurrency=2", "-help"}} {
		err := Parse(flagSet(), args)
		var help *Help
		if !errors.As(err, &help) || !errors.Is(err, flag.ErrHelp) || workerproc.IsConfiguration(err) ||
			!strings.HasPrefix(help.Usage, "Usage of email-worker:\n") || !strings.Contains(help.Usage, "-send-concurrency") ||
			err.Error() != flag.ErrHelp.Error() {
			t.Fatalf("args %v: err=%v", args, err)
		}
	}
}

func TestParseKeepsBadFlagsAsConfigurationErrors(t *testing.T) {
	for _, args := range [][]string{{"--send-concurrency=x"}, {"--feedback-concurrency=1"}} {
		err := Parse(flagSet(), args)
		var help *Help
		if !workerproc.IsConfiguration(err) || errors.As(err, &help) {
			t.Fatalf("args %v: err=%v", args, err)
		}
	}
	if err := Parse(flagSet(), []string{"--send-concurrency=2"}); err != nil {
		t.Fatal(err)
	}
}
