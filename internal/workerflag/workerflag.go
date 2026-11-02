// Package workerflag parses ADR-020 worker flags. A bad flag is a configuration error (exit
// 78, through workerproc.ParseFlags); -h or -help is a request for the usage text, which the
// command prints before it exits 0.
package workerflag

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

// Help is a -h or -help request. It wraps flag.ErrHelp and carries the flag set's usage.
type Help struct {
	Usage string
}

func (help *Help) Error() string { return flag.ErrHelp.Error() }

func (help *Help) Unwrap() error { return flag.ErrHelp }

// Parse parses args with workerproc.ParseFlags and returns a *Help for a help request.
func Parse(fs *flag.FlagSet, args []string) error {
	err := workerproc.ParseFlags(fs, args)
	if !errors.Is(err, flag.ErrHelp) {
		return err
	}
	var usage strings.Builder
	fmt.Fprintf(&usage, "Usage of %s:\n", fs.Name())
	fs.SetOutput(&usage)
	fs.PrintDefaults()
	return &Help{Usage: usage.String()}
}
