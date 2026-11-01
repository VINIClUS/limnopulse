package workerproc

import (
	"errors"
	"flag"
)

// ParseFlags parses args into fs and reports a bad flag as a configuration error. It
// switches fs to flag.ContinueOnError first: the default ExitOnError exits 2, which reads
// as a partial one-shot run. -h and -help return flag.ErrHelp unchanged.
func ParseFlags(fs *flag.FlagSet, args []string) error {
	fs.Init(fs.Name(), flag.ContinueOnError)
	err := fs.Parse(args)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return err
	}
	return Configuration(err)
}
