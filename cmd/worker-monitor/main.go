// Command worker-monitor publishes ADR-020 unit health for every enabled worker role. A
// systemd timer starts it once per schedule interval; each run is one pass with no loop.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VINIClUS/limnopulse/internal/workermonitor"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

// runTimeout bounds one pass well inside the unit's TimeoutStartSec (50 s), so a stuck
// systemctl or CloudWatch call ends as a logged exit 1 instead of a kill.
const runTimeout = 30 * time.Second

func main() {
	os.Exit(runMain(os.Args[1:], defaultEnvironment()))
}

// environment holds what runMain takes from the process, so tests can replace it.
type environment struct {
	lookup    workerproc.LookupEnv
	stdout    io.Writer
	systemctl workermonitor.Systemctl
	publisher func(aws.Config) workermonitor.MetricPublisher
	now       func() time.Time
}

func defaultEnvironment() environment {
	return environment{
		lookup:    os.LookupEnv,
		stdout:    os.Stdout,
		systemctl: workermonitor.ExecSystemctl{Path: workermonitor.SystemctlPath},
		publisher: func(config aws.Config) workermonitor.MetricPublisher {
			return cloudwatch.NewFromConfig(config)
		},
		now: time.Now,
	}
}

func runMain(args []string, env environment) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	summary, err := run(ctx, args, env)
	if errors.Is(err, flag.ErrHelp) {
		return workerproc.ExitSuccess
	}
	writeSummary(env.stdout, summary, err)
	return workerproc.ExitCode(err)
}

// run checks every input before it queries a unit, so a configuration error always exits
// 78 and never publishes.
func run(ctx context.Context, args []string, env environment) (workermonitor.Summary, error) {
	options, err := parseFlags(args)
	if err != nil {
		return workermonitor.Summary{}, err
	}
	table, err := workermonitor.LoadTable(options.roles)
	if err != nil {
		return workermonitor.Summary{}, err
	}
	config, err := loadAWSConfig(ctx, env.lookup)
	if err != nil {
		return workermonitor.Summary{}, err
	}
	monitor := workermonitor.Monitor{
		Systemctl: env.systemctl,
		Publisher: env.publisher(config),
		Now:       env.now,
	}
	return monitor.Run(ctx, table)
}

type flags struct {
	roles string
}

func parseFlags(args []string) (flags, error) {
	fs := flag.NewFlagSet("worker-monitor", flag.ContinueOnError)
	var options flags
	fs.StringVar(&options.roles, "roles", workermonitor.DefaultTablePath, "role table: the workers bundle's roles.json")
	if err := workerproc.ParseFlags(fs, args); err != nil {
		return flags{}, err
	}
	if fs.NArg() > 0 {
		return flags{}, workerproc.Configuration(fmt.Errorf("unexpected arguments %q", fs.Args()))
	}
	return options, nil
}

// loadAWSConfig takes the keys from LoadCredential= under systemd and the default chain
// on local runs. A missing region is a configuration error, not a failed publish.
func loadAWSConfig(ctx context.Context, lookup workerproc.LookupEnv) (aws.Config, error) {
	credentials, err := workerproc.CredentialsFromEnv(lookup)
	if err != nil {
		return aws.Config{}, err
	}
	options, err := credentials.AWSLoadOptions()
	if err != nil {
		return aws.Config{}, err
	}
	config, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return aws.Config{}, workerproc.Configuration(fmt.Errorf("load AWS config: %w", err))
	}
	if config.Region == "" {
		return aws.Config{}, workerproc.Configuration(errors.New("no AWS region: set AWS_REGION"))
	}
	return config, nil
}

func writeSummary(out io.Writer, summary workermonitor.Summary, err error) {
	line := struct {
		Result    string                 `json:"result"`
		Error     string                 `json:"error,omitempty"`
		Published int                    `json:"published"`
		Roles     []workermonitor.Report `json:"roles,omitempty"`
	}{Result: "success", Published: summary.Published, Roles: summary.Roles}
	if err != nil {
		line.Result = "failure"
		if workerproc.IsConfiguration(err) {
			line.Result = "configuration_error"
		}
		line.Error = err.Error()
	}
	_ = json.NewEncoder(out).Encode(line)
}
