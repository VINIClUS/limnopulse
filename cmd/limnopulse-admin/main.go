// Command limnopulse-admin runs the operator backfills by hand (ADR-020). No worker role runs
// them: each command is one bounded pass over explicit tenants, dry-run unless --apply.
//
//	limnopulse-admin backfill-relay              --tenant=ID|--tenant-file=PATH ... [--apply]
//	limnopulse-admin backfill-telegram           --tenant=ID|--tenant-file=PATH ... [--apply]
//	limnopulse-admin backfill-schedule           --tenant=ID ... [--evaluation-time=RFC3339] [--apply]
//	limnopulse-admin backfill-active-alert-index --tenant=ID ... [--limit=N] [--apply]
//
// Exit codes: 0 success, 1 fatal or incomplete, 2 partial, 78 configuration error.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	alertdynamo "github.com/VINIClUS/limnopulse/internal/alertevaluator/dynamo"
	"github.com/VINIClUS/limnopulse/internal/notifications"
	notificationdynamo "github.com/VINIClUS/limnopulse/internal/notifications/dynamo"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerflag"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// dynamoClient is the DynamoDB surface of both backfill stores.
type dynamoClient interface {
	notificationdynamo.Client
	alertdynamo.Client
}

type dependencies struct {
	Output    io.Writer
	LookupEnv func(string) (string, bool)
	// LoadDynamo builds the client from the AWS loader; tests replace it.
	LoadDynamo func(ctx context.Context, load workercmd.AWSLoader, region, endpoint string) (dynamoClient, error)
}

type commandResult struct {
	Result           string         `json:"result"`
	ExitCode         int            `json:"exit_code"`
	ScopeCompleted   bool           `json:"scope_completed"`
	RetryRecommended bool           `json:"retry_recommended"`
	ErrorCategories  map[string]int `json:"error_categories,omitempty"`
	Summary          any            `json:"summary,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runMain(ctx, os.Args[1:], dependencies{Output: os.Stdout, LookupEnv: os.LookupEnv, LoadDynamo: loadDynamo})
	stop()
	os.Exit(code)
}

func runMain(ctx context.Context, args []string, deps dependencies) int {
	if len(args) == 0 {
		return workercmd.Fail(deps.Output, errUsage, "usage")
	}
	var run func(context.Context, []string, dependencies, workercmd.AWSLoader) int
	switch args[0] {
	case "backfill-relay":
		run = func(ctx context.Context, args []string, deps dependencies, load workercmd.AWSLoader) int {
			return backfillRelay(ctx, "backfill-relay", "", args, deps, load)
		}
	case "backfill-telegram":
		run = func(ctx context.Context, args []string, deps dependencies, load workercmd.AWSLoader) int {
			return backfillRelay(ctx, "backfill-telegram", notifications.ChannelTelegram, args, deps, load)
		}
	case "backfill-schedule":
		run = backfillSchedule
	case "backfill-active-alert-index":
		run = backfillActiveAlertIndex
	default:
		return workercmd.Fail(deps.Output, errUsage, "usage")
	}
	load, _, err := workercmd.AWSLoaderFromEnv(deps.LookupEnv)
	if err != nil {
		return workercmd.Fail(deps.Output, err, "credentials")
	}
	return run(ctx, args[1:], deps, load)
}

// connect reads the table settings and builds the DynamoDB client. A non-zero code means the
// failure line is already written.
func connect(ctx context.Context, deps dependencies, load workercmd.AWSLoader) (dynamoClient, string, int) {
	region := envOr(deps.LookupEnv, "AWS_REGION", "us-east-1")
	endpoint := envOr(deps.LookupEnv, "DYNAMODB_ENDPOINT_URL", "")
	table := envOr(deps.LookupEnv, "DYNAMODB_DOMAIN_TABLE", "LimnopulseDomain")
	for _, required := range []struct{ name, value string }{
		{"AWS_REGION", region}, {"DYNAMODB_DOMAIN_TABLE", table},
	} {
		if strings.TrimSpace(required.value) == "" {
			err := fmt.Errorf("%s must not be empty", required.name)
			return nil, "", workercmd.Fail(deps.Output, workerproc.Configuration(err), "configuration")
		}
	}
	client, err := deps.LoadDynamo(ctx, load, region, endpoint)
	if err != nil {
		return nil, "", workercmd.Fail(deps.Output, err, "aws_configuration")
	}
	return client, table, workerproc.ExitSuccess
}

func loadDynamo(ctx context.Context, load workercmd.AWSLoader, region, endpoint string) (dynamoClient, error) {
	config, err := load(ctx, region, endpoint)
	if err != nil {
		return nil, err
	}
	return dynamodb.NewFromConfig(config), nil
}

type tenantSourceKind uint8

const (
	tenantSourceFlag tenantSourceKind = iota
	tenantSourceFile
)

type tenantSource struct {
	Kind  tenantSourceKind
	Value string
}

type tenantSourceFlagValue struct {
	Kind    tenantSourceKind
	Sources *[]tenantSource
}

func (value tenantSourceFlagValue) String() string { return "" }

func (value tenantSourceFlagValue) Set(raw string) error {
	*value.Sources = append(*value.Sources, tenantSource{Kind: value.Kind, Value: raw})
	return nil
}

// resolveTenants returns the tenants of every --tenant and --tenant-file in first-seen order.
func resolveTenants(sources []tenantSource) ([]string, error) {
	seen := make(map[string]struct{})
	tenants := make([]string, 0, len(sources))
	add := func(raw string) error {
		tenantID := strings.TrimSpace(raw)
		if tenantID == "" || strings.ContainsRune(tenantID, '\x00') {
			return errors.New("--tenant is invalid")
		}
		if _, exists := seen[tenantID]; !exists {
			seen[tenantID] = struct{}{}
			tenants = append(tenants, tenantID)
		}
		return nil
	}
	for _, source := range sources {
		if source.Kind == tenantSourceFlag {
			if err := add(source.Value); err != nil {
				return nil, err
			}
			continue
		}
		lines, err := readTenantFile(source.Value)
		if err != nil {
			return nil, err
		}
		for _, line := range lines {
			if err := add(line); err != nil {
				return nil, err
			}
		}
	}
	if len(tenants) == 0 {
		return nil, errors.New("at least one --tenant is required")
	}
	return tenants, nil
}

func readTenantFile(raw string) ([]string, error) {
	// The errors name the flag only: a path or a tenant ID never reaches the result line.
	path := strings.TrimSpace(raw)
	if path == "" || strings.ContainsRune(path, '\x00') {
		return nil, errors.New("--tenant-file is invalid")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("--tenant-file is not readable")
	}
	if bytes.IndexByte(contents, 0) >= 0 {
		return nil, errors.New("--tenant-file contains NUL")
	}
	var lines []string
	for _, line := range strings.Split(string(contents), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines, nil
}

var errUsage = workerproc.Configuration(errors.New(
	"expected a command: backfill-relay, backfill-telegram, backfill-schedule or backfill-active-alert-index",
))

// parse parses a command's flags. A help request prints the usage through workercmd.Fail;
// a bad flag or a positional argument is a configuration error.
func parse(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(io.Discard)
	if err := workerflag.Parse(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) != 0 {
		return workerproc.Configuration(errors.New("positional arguments are not accepted"))
	}
	return nil
}

// bound is a flag's range check.
type bound struct {
	ok   bool
	flag string
}

func checkBounds(bounds ...bound) error {
	for _, check := range bounds {
		if !check.ok {
			return workerproc.Configuration(fmt.Errorf("--%s is out of range", check.flag))
		}
	}
	return nil
}

func envOr(lookup func(string) (string, bool), key, fallback string) string {
	if value, ok := lookup(key); ok {
		return value
	}
	return fallback
}
