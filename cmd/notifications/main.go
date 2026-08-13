package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	notificationdynamo "github.com/VINIClUS/limnopulse/internal/notifications/dynamo"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
	relaydynamo "github.com/VINIClUS/limnopulse/internal/notifications/relay/dynamo"
	relaysqs "github.com/VINIClUS/limnopulse/internal/notifications/relay/sqs"
	relaytelemetry "github.com/VINIClUS/limnopulse/internal/notifications/relay/telemetry"
	telegramworkerconfig "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	exitSuccess            = 0
	exitFatal              = 1
	exitPartial            = 2
	maxPageSize            = 1<<31 - 1
	defaultBackfillTimeout = 5 * time.Minute
	maxBackfillTimeout     = 24 * time.Hour
)

type loadDynamoFunc func(context.Context, string, string) (notificationdynamo.Client, error)
type runRelayFunc func(context.Context, relayconfig.RunConfig) (relay.RunSummary, error)
type runWorkerFunc func(context.Context, workerconfig.RunConfig) (worker.RunSummary, error)
type runTelegramWorkerFunc func(context.Context, telegramworkerconfig.RunConfig) (worker.RunSummary, error)

type dependencies struct {
	Output            io.Writer
	LookupEnv         func(string) (string, bool)
	LoadDynamo        loadDynamoFunc
	RunRelay          runRelayFunc
	RunWorker         runWorkerFunc
	RunTelegramWorker runTelegramWorkerFunc
}

type commandResult struct {
	Result           string                              `json:"result"`
	ExitCode         int                                 `json:"exit_code"`
	ScopeCompleted   bool                                `json:"scope_completed"`
	RetryRecommended bool                                `json:"retry_recommended"`
	ErrorCategories  map[string]int                      `json:"error_categories,omitempty"`
	Summary          *notificationdynamo.BackfillSummary `json:"summary,omitempty"`
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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runMain(ctx, os.Args[1:], defaultDependencies()))
}

func defaultDependencies() dependencies {
	return dependencies{
		Output:            os.Stdout,
		LookupEnv:         os.LookupEnv,
		LoadDynamo:        loadDynamo,
		RunRelay:          executeRelay,
		RunWorker:         executeWorker,
		RunTelegramWorker: executeTelegramWorker,
	}
}

func runMain(ctx context.Context, args []string, deps dependencies) int {
	if len(args) == 0 {
		return writeFatal(deps.Output, "configuration")
	}
	switch args[0] {
	case "worker":
		return runWorkerCommand(ctx, args[1:], deps)
	case "telegram-worker":
		return runTelegramWorkerCommand(ctx, args[1:], deps)
	case "relay":
		return runRelayCommand(ctx, args[1:], deps)
	case "backfill-relay":
		return runBackfillRelay(ctx, args[1:], deps)
	case "backfill-telegram":
		return runBackfillTelegram(ctx, args[1:], deps)
	default:
		return writeFatal(deps.Output, "configuration")
	}
}

func runRelayCommand(ctx context.Context, args []string, deps dependencies) int {
	budgetStartedAt := time.Now().UTC()
	config, err := relayconfig.Load(args, deps.LookupEnv)
	if err != nil || deps.RunRelay == nil {
		return writeFatal(deps.Output, "configuration")
	}
	config.BudgetStartedAt = &budgetStartedAt
	runCtx, cancel := context.WithDeadline(ctx, budgetStartedAt.Add(config.GlobalDeadline))
	defer cancel()
	summary, err := deps.RunRelay(runCtx, config)
	if err != nil {
		return writeFatal(deps.Output, "aws_configuration")
	}
	writeResult(deps.Output, summary)
	return summary.ExitCode
}

func runBackfillRelay(ctx context.Context, args []string, deps dependencies) int {
	return runBackfill(ctx, args, deps, "", "notifications backfill-relay")
}

func runBackfillTelegram(ctx context.Context, args []string, deps dependencies) int {
	return runBackfill(
		ctx, args, deps, notifications.ChannelTelegram, "notifications backfill-telegram",
	)
}

func runBackfill(
	ctx context.Context,
	args []string,
	deps dependencies,
	targetChannel notifications.Channel,
	commandName string,
) int {
	fs := flag.NewFlagSet(commandName, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var sources []tenantSource
	var apply bool
	var pageSize int
	var maxRows int
	var timeout time.Duration
	fs.Var(tenantSourceFlagValue{Kind: tenantSourceFlag, Sources: &sources}, "tenant", "explicit tenant ID; repeatable")
	fs.Var(tenantSourceFlagValue{Kind: tenantSourceFile, Sources: &sources}, "tenant-file", "file containing tenant IDs; repeatable")
	fs.BoolVar(&apply, "apply", false, "write relay fields; default is dry-run")
	fs.IntVar(&pageSize, "page-size", 25, "DynamoDB query page size")
	fs.IntVar(&maxRows, "max-rows", notificationdynamo.DefaultBackfillMaxRows, "maximum migration candidates per run")
	fs.DurationVar(&timeout, "timeout", defaultBackfillTimeout, "total backfill deadline")
	if err := fs.Parse(args); err != nil || len(fs.Args()) != 0 || pageSize < 1 || pageSize > maxPageSize ||
		maxRows < 1 || timeout <= 0 || timeout > maxBackfillTimeout {
		return writeFatal(deps.Output, "configuration")
	}
	tenants, err := resolveTenants(sources)
	if err != nil {
		return writeFatal(deps.Output, "tenant_scope")
	}

	region := envOr(deps.LookupEnv, "AWS_REGION", "us-east-1")
	endpoint := envOr(deps.LookupEnv, "DYNAMODB_ENDPOINT_URL", "")
	table := envOr(deps.LookupEnv, "DYNAMODB_DOMAIN_TABLE", "LimnopulseDomain")
	if strings.TrimSpace(region) == "" || strings.TrimSpace(table) == "" {
		return writeFatal(deps.Output, "configuration")
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, err := deps.LoadDynamo(runCtx, region, endpoint)
	if err != nil {
		return writeFatal(deps.Output, "aws_configuration")
	}
	summary, err := (notificationdynamo.Store{Table: table, Client: client}).BackfillRelay(
		runCtx,
		notificationdynamo.BackfillOptions{
			Tenants: tenants, Apply: apply, PageSize: pageSize, MaxRows: maxRows,
			TargetChannel: targetChannel,
		},
	)
	if err != nil {
		writeResult(deps.Output, commandResult{
			Result: "fatal_failure", ExitCode: exitFatal, ScopeCompleted: false,
			RetryRecommended: true, ErrorCategories: map[string]int{"query_failure": 1}, Summary: &summary,
		})
		return exitFatal
	}
	incomplete := summary.LimitReached || summary.DeadlineReached
	if incomplete {
		writeResult(deps.Output, commandResult{
			Result: "incomplete", ExitCode: exitFatal, ScopeCompleted: false,
			RetryRecommended: true, ErrorCategories: summaryErrorCategories(summary), Summary: &summary,
		})
		return exitFatal
	}
	if summary.RowFailures > 0 {
		writeResult(deps.Output, commandResult{
			Result: "partial_failure", ExitCode: exitPartial, ScopeCompleted: true,
			RetryRecommended: true, ErrorCategories: summaryErrorCategories(summary), Summary: &summary,
		})
		return exitPartial
	}
	writeResult(deps.Output, commandResult{
		Result: "success", ExitCode: exitSuccess, ScopeCompleted: true,
		RetryRecommended: false, Summary: &summary,
	})
	return exitSuccess
}

func resolveTenants(sources []tenantSource) ([]string, error) {
	seen := make(map[string]struct{})
	tenants := make([]string, 0, len(sources))
	add := func(raw string) error {
		tenantID := strings.TrimSpace(raw)
		if tenantID == "" || strings.ContainsRune(tenantID, '\x00') {
			return fmt.Errorf("tenant ID is invalid")
		}
		if _, exists := seen[tenantID]; !exists {
			seen[tenantID] = struct{}{}
			tenants = append(tenants, tenantID)
		}
		return nil
	}
	for _, source := range sources {
		switch source.Kind {
		case tenantSourceFlag:
			if err := add(source.Value); err != nil {
				return nil, err
			}
		case tenantSourceFile:
			path := strings.TrimSpace(source.Value)
			if path == "" || strings.ContainsRune(path, '\x00') {
				return nil, fmt.Errorf("tenant file path is invalid")
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read tenant file: %w", err)
			}
			if bytes.IndexByte(contents, 0) >= 0 {
				return nil, fmt.Errorf("tenant file contains NUL")
			}
			for _, line := range strings.Split(string(contents), "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				if err := add(line); err != nil {
					return nil, err
				}
			}
		default:
			return nil, fmt.Errorf("tenant source is invalid")
		}
	}
	if len(tenants) == 0 {
		return nil, fmt.Errorf("at least one explicit tenant is required")
	}
	return tenants, nil
}

func summaryErrorCategories(summary notificationdynamo.BackfillSummary) map[string]int {
	categories := make(map[string]int)
	for category, count := range map[string]int{
		"schema_conflict":   summary.SchemaConflicts,
		"concurrent_change": summary.ConcurrentChanges,
		"decode_failure":    summary.DecodeFailures,
		"update_failure":    summary.UpdateFailures,
	} {
		if count > 0 {
			categories[category] = count
		}
	}
	if summary.LimitReached {
		categories["row_limit_reached"] = 1
	}
	if summary.DeadlineReached {
		categories["deadline_reached"] = 1
	}
	return categories
}

func writeFatal(output io.Writer, category string) int {
	writeResult(output, commandResult{
		Result: "fatal_failure", ExitCode: exitFatal, ScopeCompleted: false,
		RetryRecommended: true, ErrorCategories: map[string]int{category: 1},
	})
	return exitFatal
}

func writeResult(output io.Writer, result any) {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(result)
}

func envOr(lookup func(string) (string, bool), key, fallback string) string {
	if value, ok := lookup(key); ok {
		return value
	}
	return fallback
}

func loadDynamo(ctx context.Context, region, endpoint string) (notificationdynamo.Client, error) {
	config, err := loadAWSConfig(ctx, region, endpoint)
	if err != nil {
		return nil, err
	}
	return awssdk.NewFromConfig(config, func(options *awssdk.Options) {
		if endpoint != "" {
			options.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

func loadAWSConfig(ctx context.Context, region, endpoint string) (aws.Config, error) {
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if endpoint != "" {
		options = append(options, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", ""),
		))
	}
	return awsconfig.LoadDefaultConfig(ctx, options...)
}

func executeRelay(ctx context.Context, config relayconfig.RunConfig) (relay.RunSummary, error) {
	awsConfig, err := loadAWSConfig(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return relay.RunSummary{}, err
	}
	dynamoClient := awssdk.NewFromConfig(awsConfig, func(options *awssdk.Options) {
		if config.DynamoDBEndpoint != "" {
			options.BaseEndpoint = aws.String(config.DynamoDBEndpoint)
		}
	})
	sqsClient := awssqs.NewFromConfig(awsConfig, func(options *awssqs.Options) {
		if config.SQSEndpoint != "" {
			options.BaseEndpoint = aws.String(config.SQSEndpoint)
		}
	})
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		return relay.RunSummary{}, err
	}
	runner := relay.Runner{
		Store: relaydynamo.Store{
			Table: config.DynamoDBTable, Client: dynamoClient, Renderer: renderer,
			WebURL:              config.WebURL,
			AllowInsecureWebURL: config.AppEnv == "local" || config.AppEnv == "test",
		},
		Publisher: relaysqs.Router{
			TelegramEnabled: config.TelegramDeliveryEnabled,
			Email: relaysqs.Publisher{
				Client: sqsClient, QueueURL: config.SQSQueueURL,
				RequestTimeout: config.SQSRequestTimeout,
			},
			Telegram: relaysqs.Publisher{
				Client: sqsClient, QueueURL: config.SQSTelegramQueueURL,
				RequestTimeout: config.SQSRequestTimeout,
			},
		},
	}
	summary := runner.Run(ctx, config)
	flushCtx, cancel := newRelayTelemetryFlushContext(ctx, config.OTLPFlushTimeout)
	defer cancel()
	recorder, metricsErr := relaytelemetry.New(flushCtx, config.OTLPEndpoint)
	if metricsErr != nil {
		summary.TelemetryExportError = "initialization_failed"
		return summary, nil
	}
	if recorder != nil {
		recorder.Record(flushCtx, summary)
		if err := recorder.Shutdown(flushCtx); err != nil {
			summary.TelemetryExportError = "export_failed"
		}
	}
	return summary, nil
}

func newRelayTelemetryFlushContext(
	ctx context.Context,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}
