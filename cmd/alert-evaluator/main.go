package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	dynamoadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/dynamo"
	influxadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/influx"
	"github.com/VINIClUS/limnopulse/internal/alertevaluator/telemetry"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func main() {
	os.Exit(runMain(os.Args[1:]))
}

func runMain(args []string) int {
	if len(args) == 0 {
		return writeFailure("usage", "expected run or backfill-schedule")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "run":
		return runEvaluator(ctx, args[1:])
	case "backfill-schedule":
		return runBackfill(ctx, args[1:])
	default:
		return writeFailure("usage", fmt.Sprintf("unknown command %q", args[0]))
	}
}

func runEvaluator(ctx context.Context, args []string) int {
	config, err := alertevaluator.LoadRunConfig(args, os.LookupEnv)
	if err != nil {
		return writeFailure("configuration", err.Error())
	}
	awsConfig, err := loadAWSConfig(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return writeFailure("aws_configuration", err.Error())
	}
	dynamoClient := dynamodb.NewFromConfig(awsConfig, func(options *dynamodb.Options) {
		if config.DynamoDBEndpoint != "" {
			options.BaseEndpoint = aws.String(config.DynamoDBEndpoint)
		}
	})
	influxClient := influxdb2.NewClient(config.InfluxDBURL, config.InfluxDBToken)
	defer influxClient.Close()
	runner := alertevaluator.Runner{
		Store: dynamoadapter.Store{Table: config.DynamoDBTable, Client: dynamoClient},
		Windows: influxadapter.WindowReader{
			Bucket:  config.InfluxDBBucket,
			Querier: influxadapter.ClientQuerier{API: influxClient.QueryAPI(config.InfluxDBOrg)},
		},
	}
	summary := runner.Run(ctx, config)
	metrics, metricsErr := telemetry.New(context.Background(), config.OTLPEndpoint)
	if metricsErr != nil {
		summary.TelemetryExportError = metricsErr.Error()
	} else if metrics != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), config.OTLPFlushTimeout)
		metrics.Record(flushCtx, summary)
		if err := metrics.Shutdown(flushCtx); err != nil {
			summary.TelemetryExportError = err.Error()
		}
		cancel()
	}
	writeJSON(summary)
	return summary.ExitCode
}

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }
func (values *stringList) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("tenant cannot be empty")
	}
	*values = append(*values, value)
	return nil
}

func runBackfill(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("alert-evaluator backfill-schedule", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var tenants stringList
	var evaluationTime string
	var apply bool
	var pageSize int
	fs.Var(&tenants, "tenant", "explicit tenant id; repeat for multiple tenants")
	fs.StringVar(&evaluationTime, "evaluation-time", "", "stable RFC3339 schedule time")
	fs.BoolVar(&apply, "apply", false, "write schedules; default is dry-run")
	fs.IntVar(&pageSize, "page-size", 25, "DynamoDB query page size")
	if err := fs.Parse(args); err != nil {
		return writeFailure("configuration", err.Error())
	}
	now := time.Now().UTC()
	if evaluationTime != "" {
		parsed, err := time.Parse(time.RFC3339Nano, evaluationTime)
		if err != nil {
			return writeFailure("configuration", fmt.Sprintf("evaluation-time: %v", err))
		}
		now = parsed.UTC()
	}
	region := envOr("AWS_REGION", "us-east-1")
	endpoint := envOr("DYNAMODB_ENDPOINT_URL", "")
	awsConfig, err := loadAWSConfig(ctx, region, endpoint)
	if err != nil {
		return writeFailure("aws_configuration", err.Error())
	}
	client := dynamodb.NewFromConfig(awsConfig, func(options *dynamodb.Options) {
		if endpoint != "" {
			options.BaseEndpoint = aws.String(endpoint)
		}
	})
	store := dynamoadapter.Store{Table: envOr("DYNAMODB_DOMAIN_TABLE", "LimnopulseDomain"), Client: client}
	summary, err := store.BackfillSchedule(ctx, dynamoadapter.BackfillOptions{
		Tenants: tenants, EvaluationTime: now, Apply: apply, PageSize: pageSize,
	})
	if err != nil {
		writeJSON(struct {
			Result  string                        `json:"result"`
			Error   string                        `json:"error"`
			Summary dynamoadapter.BackfillSummary `json:"summary"`
		}{Result: "fatal_failure", Error: err.Error(), Summary: summary})
		return alertevaluator.ExitFatal
	}
	writeJSON(struct {
		Result string                        `json:"result"`
		Data   dynamoadapter.BackfillSummary `json:"summary"`
	}{Result: "success", Data: summary})
	return alertevaluator.ExitSuccess
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

func envOr(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func writeFailure(category, message string) int {
	writeJSON(struct {
		Result           string         `json:"result"`
		ExitCode         int            `json:"exit_code"`
		ScopeCompleted   bool           `json:"scope_completed"`
		RetryRecommended bool           `json:"retry_recommended"`
		ErrorCategories  map[string]int `json:"error_categories"`
		Error            string         `json:"error"`
	}{
		Result: "fatal_failure", ExitCode: alertevaluator.ExitFatal,
		ScopeCompleted: false, RetryRecommended: true,
		ErrorCategories: map[string]int{category: 1}, Error: message,
	})
	return alertevaluator.ExitFatal
}

func writeJSON(value any) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}
