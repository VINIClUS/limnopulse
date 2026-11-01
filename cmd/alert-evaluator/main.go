// Command alert-evaluator is the ADR-020 alert-evaluator role: one bounded pass over the
// alert rules per invocation, started by its systemd timer. The schedule and active-alert
// index backfills moved to cmd/limnopulse-admin.
//
// Exit codes: 0 success, 1 fatal, 2 partial (the next run retries), 78 configuration error.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	dynamoadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/dynamo"
	influxadapter "github.com/VINIClUS/limnopulse/internal/alertevaluator/influx"
	"github.com/VINIClUS/limnopulse/internal/alertevaluator/telemetry"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
)

type dependencies struct {
	Output    io.Writer
	LookupEnv func(string) (string, bool)
	Evaluate  func(context.Context, alertevaluator.RunConfig, workercmd.AWSLoader) (alertevaluator.RunSummary, error)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runMain(ctx, os.Args[1:], dependencies{Output: os.Stdout, LookupEnv: os.LookupEnv, Evaluate: evaluate})
	stop()
	os.Exit(code)
}

func runMain(ctx context.Context, args []string, deps dependencies) int {
	if len(args) == 0 || args[0] != "run" {
		return writeFailure(deps.Output, "usage", workerproc.Configuration(
			fmt.Errorf("expected run; the backfills are limnopulse-admin commands"),
		))
	}
	load, creds, err := workercmd.AWSLoaderFromEnv(deps.LookupEnv)
	if err != nil {
		return writeFailure(deps.Output, "credentials", err)
	}
	// Under systemd the token is the influxdb-token credential, never an env-file value.
	lookup, err := workercmd.SecretLookup(deps.LookupEnv, creds, workerproc.CredentialInfluxDBToken, "INFLUXDB_TOKEN")
	if err != nil {
		return writeFailure(deps.Output, "credentials", err)
	}
	config, err := alertevaluator.LoadRunConfig(args[1:], lookup)
	if err != nil {
		return writeFailure(deps.Output, "configuration", workerproc.Configuration(err))
	}
	summary, err := deps.Evaluate(ctx, config, load)
	if err != nil {
		return writeFailure(deps.Output, "aws_configuration", err)
	}
	workercmd.WriteJSON(deps.Output, summary)
	return summary.ExitCode
}

// evaluate composes the DynamoDB and InfluxDB adapters, runs one pass and exports its metrics.
func evaluate(ctx context.Context, config alertevaluator.RunConfig, load workercmd.AWSLoader) (alertevaluator.RunSummary, error) {
	awsConfig, err := load(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return alertevaluator.RunSummary{}, err
	}
	influxClient := influxdb2.NewClient(config.InfluxDBURL, config.InfluxDBToken)
	defer influxClient.Close()
	runner := newRunner(config, dynamodb.NewFromConfig(awsConfig), influxClient.QueryAPI(config.InfluxDBOrg))
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
	return summary, nil
}

func newRunner(config alertevaluator.RunConfig, dynamoClient *dynamodb.Client, queryAPI influxapi.QueryAPI) alertevaluator.Runner {
	return alertevaluator.Runner{
		Store: dynamoadapter.Store{
			Table: config.DynamoDBTable, Client: dynamoClient,
			TelegramDeliveryEnabled: config.TelegramDeliveryEnabled,
		},
		Windows: windowReaderFor(config, queryAPI),
	}
}

// windowReaderFor selects the window reader for config.TelemetrySource.
func windowReaderFor(config alertevaluator.RunConfig, queryAPI influxapi.QueryAPI) alertevaluator.WindowReader {
	if config.TelemetrySource == alertevaluator.TelemetrySourceV2 {
		return influxadapter.V2WindowReader{Bucket: config.InfluxDBBucket, Querier: influxadapter.ClientV2Querier{API: queryAPI}}
	}
	return influxadapter.WindowReader{Bucket: config.InfluxDBBucket, Querier: influxadapter.ClientQuerier{API: queryAPI}}
}

// writeFailure writes the failure line and returns its exit code: 78 for a configuration
// error, which no rerun can fix, and 1 otherwise. No error here carries a secret value.
func writeFailure(output io.Writer, category string, err error) int {
	code := workerproc.ExitCode(err)
	workercmd.WriteJSON(output, struct {
		workercmd.Failure
		Error string `json:"error"`
	}{
		Failure: workercmd.Failure{
			Result: "fatal_failure", ExitCode: code,
			RetryRecommended: code != workerproc.ExitConfiguration,
			ErrorCategories:  map[string]int{category: 1},
		},
		Error: err.Error(),
	})
	return code
}
