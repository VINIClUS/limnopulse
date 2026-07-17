package main

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	workerdynamo "github.com/VINIClUS/limnopulse/internal/notifications/worker/dynamo"
	workerses "github.com/VINIClUS/limnopulse/internal/notifications/worker/ses"
	workersqs "github.com/VINIClUS/limnopulse/internal/notifications/worker/sqs"
	workertelemetry "github.com/VINIClUS/limnopulse/internal/notifications/worker/telemetry"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

func runWorkerCommand(ctx context.Context, args []string, deps dependencies) int {
	config, err := workerconfig.Load(args, deps.LookupEnv)
	if err != nil || deps.RunWorker == nil {
		return writeFatal(deps.Output, "configuration")
	}
	summary, err := deps.RunWorker(ctx, config)
	if err != nil {
		return writeFatal(deps.Output, "aws_configuration")
	}
	writeResult(deps.Output, summary)
	return summary.ExitCode
}

func executeWorker(ctx context.Context, config workerconfig.RunConfig) (worker.RunSummary, error) {
	credentialEndpoint := firstEndpoint(config.DynamoDBEndpoint, config.SQSEndpoint, config.SESEndpoint)
	awsConfig, err := loadAWSConfig(ctx, config.AWSRegion, credentialEndpoint)
	if err != nil {
		return worker.RunSummary{}, err
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
	queue := workersqs.Queue{
		Client: sqsClient, QueueURL: config.SQSQueueURL,
		ReceiveTimeout: config.SQSReceiveTimeout, MutationTimeout: config.SQSRequestTimeout,
	}
	var sender worker.EmailSender
	if config.EmailSenderMode == "aws" {
		sender = workerses.Sender{
			Client:    workerses.NewClient(awsConfig, config.SESEndpoint),
			FromEmail: config.SESFromEmail, ConfigurationSet: config.ConfigurationSet,
			Timeout: config.ProviderTimeout,
		}
	} else {
		sender = workerses.FakeSender{Mode: workerses.FakeMode(config.EmailSenderMode)}
	}
	limiter, err := worker.NewTokenLimiter(config.MaxSendRate, config.SendBurst)
	if err != nil {
		return worker.RunSummary{}, err
	}
	recorder, telemetryErr := workertelemetry.New(ctx, config.OTLPEndpoint)
	metrics, telemetryState := newWorkerMetrics(config.MaxSendRate, recorder, telemetryErr)
	store := workerdynamo.Store{Table: config.DynamoDBTable, Client: dynamoClient}
	owner := "worker_" + uuid.NewString()
	processor := worker.Processor{
		Store: store, Sender: sender, Limiter: limiter, Owner: owner,
		ProcessingLease: config.ProcessingLease, ProviderTimeout: config.ProviderTimeout,
		InvalidVisibility: config.InvalidVisibility, Now: func() time.Time { return time.Now().UTC() },
		NewAttemptID:   func() string { return "att_" + uuid.NewString() },
		JitterFraction: rand.Float64, Metrics: metrics,
	}
	processor.Guard = worker.RenewalGuard{
		Store: store, Queue: queue, Interval: config.RenewalInterval,
		LeaseTTL: config.ProcessingLease, Visibility: config.VisibilityTimeout,
		Now: func() time.Time { return time.Now().UTC() },
	}
	runner := worker.Runner{
		Queue: queue, Handler: processor, Concurrency: config.SendConcurrency,
		ReceiveBatch: config.ReceiveBatch, ReceiveWait: config.ReceiveWait,
		Visibility: config.VisibilityTimeout, DrainTimeout: config.DrainTimeout,
	}
	summary := runner.Run(ctx)
	summary.Metrics = metrics.Snapshot()
	if telemetryState != "" {
		summary.TelemetryExportError = telemetryState
	} else {
		flushCtx, cancelFlush := context.WithTimeout(context.WithoutCancel(ctx), config.OTLPFlushTimeout)
		defer cancelFlush()
		if err := recorder.Shutdown(flushCtx); err != nil {
			summary.TelemetryExportError = "export_failed"
		}
	}
	return summary, nil
}

func newWorkerMetrics(
	rate float64,
	observer worker.MetricsObserver,
	initializationErr error,
) (*worker.Metrics, string) {
	if initializationErr != nil {
		return worker.NewMetrics(rate), "initialization_failed"
	}
	return worker.NewMetricsWithObserver(rate, observer), ""
}

func firstEndpoint(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
