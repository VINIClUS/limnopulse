package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	feedbackdynamo "github.com/VINIClUS/limnopulse/internal/notifications/feedback/dynamo"
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
	awsConfigs, err := loadWorkerAWSConfigs(ctx, config, loadAWSConfig)
	if err != nil {
		return worker.RunSummary{}, err
	}
	if config.EmailSenderMode == "aws" {
		awsConfigs.SES, err = prepareSESAWSConfig(ctx, awsConfigs.SES)
		if err != nil {
			return worker.RunSummary{}, err
		}
	}
	dynamoClient := awssdk.NewFromConfig(awsConfigs.DynamoDB, func(options *awssdk.Options) {
		if config.DynamoDBEndpoint != "" {
			options.BaseEndpoint = aws.String(config.DynamoDBEndpoint)
		}
	})
	sqsClient := awssqs.NewFromConfig(awsConfigs.SQS, func(options *awssqs.Options) {
		if config.SQSEndpoint != "" {
			options.BaseEndpoint = aws.String(config.SQSEndpoint)
		}
	})
	jobsQueue := workersqs.Queue{
		Client: sqsClient, QueueURL: config.SQSQueueURL,
		ReceiveTimeout: config.SQSReceiveTimeout, MutationTimeout: config.SQSRequestTimeout,
	}
	feedbackQueue := workersqs.Queue{
		Client: sqsClient, QueueURL: config.SQSFeedbackURL,
		ReceiveTimeout: config.SQSReceiveTimeout, MutationTimeout: config.SQSRequestTimeout,
	}
	var sender worker.EmailSender
	if config.EmailSenderMode == "aws" {
		sender = workerses.Sender{
			Client:      workerses.NewClient(awsConfigs.SES, config.SESEndpoint),
			Credentials: awsConfigs.SES.Credentials,
			FromEmail:   config.SESFromEmail, ConfigurationSet: config.ConfigurationSet,
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
	feedbackStore := feedbackdynamo.Store{Table: config.DynamoDBTable, Client: dynamoClient}
	feedbackMetrics := feedback.NewMetrics()
	owner := "worker_" + uuid.NewString()
	processor := worker.Processor{
		Store: store, Sender: sender, Limiter: limiter, Owner: owner,
		ProcessingLease: config.ProcessingLease, ProviderTimeout: config.ProviderTimeout,
		InvalidVisibility: config.InvalidVisibility, Now: func() time.Time { return time.Now().UTC() },
		NewAttemptID:   func() string { return "att_" + uuid.NewString() },
		JitterFraction: rand.Float64, Metrics: metrics,
	}
	processor.Guard = worker.RenewalGuard{
		Store: store, Queue: jobsQueue, Interval: config.RenewalInterval,
		LeaseTTL: config.ProcessingLease, Visibility: config.VisibilityTimeout,
		Now: func() time.Time { return time.Now().UTC() },
	}
	jobsRunner := worker.Runner{
		Queue: jobsQueue, Handler: processor, Concurrency: config.SendConcurrency,
		ReceiveBatch: config.ReceiveBatch, ReceiveWait: config.ReceiveWait,
		Visibility: config.VisibilityTimeout, DrainTimeout: config.DrainTimeout,
	}
	feedbackProcessor := feedback.Processor{
		Store: feedbackStore, Now: func() time.Time { return time.Now().UTC() },
		InvalidVisibility: config.InvalidVisibility, Metrics: feedbackMetrics,
	}
	feedbackRunner := worker.Runner{
		Queue: feedbackQueue, Handler: feedbackProcessor, Concurrency: config.FeedbackConcurrency,
		ReceiveBatch: config.ReceiveBatch, ReceiveWait: config.ReceiveWait,
		Visibility: config.VisibilityTimeout, DrainTimeout: config.DrainTimeout,
	}
	summary := (worker.Supervisor{Jobs: jobsRunner, Feedback: feedbackRunner}).Run(ctx)
	summary.Metrics = metrics.Snapshot()
	summary.FeedbackMetrics = feedbackMetrics.Snapshot()
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

func prepareSESAWSConfig(ctx context.Context, config aws.Config) (aws.Config, error) {
	config.Credentials = workerses.WrapCredentials(config.Credentials)
	if err := preflightAWSCredentials(ctx, config.Credentials); err != nil {
		return aws.Config{}, err
	}
	return config, nil
}

type workerAWSConfigLoader func(context.Context, string, string) (aws.Config, error)

type workerAWSConfigs struct {
	DynamoDB aws.Config
	SQS      aws.Config
	SES      aws.Config
}

func loadWorkerAWSConfigs(
	ctx context.Context,
	config workerconfig.RunConfig,
	load workerAWSConfigLoader,
) (workerAWSConfigs, error) {
	if load == nil {
		return workerAWSConfigs{}, fmt.Errorf("AWS config loader is required")
	}
	dynamoConfig, err := load(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return workerAWSConfigs{}, fmt.Errorf("load DynamoDB AWS config: %w", err)
	}
	sqsConfig, err := load(ctx, config.AWSRegion, config.SQSEndpoint)
	if err != nil {
		return workerAWSConfigs{}, fmt.Errorf("load SQS AWS config: %w", err)
	}
	configs := workerAWSConfigs{DynamoDB: dynamoConfig, SQS: sqsConfig}
	if config.EmailSenderMode == "aws" {
		configs.SES, err = load(ctx, config.AWSRegion, config.SESEndpoint)
		if err != nil {
			return workerAWSConfigs{}, fmt.Errorf("load SES AWS config: %w", err)
		}
	}
	return configs, nil
}

func preflightAWSCredentials(ctx context.Context, provider aws.CredentialsProvider) error {
	if provider == nil {
		return fmt.Errorf("AWS credentials provider is required")
	}
	if _, err := provider.Retrieve(ctx); err != nil {
		return fmt.Errorf("retrieve AWS credentials: %w", err)
	}
	return nil
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
