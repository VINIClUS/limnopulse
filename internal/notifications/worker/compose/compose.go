// Package compose builds the email-jobs consumer (ADR-020 email-worker, SES consumer 1): SQS
// email jobs → SES, with its rate limiter, lease guard and telemetry. It reads no SES events
// queue; feedback/compose builds the SES-feedback consumer as its own process.
package compose

import (
	"context"
	"fmt"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker/delivery"
	workerdynamo "github.com/VINIClUS/limnopulse/internal/notifications/worker/dynamo"
	workerses "github.com/VINIClUS/limnopulse/internal/notifications/worker/ses"
	workersqs "github.com/VINIClUS/limnopulse/internal/notifications/worker/sqs"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Run composes the consumer and runs it until ctx is cancelled, reporting to loop. An error
// means it could not start.
func Run(
	ctx context.Context,
	config workerconfig.RunConfig,
	load workercmd.AWSLoader,
	loop worker.Loop,
) (worker.RunSummary, error) {
	dynamoConfig, err := load(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return worker.RunSummary{}, fmt.Errorf("load DynamoDB AWS config: %w", err)
	}
	sqsConfig, err := load(ctx, config.AWSRegion, config.SQSEndpoint)
	if err != nil {
		return worker.RunSummary{}, fmt.Errorf("load SQS AWS config: %w", err)
	}
	sender, err := newSender(ctx, config, load)
	if err != nil {
		return worker.RunSummary{}, err
	}
	limiter, err := worker.NewTokenLimiter(config.MaxSendRate, config.SendBurst)
	if err != nil {
		return worker.RunSummary{}, workerproc.Configuration(err)
	}
	return (delivery.Consumer{
		OwnerPrefix: "worker_",
		Queue: workersqs.Queue{
			Client: awssqs.NewFromConfig(sqsConfig), QueueURL: config.SQSQueueURL,
			ReceiveTimeout: config.SQSReceiveTimeout, MutationTimeout: config.SQSRequestTimeout,
		},
		Store:  workerdynamo.Store{Table: config.DynamoDBTable, Client: awsdynamodb.NewFromConfig(dynamoConfig)},
		Sender: sender, Limiter: limiter, Rate: config.MaxSendRate, OTLPEndpoint: config.OTLPEndpoint,
		Concurrency: config.SendConcurrency, ReceiveBatch: config.ReceiveBatch,
		ReceiveWait: config.ReceiveWait, Visibility: config.VisibilityTimeout,
		ProcessingLease: config.ProcessingLease, ProviderTimeout: config.ProviderTimeout,
		InvalidVisibility: config.InvalidVisibility, RenewalInterval: config.RenewalInterval,
		DrainTimeout: config.DrainTimeout, OTLPFlushTimeout: config.OTLPFlushTimeout,
	}).Run(ctx, loop), nil
}

func newSender(ctx context.Context, config workerconfig.RunConfig, load workercmd.AWSLoader) (worker.EmailSender, error) {
	if config.EmailSenderMode != "aws" {
		return workerses.FakeSender{
			Mode: workerses.FakeMode(config.EmailSenderMode), MessageID: config.FakeMessageID,
		}, nil
	}
	sesConfig, err := load(ctx, config.AWSRegion, config.SESEndpoint)
	if err != nil {
		return nil, fmt.Errorf("load SES AWS config: %w", err)
	}
	sesConfig, err = PrepareSES(ctx, sesConfig)
	if err != nil {
		return nil, err
	}
	return workerses.Sender{
		Client:      workerses.NewClient(sesConfig, config.SESEndpoint),
		Credentials: sesConfig.Credentials,
		FromEmail:   config.SESFromEmail, ConfigurationSet: config.ConfigurationSet,
		Timeout: config.ProviderTimeout,
	}, nil
}

// PrepareSES classifies credential failures for the sender and retrieves the credentials once,
// so a broken credential source stops the worker before it claims any delivery.
func PrepareSES(ctx context.Context, config aws.Config) (aws.Config, error) {
	if config.Credentials == nil {
		return aws.Config{}, fmt.Errorf("AWS credentials provider is required")
	}
	config.Credentials = workerses.WrapCredentials(config.Credentials)
	if _, err := config.Credentials.Retrieve(ctx); err != nil {
		return aws.Config{}, fmt.Errorf("retrieve AWS credentials: %w", err)
	}
	return config, nil
}
