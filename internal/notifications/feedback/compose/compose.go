// Package compose builds the SES-feedback consumer (ADR-020 ses-feedback-worker, SES
// consumer 2): SQS SES events → delivery state. It has no sender, no rate limiter and no
// email-jobs queue; worker/compose builds the email-jobs consumer as its own process.
package compose

import (
	"context"
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	feedbackconfig "github.com/VINIClUS/limnopulse/internal/notifications/feedback/config"
	feedbackdynamo "github.com/VINIClUS/limnopulse/internal/notifications/feedback/dynamo"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workersqs "github.com/VINIClUS/limnopulse/internal/notifications/worker/sqs"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Run composes the consumer and runs it until ctx is cancelled, reporting to loop. An error
// means it could not start.
func Run(
	ctx context.Context,
	config feedbackconfig.RunConfig,
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
	metrics := feedback.NewMetrics()
	summary := (worker.Runner{
		Queue: workersqs.Queue{
			Client: awssqs.NewFromConfig(sqsConfig), QueueURL: config.SQSQueueURL,
			ReceiveTimeout: config.SQSReceiveTimeout, MutationTimeout: config.SQSRequestTimeout,
		},
		Handler: feedback.Processor{
			Store: feedbackdynamo.Store{Table: config.DynamoDBTable, Client: awsdynamodb.NewFromConfig(dynamoConfig)},
			Now:   func() time.Time { return time.Now().UTC() }, InvalidVisibility: config.InvalidVisibility,
			Metrics: metrics,
		},
		Concurrency: config.Concurrency, ReceiveBatch: config.ReceiveBatch, ReceiveWait: config.ReceiveWait,
		Visibility: config.VisibilityTimeout, DrainTimeout: config.DrainTimeout, Loop: loop,
	}).Run(ctx)
	summary.FeedbackMetrics = metrics.Snapshot()
	return summary, nil
}
