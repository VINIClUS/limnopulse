// Package compose builds the notification relay (ADR-020 notification-relay) from its run
// configuration: the outbox store, the channel SQS lanes and the run's telemetry. The relay
// stays a one-shot; its leases, fencing and conditional writes live in relay/dynamo.
package compose

import (
	"context"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
	relaydynamo "github.com/VINIClUS/limnopulse/internal/notifications/relay/dynamo"
	relaysqs "github.com/VINIClUS/limnopulse/internal/notifications/relay/sqs"
	relaytelemetry "github.com/VINIClUS/limnopulse/internal/notifications/relay/telemetry"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Run performs one relay run and records its telemetry. An error means the relay could not
// be composed; a finished run reports its outcome in the summary.
func Run(ctx context.Context, config relayconfig.RunConfig, load workercmd.AWSLoader) (relay.RunSummary, error) {
	dynamoConfig, err := load(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return relay.RunSummary{}, err
	}
	sqsConfig, err := load(ctx, config.AWSRegion, config.SQSEndpoint)
	if err != nil {
		return relay.RunSummary{}, err
	}
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		return relay.RunSummary{}, err
	}
	sqsClient := awssqs.NewFromConfig(sqsConfig)
	runner := relay.Runner{
		Store: relaydynamo.Store{
			Table: config.DynamoDBTable, Client: awsdynamodb.NewFromConfig(dynamoConfig), Renderer: renderer,
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
	flushCtx, cancel := TelemetryFlushContext(ctx, config.OTLPFlushTimeout)
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

// TelemetryFlushContext gives the telemetry flush its own budget: the run's deadline has
// usually passed by then.
func TelemetryFlushContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}
