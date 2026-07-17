package sqs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client interface {
	SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

type Publisher struct {
	Client         Client
	QueueURL       string
	RequestTimeout time.Duration
}

func (publisher Publisher) Publish(
	ctx context.Context,
	request relay.PublishRequest,
) (string, error) {
	if publisher.Client == nil || strings.TrimSpace(publisher.QueueURL) == "" ||
		publisher.RequestTimeout <= 0 {
		return "", fmt.Errorf("SQS publisher is not configured")
	}
	body, err := request.Job.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("marshal notification job: %w", err)
	}
	input := &awssqs.SendMessageInput{
		QueueUrl: aws.String(publisher.QueueURL), MessageBody: aws.String(string(body)),
	}
	if request.Traceparent != "" {
		input.MessageAttributes = map[string]types.MessageAttributeValue{
			notifications.TraceparentMessageAttribute: {
				DataType: aws.String("String"), StringValue: aws.String(request.Traceparent),
			},
		}
	}
	requestCtx, cancel := context.WithTimeout(ctx, publisher.RequestTimeout)
	defer cancel()
	output, err := publisher.Client.SendMessage(requestCtx, input)
	if err != nil {
		return "", fmt.Errorf("SQS SendMessage was not confirmed: %w", err)
	}
	if output == nil || output.MessageId == nil || strings.TrimSpace(aws.ToString(output.MessageId)) == "" {
		return "", fmt.Errorf("SQS SendMessage was not confirmed")
	}
	return aws.ToString(output.MessageId), nil
}
