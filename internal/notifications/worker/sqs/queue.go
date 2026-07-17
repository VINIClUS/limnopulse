package sqs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client interface {
	ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

type Queue struct {
	Client          Client
	QueueURL        string
	ReceiveTimeout  time.Duration
	MutationTimeout time.Duration
}

func (queue Queue) Receive(
	ctx context.Context,
	maxMessages int,
	wait time.Duration,
	visibility time.Duration,
) ([]worker.QueueMessage, error) {
	if queue.Client == nil || strings.TrimSpace(queue.QueueURL) == "" || maxMessages < 1 || maxMessages > 10 ||
		wait < 0 || wait > 20*time.Second || visibility < 0 || visibility > 12*time.Hour {
		return nil, fmt.Errorf("invalid SQS receive configuration")
	}
	timeout := queue.ReceiveTimeout
	if timeout <= wait {
		timeout = wait + 5*time.Second
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	output, err := queue.Client.ReceiveMessage(requestCtx, &awssqs.ReceiveMessageInput{
		QueueUrl: aws.String(queue.QueueURL), MaxNumberOfMessages: int32(maxMessages),
		WaitTimeSeconds: int32(wait / time.Second), VisibilityTimeout: int32(visibility / time.Second),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return nil, fmt.Errorf("receive notification jobs: %w", err)
	}
	if output == nil {
		return nil, fmt.Errorf("receive notification jobs returned no output")
	}
	messages := make([]worker.QueueMessage, 0, len(output.Messages))
	for _, message := range output.Messages {
		body := aws.ToString(message.Body)
		receipt := aws.ToString(message.ReceiptHandle)
		messageID := aws.ToString(message.MessageId)
		if body == "" || receipt == "" || messageID == "" {
			return nil, fmt.Errorf("received notification job is malformed")
		}
		receiveCount := 0
		if raw := message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; raw != "" {
			receiveCount, err = strconv.Atoi(raw)
			if err != nil || receiveCount < 1 {
				return nil, fmt.Errorf("notification job receive count is malformed")
			}
		}
		messages = append(messages, worker.QueueMessage{
			MessageID: messageID, ReceiptHandle: receipt,
			Body: body, ReceiveCount: receiveCount,
		})
	}
	return messages, nil
}

func (queue Queue) Delete(ctx context.Context, message worker.QueueMessage) error {
	if queue.Client == nil || strings.TrimSpace(queue.QueueURL) == "" || message.ReceiptHandle == "" {
		return fmt.Errorf("invalid SQS delete request")
	}
	requestCtx, cancel := context.WithTimeout(ctx, queue.mutationTimeout())
	defer cancel()
	_, err := queue.Client.DeleteMessage(requestCtx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(queue.QueueURL), ReceiptHandle: aws.String(message.ReceiptHandle),
	})
	if err != nil {
		return fmt.Errorf("delete notification job: %w", err)
	}
	return nil
}

func (queue Queue) ChangeVisibility(ctx context.Context, message worker.QueueMessage, visibility time.Duration) error {
	if queue.Client == nil || strings.TrimSpace(queue.QueueURL) == "" || message.ReceiptHandle == "" ||
		visibility < 0 || visibility > 12*time.Hour || visibility%time.Second != 0 {
		return fmt.Errorf("invalid SQS visibility request")
	}
	requestCtx, cancel := context.WithTimeout(ctx, queue.mutationTimeout())
	defer cancel()
	_, err := queue.Client.ChangeMessageVisibility(requestCtx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(queue.QueueURL), ReceiptHandle: aws.String(message.ReceiptHandle),
		VisibilityTimeout: int32(visibility / time.Second),
	})
	if err != nil {
		return fmt.Errorf("change notification job visibility: %w", err)
	}
	return nil
}

func (queue Queue) mutationTimeout() time.Duration {
	if queue.MutationTimeout > 0 {
		return queue.MutationTimeout
	}
	return 5 * time.Second
}

var _ worker.Queue = Queue{}
