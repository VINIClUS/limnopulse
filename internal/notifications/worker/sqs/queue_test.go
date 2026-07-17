package sqs

import (
	"context"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestQueueLongPollsJobsWithBoundedBatchAndReceiveCount(t *testing.T) {
	client := &fakeClient{receiveOutput: &awssqs.ReceiveMessageOutput{Messages: []types.Message{{
		MessageId: aws.String("message_1"), ReceiptHandle: aws.String("receipt_1"),
		Body:       aws.String(`{"schema_version":1}`),
		Attributes: map[string]string{"ApproximateReceiveCount": "3"},
	}}}}
	queue := Queue{Client: client, QueueURL: "https://sqs/jobs", ReceiveTimeout: 25 * time.Second, MutationTimeout: 5 * time.Second}
	messages, err := queue.Receive(context.Background(), 10, 20*time.Second, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].MessageID != "message_1" || messages[0].ReceiptHandle != "receipt_1" ||
		messages[0].ReceiveCount != 3 {
		t.Fatalf("messages = %#v", messages)
	}
	input := client.receiveInput
	if aws.ToString(input.QueueUrl) != "https://sqs/jobs" || input.MaxNumberOfMessages != 10 ||
		input.WaitTimeSeconds != 20 || input.VisibilityTimeout != 60 {
		t.Fatalf("receive input = %#v", input)
	}
	if len(input.MessageSystemAttributeNames) != 1 || input.MessageSystemAttributeNames[0] != types.MessageSystemAttributeNameApproximateReceiveCount {
		t.Fatalf("system attributes = %#v", input.MessageSystemAttributeNames)
	}
	if !client.receiveDeadline || client.receiveRemaining < 24*time.Second || client.receiveRemaining > 25*time.Second {
		t.Fatalf("ReceiveMessage deadline set=%t remaining=%s", client.receiveDeadline, client.receiveRemaining)
	}
}

func TestQueueDeleteAndVisibilityUseLatestReceiptAndRequestDeadline(t *testing.T) {
	client := &fakeClient{}
	queue := Queue{Client: client, QueueURL: "https://sqs/jobs", ReceiveTimeout: 25 * time.Second, MutationTimeout: 5 * time.Second}
	message := worker.QueueMessage{MessageID: "message_1", ReceiptHandle: "latest_receipt"}
	if err := queue.ChangeVisibility(context.Background(), message, 91*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := queue.Delete(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if aws.ToString(client.visibilityInput.ReceiptHandle) != "latest_receipt" ||
		client.visibilityInput.VisibilityTimeout != 91 || !client.visibilityDeadline || client.visibilityRemaining > 5*time.Second {
		t.Fatalf("visibility input=%#v deadline=%t", client.visibilityInput, client.visibilityDeadline)
	}
	if aws.ToString(client.deleteInput.ReceiptHandle) != "latest_receipt" || !client.deleteDeadline || client.deleteRemaining > 5*time.Second {
		t.Fatalf("delete input=%#v deadline=%t", client.deleteInput, client.deleteDeadline)
	}
}

type fakeClient struct {
	receiveOutput       *awssqs.ReceiveMessageOutput
	receiveInput        *awssqs.ReceiveMessageInput
	visibilityInput     *awssqs.ChangeMessageVisibilityInput
	deleteInput         *awssqs.DeleteMessageInput
	receiveDeadline     bool
	receiveRemaining    time.Duration
	visibilityDeadline  bool
	visibilityRemaining time.Duration
	deleteDeadline      bool
	deleteRemaining     time.Duration
}

func (client *fakeClient) ReceiveMessage(ctx context.Context, input *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	client.receiveInput = input
	deadline, ok := ctx.Deadline()
	client.receiveDeadline = ok
	client.receiveRemaining = time.Until(deadline)
	return client.receiveOutput, nil
}
func (client *fakeClient) ChangeMessageVisibility(ctx context.Context, input *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	client.visibilityInput = input
	deadline, ok := ctx.Deadline()
	client.visibilityDeadline = ok
	client.visibilityRemaining = time.Until(deadline)
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}
func (client *fakeClient) DeleteMessage(ctx context.Context, input *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	client.deleteInput = input
	deadline, ok := ctx.Deadline()
	client.deleteDeadline = ok
	client.deleteRemaining = time.Until(deadline)
	return &awssqs.DeleteMessageOutput{}, nil
}
