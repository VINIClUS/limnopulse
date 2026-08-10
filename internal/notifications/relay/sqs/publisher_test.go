package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

type fakeClient struct {
	input       *awssqs.SendMessageInput
	output      *awssqs.SendMessageOutput
	err         error
	deadlineSet bool
	timeLeft    time.Duration
}

func (client *fakeClient) SendMessage(
	ctx context.Context,
	input *awssqs.SendMessageInput,
	_ ...func(*awssqs.Options),
) (*awssqs.SendMessageOutput, error) {
	client.input = input
	deadline, ok := ctx.Deadline()
	client.deadlineSet = ok
	client.timeLeft = time.Until(deadline)
	return client.output, client.err
}

func TestPublishSendsStrictIdentifiersOnlyEnvelopeWithBoundedContext(t *testing.T) {
	client := &fakeClient{output: &awssqs.SendMessageOutput{MessageId: aws.String("message_1")}}
	publisher := Publisher{
		Client: client, QueueURL: "http://sqs:9324/queue/jobs", RequestTimeout: 5 * time.Second,
	}
	job, err := notifications.NewDeliveryJob(
		"tnt_1", "outbox_1", "delivery_1", "event_1",
		notifications.NotificationKindOpening, notifications.ChannelEmail,
	)
	if err != nil {
		t.Fatal(err)
	}

	messageID, err := publisher.Publish(context.Background(), relay.PublishRequest{
		Job: job, Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	})
	if err != nil {
		t.Fatal(err)
	}
	if messageID != "message_1" || client.input == nil ||
		aws.ToString(client.input.QueueUrl) != "http://sqs:9324/queue/jobs" {
		t.Fatalf("message ID/input = %q, %#v", messageID, client.input)
	}
	var decoded notifications.JobEnvelope
	if err := decoded.UnmarshalJSON([]byte(aws.ToString(client.input.MessageBody))); err != nil {
		t.Fatalf("strict job body: %v; body = %s", err, aws.ToString(client.input.MessageBody))
	}
	if decoded != job {
		t.Fatalf("job = %#v, want %#v", decoded, job)
	}
	body := aws.ToString(client.input.MessageBody)
	var object map[string]any
	if err := json.Unmarshal([]byte(body), &object); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"email", "email_address", "subject", "body", "content", "html", "text"} {
		if _, exists := object[forbidden]; exists {
			t.Fatalf("job body contains key %q: %s", forbidden, body)
		}
	}
	attribute, ok := client.input.MessageAttributes[notifications.TraceparentMessageAttribute]
	if !ok || aws.ToString(attribute.DataType) != "String" ||
		aws.ToString(attribute.StringValue) != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("traceparent attribute = %#v", client.input.MessageAttributes)
	}
	if !client.deadlineSet || client.timeLeft <= 0 || client.timeLeft > 5*time.Second {
		t.Fatalf("SQS deadline set = %t, remaining = %s", client.deadlineSet, client.timeLeft)
	}
}

func TestPublishTreatsEveryUnconfirmedSendAsAmbiguous(t *testing.T) {
	tests := []struct {
		name   string
		output *awssqs.SendMessageOutput
		err    error
	}{
		{name: "transport error", err: errors.New("connection reset")},
		{name: "nil output"},
		{name: "missing message ID", output: &awssqs.SendMessageOutput{}},
		{name: "blank message ID", output: &awssqs.SendMessageOutput{MessageId: aws.String("  ")}},
	}
	job, err := notifications.NewDeliveryJob(
		"tnt_1", "outbox_1", "delivery_1", "event_1",
		notifications.NotificationKindOpening, notifications.ChannelEmail,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			publisher := Publisher{
				Client:   &fakeClient{output: test.output, err: test.err},
				QueueURL: "queue", RequestTimeout: 5 * time.Second,
			}
			if messageID, err := publisher.Publish(
				context.Background(), relay.PublishRequest{Job: job},
			); err == nil || messageID != "" {
				t.Fatalf("Publish() = %q, %v", messageID, err)
			}
		})
	}
}
