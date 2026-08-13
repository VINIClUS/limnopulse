package sqs

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

type routingClient struct{ queues []string }

func (client *routingClient) SendMessage(
	_ context.Context,
	input *awssqs.SendMessageInput,
	_ ...func(*awssqs.Options),
) (*awssqs.SendMessageOutput, error) {
	client.queues = append(client.queues, aws.ToString(input.QueueUrl))
	return &awssqs.SendMessageOutput{MessageId: aws.String("message-1")}, nil
}

func TestRouterNeverCrossPublishesEmailAndTelegramJobs(t *testing.T) {
	client := &routingClient{}
	router := Router{
		Email:           Publisher{Client: client, QueueURL: "email-queue", RequestTimeout: time.Second},
		Telegram:        Publisher{Client: client, QueueURL: "telegram-queue", RequestTimeout: time.Second},
		TelegramEnabled: true,
	}
	for _, channel := range []notifications.Channel{
		notifications.ChannelEmail,
		notifications.ChannelTelegram,
	} {
		job, err := notifications.NewDeliveryJob(
			"tnt_1", "outbox_1", "delivery_1", "alert_1",
			notifications.NotificationKindOpening, channel,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := router.Publish(context.Background(), relay.PublishRequest{Job: job}); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := client.queues, []string{"email-queue", "telegram-queue"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("queues = %v, want %v", got, want)
	}
}

func TestRouterDoesNotPublishTelegramWhenDeliveryFlagIsDisabled(t *testing.T) {
	client := &routingClient{}
	router := Router{
		Email: Publisher{Client: client, QueueURL: "email-queue", RequestTimeout: time.Second},
		Telegram: Publisher{
			Client: client, QueueURL: "telegram-queue", RequestTimeout: time.Second,
		},
		TelegramEnabled: false,
	}
	job, err := notifications.NewDeliveryJob(
		"tnt_1", "outbox_1", "delivery_1", "alert_1",
		notifications.NotificationKindOpening, notifications.ChannelTelegram,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = router.Publish(context.Background(), relay.PublishRequest{Job: job})
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("Publish() error = %v, want disabled", err)
	}
	if len(client.queues) != 0 {
		t.Fatalf("SendMessage queues = %v, want none", client.queues)
	}
}
