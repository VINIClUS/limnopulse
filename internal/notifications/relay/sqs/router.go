package sqs

import (
	"context"
	"fmt"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
)

type Router struct {
	Email           Publisher
	Telegram        Publisher
	TelegramEnabled bool
}

func (router Router) Publish(ctx context.Context, request relay.PublishRequest) (string, error) {
	switch request.Job.Channel {
	case notifications.ChannelEmail:
		return router.Email.Publish(ctx, request)
	case notifications.ChannelTelegram:
		if !router.TelegramEnabled {
			return "", fmt.Errorf("Telegram delivery is disabled")
		}
		return router.Telegram.Publish(ctx, request)
	default:
		return "", fmt.Errorf("unsupported notification channel %q", request.Job.Channel)
	}
}
