package telegramworker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/redis/go-redis/v9"
)

type fakeRedisEvaler struct {
	result any
	err    error
	keys   []string
	args   []any
}

func (client *fakeRedisEvaler) Eval(
	_ context.Context,
	_ string,
	keys []string,
	args ...any,
) *redis.Cmd {
	client.keys = append([]string(nil), keys...)
	client.args = append([]any(nil), args...)
	return redis.NewCmdResult(client.result, client.err)
}

func TestRedisLimiterAtomicallyUsesGlobalAndDestinationBuckets(t *testing.T) {
	client := &fakeRedisEvaler{result: []any{int64(1), int64(0)}}
	limiter, err := NewRedisLimiter(client, RedisLimiterConfig{
		BotToken: "123456:super-secret", GlobalRate: 25, GlobalBurst: 25,
		DestinationRate: 1, DestinationBurst: 1, UnavailableRetryAfter: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery := telegramSendRequest(t).Delivery
	if err := limiter.WaitFor(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if len(client.keys) != 2 || !strings.Contains(client.keys[0], "{telegram:") ||
		!strings.Contains(client.keys[1], "{telegram:") ||
		strings.Split(client.keys[0], "}")[0] != strings.Split(client.keys[1], "}")[0] {
		t.Fatalf("Redis keys do not share a cluster hash tag: %#v", client.keys)
	}
	for _, key := range client.keys {
		if strings.Contains(key, "super-secret") || strings.Contains(key, "123456") ||
			strings.Contains(key, "123") {
			t.Fatalf("Redis key leaks provider secret or raw chat ID: %q", key)
		}
	}
	if len(client.args) != 4 {
		t.Fatalf("script args = %#v", client.args)
	}
}

func TestRedisLimiterReturnsDurableDeferralHints(t *testing.T) {
	tests := []struct {
		name        string
		result      any
		err         error
		wantDelay   time.Duration
		unavailable bool
	}{
		{name: "rate denied", result: []any{int64(0), int64(2500)}, wantDelay: 2500 * time.Millisecond},
		{name: "Redis unavailable", err: errors.New("connection refused"), wantDelay: 11 * time.Second, unavailable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeRedisEvaler{result: test.result, err: test.err}
			limiter, err := NewRedisLimiter(client, RedisLimiterConfig{
				BotToken: "secret", GlobalRate: 20, GlobalBurst: 20,
				DestinationRate: 1, DestinationBurst: 1, UnavailableRetryAfter: 11 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = limiter.WaitFor(context.Background(), telegramSendRequest(t).Delivery)
			var rateErr *worker.RateLimitError
			if !errors.As(err, &rateErr) || rateErr.RetryAfter != test.wantDelay ||
				rateErr.Unavailable != test.unavailable {
				t.Fatalf("WaitFor error = %#v", err)
			}
		})
	}
}

func TestRedisLimiterRejectsInvalidConfigurationAndNonTelegramDelivery(t *testing.T) {
	if _, err := NewRedisLimiter(&fakeRedisEvaler{}, RedisLimiterConfig{}); err == nil {
		t.Fatal("NewRedisLimiter accepted empty configuration")
	}
	limiter, err := NewRedisLimiter(&fakeRedisEvaler{}, RedisLimiterConfig{
		BotToken: "secret", GlobalRate: 20, GlobalBurst: 20,
		DestinationRate: 1, DestinationBurst: 1, UnavailableRetryAfter: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery := telegramSendRequest(t).Delivery
	delivery.Channel = "email"
	if err := limiter.WaitFor(context.Background(), delivery); err == nil {
		t.Fatal("WaitFor accepted a non-Telegram delivery")
	}
}
