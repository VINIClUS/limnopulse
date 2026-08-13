package telegramworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/redis/go-redis/v9"
)

// RedisEvaler is the narrow go-redis seam needed by the limiter. Both
// redis.Client and redis.ClusterClient implement it.
type RedisEvaler interface {
	Eval(context.Context, string, []string, ...any) *redis.Cmd
}

type RedisLimiterConfig struct {
	BotToken              string
	GlobalRate            float64
	GlobalBurst           int
	DestinationRate       float64
	DestinationBurst      int
	UnavailableRetryAfter time.Duration
}

type RedisLimiter struct {
	client                RedisEvaler
	keyPrefix             string
	globalRate            float64
	globalBurst           int
	destinationRate       float64
	destinationBurst      int
	unavailableRetryAfter time.Duration
}

const telegramRateLimitScript = `
local now_parts = redis.call('TIME')
local now_ms = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000)

local function bucket(key, rate, burst)
  local values = redis.call('HMGET', key, 'tokens', 'updated_at_ms')
  local tokens = tonumber(values[1]) or burst
  local updated_at_ms = tonumber(values[2]) or now_ms
  if now_ms > updated_at_ms then
    tokens = math.min(burst, tokens + ((now_ms - updated_at_ms) * rate / 1000))
  end
  local wait_ms = 0
  if tokens < 1 then
    wait_ms = math.ceil((1 - tokens) * 1000 / rate)
  end
  return tokens, wait_ms
end

local global_rate = tonumber(ARGV[1])
local global_burst = tonumber(ARGV[2])
local destination_rate = tonumber(ARGV[3])
local destination_burst = tonumber(ARGV[4])
local global_tokens, global_wait = bucket(KEYS[1], global_rate, global_burst)
local destination_tokens, destination_wait = bucket(KEYS[2], destination_rate, destination_burst)
local allowed = global_wait == 0 and destination_wait == 0
if allowed then
  global_tokens = global_tokens - 1
  destination_tokens = destination_tokens - 1
end

local function persist(key, tokens, rate, burst)
  redis.call('HSET', key, 'tokens', tokens, 'updated_at_ms', now_ms)
  local ttl_ms = math.max(60000, math.ceil((burst / rate) * 2000))
  redis.call('PEXPIRE', key, ttl_ms)
end
persist(KEYS[1], global_tokens, global_rate, global_burst)
persist(KEYS[2], destination_tokens, destination_rate, destination_burst)

if allowed then
  return {1, 0}
end
return {0, math.max(global_wait, destination_wait)}
`

func NewRedisLimiter(client RedisEvaler, config RedisLimiterConfig) (*RedisLimiter, error) {
	if client == nil || config.BotToken == "" || config.GlobalRate <= 0 || config.GlobalBurst < 1 ||
		config.DestinationRate <= 0 || config.DestinationBurst < 1 || config.UnavailableRetryAfter <= 0 {
		return nil, fmt.Errorf("invalid Telegram Redis limiter configuration")
	}
	digest := sha256.Sum256([]byte(config.BotToken))
	return &RedisLimiter{
		client: client, keyPrefix: "{telegram:" + hex.EncodeToString(digest[:]) + "}",
		globalRate: config.GlobalRate, globalBurst: config.GlobalBurst,
		destinationRate: config.DestinationRate, destinationBurst: config.DestinationBurst,
		unavailableRetryAfter: config.UnavailableRetryAfter,
	}, nil
}

func (limiter *RedisLimiter) Wait(context.Context) error {
	return errors.New("Telegram rate limiting requires a delivery destination")
}

func (limiter *RedisLimiter) WaitFor(
	ctx context.Context,
	delivery notifications.DeliverySnapshot,
) error {
	if delivery.Channel != notifications.ChannelTelegram || delivery.DestinationID == "" ||
		delivery.TelegramChatID <= 0 {
		return fmt.Errorf("Telegram rate limiter received an invalid delivery")
	}
	result, err := limiter.client.Eval(
		ctx,
		telegramRateLimitScript,
		[]string{
			limiter.keyPrefix + ":global",
			limiter.keyPrefix + ":destination:" + delivery.DestinationID,
		},
		limiter.globalRate,
		limiter.globalBurst,
		limiter.destinationRate,
		limiter.destinationBurst,
	).Result()
	if err != nil {
		return &worker.RateLimitError{RetryAfter: limiter.unavailableRetryAfter, Unavailable: true}
	}
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return &worker.RateLimitError{RetryAfter: limiter.unavailableRetryAfter, Unavailable: true}
	}
	allowed, okAllowed := redisInteger(values[0])
	retryMilliseconds, okRetry := redisInteger(values[1])
	if !okAllowed || !okRetry || (allowed != 0 && allowed != 1) || retryMilliseconds < 0 {
		return &worker.RateLimitError{RetryAfter: limiter.unavailableRetryAfter, Unavailable: true}
	}
	if allowed == 1 {
		return nil
	}
	if retryMilliseconds < 1 {
		retryMilliseconds = 1
	}
	return &worker.RateLimitError{RetryAfter: time.Duration(retryMilliseconds) * time.Millisecond}
}

func redisInteger(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	case []byte:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
