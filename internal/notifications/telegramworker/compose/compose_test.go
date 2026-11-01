package compose

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	telegramworkerconfig "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/config"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/redis/go-redis/v9"
)

type refusingHTTPClient struct{ calls atomic.Int32 }

func (client *refusingHTTPClient) Do(*http.Request) (*http.Response, error) {
	client.calls.Add(1)
	return nil, errors.New("connection refused")
}

func refusingConfig(client *refusingHTTPClient, endpoint string) aws.Config {
	config := aws.Config{
		Region: "us-east-1", HTTPClient: client,
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}
	if endpoint != "" {
		config.BaseEndpoint = aws.String(endpoint)
	}
	return config
}

func telegramConfig(t *testing.T, extra map[string]string) telegramworkerconfig.RunConfig {
	t.Helper()
	values := map[string]string{
		"APP_ENV": "test", "TELEGRAM_DELIVERY_ENABLED": "true", "AWS_REGION": "us-east-1",
		"DYNAMODB_DOMAIN_TABLE": "domain", "DYNAMODB_ENDPOINT_URL": "http://dynamodb.test",
		"SQS_ENDPOINT_URL": "http://sqs.test", "SQS_TELEGRAM_JOBS_URL": "http://sqs.test/queue/telegram",
		"REDIS_ADDR": "127.0.0.1:9", "TELEGRAM_BOT_TOKEN": "123456:test-token",
	}
	for key, value := range extra {
		values[key] = value
	}
	config, err := telegramworkerconfig.Load(nil, func(key string) (string, bool) {
		value, ok := values[key]
		if ok && value == "" {
			return "", false
		}
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

type unusedEvaler struct{}

func (unusedEvaler) Eval(context.Context, string, []string, ...any) *redis.Cmd {
	panic("the limiter was used before any message arrived")
}

func TestConsumeRunsTheTelegramQueueBehindTheLimiter(t *testing.T) {
	config := telegramConfig(t, nil)
	client := &refusingHTTPClient{}
	summary, err := consume(context.Background(), config, clients{
		dynamo: refusingConfig(client, config.DynamoDBEndpoint), sqs: refusingConfig(client, config.SQSEndpoint),
		redis: unusedEvaler{}, botToken: config.BotToken,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if client.calls.Load() == 0 || summary.ExitCode != 1 || summary.ErrorCategories["queue_receive_failure"] != 1 ||
		summary.Metrics.ConfiguredRate != config.GlobalRate {
		t.Fatalf("calls=%d summary=%#v", client.calls.Load(), summary)
	}
	config.GlobalBurst = 0
	if _, err := consume(context.Background(), config, clients{redis: unusedEvaler{}, botToken: "123456:test-token"}, nil); !workerproc.IsConfiguration(err) {
		t.Fatalf("limiter error = %v, want a configuration error", err)
	}
}

func TestRunStopsBeforeConsumingWhenAConnectionFails(t *testing.T) {
	config := telegramConfig(t, nil)
	for failAt := 1; failAt <= 2; failAt++ {
		calls := 0
		_, err := Run(context.Background(), config, func(context.Context, string, string) (aws.Config, error) {
			calls++
			if calls == failAt {
				return aws.Config{}, errors.New("no region")
			}
			return aws.Config{}, nil
		}, nil)
		if err == nil || calls != failAt {
			t.Fatalf("fail at %d: err=%v calls=%d", failAt, err, calls)
		}
	}
	client := &refusingHTTPClient{}
	load := func(_ context.Context, _ string, endpoint string) (aws.Config, error) {
		return refusingConfig(client, endpoint), nil
	}
	// Nothing listens on the discard port: the limiter's Redis is unreachable.
	_, err := Run(context.Background(), config, load, nil)
	if err == nil || !strings.Contains(err.Error(), "rate limiter") || workerproc.IsConfiguration(err) {
		t.Fatalf("Redis error = %v", err)
	}
	config.RedisTLS = true
	config.RedisAddr = "redis-without-port"
	if _, err := Run(context.Background(), config, load, nil); !workerproc.IsConfiguration(err) {
		t.Fatalf("TLS server name error = %v", err)
	}
}

func TestRunReadsTheBotTokenFromSecretsManager(t *testing.T) {
	config := telegramConfig(t, map[string]string{
		"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_BOT_TOKEN_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:bot",
	})
	client := &refusingHTTPClient{}
	var endpoints []string
	_, err := Run(context.Background(), config, func(_ context.Context, _ string, endpoint string) (aws.Config, error) {
		endpoints = append(endpoints, endpoint)
		return refusingConfig(client, endpoint), nil
	}, nil)
	// The secret's own service endpoint is used, and its refusal stops the start.
	if err == nil || !strings.Contains(err.Error(), "Telegram bot secret") || len(endpoints) != 3 || endpoints[2] != "" {
		t.Fatalf("err=%v endpoints=%v", err, endpoints)
	}
	calls := 0
	_, err = Run(context.Background(), config, func(context.Context, string, string) (aws.Config, error) {
		calls++
		if calls == 3 {
			return aws.Config{}, errors.New("no secret config")
		}
		return aws.Config{}, nil
	}, nil)
	if err == nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRedisOptionsDeriveTheTLSServerName(t *testing.T) {
	config := telegramConfig(t, map[string]string{"REDIS_PASSWORD": "redis-secret"})
	options, err := RedisOptions(config)
	if err != nil || options.TLSConfig != nil || options.Password != "redis-secret" || options.MaxRetries != 1 {
		t.Fatalf("options=%#v err=%v", options, err)
	}
	config.RedisTLS = true
	config.RedisAddr = "cache.internal:6380"
	options, err = RedisOptions(config)
	if err != nil || options.TLSConfig == nil || options.TLSConfig.ServerName != "cache.internal" {
		t.Fatalf("options=%#v err=%v", options, err)
	}
	config.RedisAddr = ":6380"
	if _, err := RedisOptions(config); !workerproc.IsConfiguration(err) {
		t.Fatalf("empty host error = %v", err)
	}
}

type fakeSecretClient struct {
	output *secretsmanager.GetSecretValueOutput
	err    error
}

func (client *fakeSecretClient) GetSecretValue(
	context.Context,
	*secretsmanager.GetSecretValueInput,
	...func(*secretsmanager.Options),
) (*secretsmanager.GetSecretValueOutput, error) {
	return client.output, client.err
}

func TestLoadBotTokenUsesTheCurrentSecretValue(t *testing.T) {
	token, err := LoadBotToken(context.Background(), &fakeSecretClient{
		output: &secretsmanager.GetSecretValueOutput{SecretString: aws.String(" 123456:current \n")},
	}, "arn:secret")
	if err != nil || token != "123456:current" {
		t.Fatalf("token=%q err=%v", token, err)
	}
	token, err = LoadBotToken(context.Background(), &fakeSecretClient{
		output: &secretsmanager.GetSecretValueOutput{SecretBinary: []byte("123456:binary")},
	}, "arn:secret")
	if err != nil || token != "123456:binary" {
		t.Fatalf("token=%q err=%v", token, err)
	}
}

func TestLoadBotTokenRejectsAnInvalidSecretWithoutEchoingIt(t *testing.T) {
	for _, value := range []string{"", "private/token", "private\ntoken", "private?token"} {
		_, err := LoadBotToken(context.Background(), &fakeSecretClient{
			output: &secretsmanager.GetSecretValueOutput{SecretString: aws.String(value)},
		}, "arn:secret")
		if !workerproc.IsConfiguration(err) || (value != "" && strings.Contains(err.Error(), value)) {
			t.Fatalf("value %q: err=%v", value, err)
		}
	}
	private := errors.New("access denied")
	if _, err := LoadBotToken(context.Background(), &fakeSecretClient{err: private}, "arn:secret"); !errors.Is(err, private) ||
		workerproc.IsConfiguration(err) {
		t.Fatalf("retrieval error = %v", err)
	}
	if _, err := LoadBotToken(context.Background(), &fakeSecretClient{}, "arn:secret"); err == nil {
		t.Fatal("an empty response was accepted")
	}
	if _, err := LoadBotToken(context.Background(), nil, "arn:secret"); !workerproc.IsConfiguration(err) {
		t.Fatalf("nil client error = %v", err)
	}
	if _, err := LoadBotToken(context.Background(), &fakeSecretClient{}, " "); !workerproc.IsConfiguration(err) {
		t.Fatalf("blank ARN error = %v", err)
	}
}
