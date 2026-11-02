// Package compose builds the Telegram consumer (ADR-020 telegram-worker): SQS Telegram jobs →
// Bot API, behind the shared Redis rate limiter.
package compose

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	telegramworker "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker"
	telegramworkerconfig "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/config"
	telegramworkerdynamo "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/dynamo"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker/delivery"
	workersqs "github.com/VINIClUS/limnopulse/internal/notifications/worker/sqs"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redis/go-redis/v9"
)

const redisTimeout = 5 * time.Second

// SecretClient reads the bot token's Secrets Manager secret.
type SecretClient interface {
	GetSecretValue(
		context.Context,
		*secretsmanager.GetSecretValueInput,
		...func(*secretsmanager.Options),
	) (*secretsmanager.GetSecretValueOutput, error)
}

// Run composes the consumer and runs it until ctx is cancelled, reporting to loop. An error
// means it could not start.
func Run(
	ctx context.Context,
	config telegramworkerconfig.RunConfig,
	load workercmd.AWSLoader,
	loop worker.Loop,
) (worker.RunSummary, error) {
	dynamoConfig, err := load(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return worker.RunSummary{}, fmt.Errorf("load Telegram DynamoDB config: %w", err)
	}
	sqsConfig, err := load(ctx, config.AWSRegion, config.SQSEndpoint)
	if err != nil {
		return worker.RunSummary{}, fmt.Errorf("load Telegram SQS config: %w", err)
	}
	botToken := config.BotToken
	if config.BotTokenSecretARN != "" {
		secretConfig, err := load(ctx, config.AWSRegion, "")
		if err != nil {
			return worker.RunSummary{}, fmt.Errorf("load Telegram secret config: %w", err)
		}
		botToken, err = LoadBotToken(ctx, secretsmanager.NewFromConfig(secretConfig), config.BotTokenSecretARN)
		if err != nil {
			return worker.RunSummary{}, err
		}
	}
	redisOptions, err := RedisOptions(config)
	if err != nil {
		return worker.RunSummary{}, err
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	startupCtx, cancelStartup := context.WithTimeout(ctx, redisTimeout)
	defer cancelStartup()
	if err := redisClient.Ping(startupCtx).Err(); err != nil {
		return worker.RunSummary{}, fmt.Errorf("connect Telegram rate limiter: %w", err)
	}
	return consume(ctx, config, clients{dynamo: dynamoConfig, sqs: sqsConfig, redis: redisClient, botToken: botToken}, loop)
}

// clients are the connections Run opened for the consumer.
type clients struct {
	dynamo, sqs aws.Config
	redis       telegramworker.RedisEvaler
	botToken    string
}

func consume(
	ctx context.Context,
	config telegramworkerconfig.RunConfig,
	opened clients,
	loop worker.Loop,
) (worker.RunSummary, error) {
	botToken := opened.botToken
	limiter, err := telegramworker.NewRedisLimiter(opened.redis, telegramworker.RedisLimiterConfig{
		BotToken: botToken, GlobalRate: config.GlobalRate, GlobalBurst: config.GlobalBurst,
		DestinationRate: config.DestinationRate, DestinationBurst: config.DestinationBurst,
		UnavailableRetryAfter: config.LimiterUnavailableRetryAfter,
	})
	if err != nil {
		return worker.RunSummary{}, workerproc.Configuration(err)
	}
	return (delivery.Consumer{
		OwnerPrefix: "telegram_worker_",
		Queue: workersqs.Queue{
			Client: awssqs.NewFromConfig(opened.sqs), QueueURL: config.SQSQueueURL,
			ReceiveTimeout: config.SQSReceiveTimeout, MutationTimeout: config.SQSRequestTimeout,
		},
		Store: telegramworkerdynamo.New(config.DynamoDBTable, awsdynamodb.NewFromConfig(opened.dynamo)),
		Sender: telegramworker.Sender{
			Client: &http.Client{}, BotToken: botToken, Timeout: config.ProviderTimeout,
			BaseURL: config.BotAPIBaseURL,
		},
		Limiter: limiter, Rate: config.GlobalRate, OTLPEndpoint: config.OTLPEndpoint,
		Concurrency: config.SendConcurrency, ReceiveBatch: config.ReceiveBatch,
		ReceiveWait: config.ReceiveWait, Visibility: config.VisibilityTimeout,
		ProcessingLease: config.ProcessingLease, ProviderTimeout: config.ProviderTimeout,
		InvalidVisibility: config.InvalidVisibility, RenewalInterval: config.RenewalInterval,
		DrainTimeout: config.DrainTimeout, OTLPFlushTimeout: config.OTLPFlushTimeout,
	}).Run(ctx, loop), nil
}

// RedisOptions connects to the limiter's Redis. With TLS, the server name comes from the
// address, so an address without a host is a configuration error.
func RedisOptions(config telegramworkerconfig.RunConfig) (*redis.Options, error) {
	options := &redis.Options{
		Addr: config.RedisAddr, Password: config.RedisPassword, DB: config.RedisDB,
		DialTimeout: redisTimeout, ReadTimeout: redisTimeout, WriteTimeout: redisTimeout,
		MaxRetries: 1,
	}
	if config.RedisTLS {
		host, _, err := net.SplitHostPort(config.RedisAddr)
		if err != nil || host == "" {
			return nil, workerproc.Configuration(errors.New("derive Redis TLS server name"))
		}
		options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	}
	return options, nil
}

// LoadBotToken reads the current bot token. A secret that cannot be a token is a
// configuration error; the error never carries the secret's value.
func LoadBotToken(ctx context.Context, client SecretClient, secretARN string) (string, error) {
	if client == nil || strings.TrimSpace(secretARN) == "" {
		return "", workerproc.Configuration(errors.New("Telegram bot secret configuration is invalid"))
	}
	output, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(secretARN)})
	if err != nil {
		return "", fmt.Errorf("retrieve Telegram bot secret: %w", err)
	}
	if output == nil {
		return "", errors.New("Telegram bot secret response is empty")
	}
	value := strings.TrimSpace(aws.ToString(output.SecretString))
	if value == "" && len(output.SecretBinary) > 0 {
		value = strings.TrimSpace(string(output.SecretBinary))
	}
	if value == "" || strings.ContainsAny(value, "\r\n\x00/?#") {
		return "", workerproc.Configuration(errors.New("Telegram bot secret value is invalid"))
	}
	return value, nil
}
