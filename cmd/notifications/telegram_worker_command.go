package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"time"

	telegramworker "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker"
	telegramworkerconfig "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/config"
	telegramworkerdynamo "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/dynamo"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workersqs "github.com/VINIClUS/limnopulse/internal/notifications/worker/sqs"
	workertelemetry "github.com/VINIClUS/limnopulse/internal/notifications/worker/telemetry"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type telegramSecretClient interface {
	GetSecretValue(
		context.Context,
		*secretsmanager.GetSecretValueInput,
		...func(*secretsmanager.Options),
	) (*secretsmanager.GetSecretValueOutput, error)
}

func runTelegramWorkerCommand(ctx context.Context, args []string, deps dependencies) int {
	config, err := telegramworkerconfig.Load(args, deps.LookupEnv)
	if err != nil || deps.RunTelegramWorker == nil {
		return writeFatal(deps.Output, "configuration")
	}
	summary, err := deps.RunTelegramWorker(ctx, config)
	if err != nil {
		return writeFatal(deps.Output, "worker_configuration")
	}
	writeResult(deps.Output, summary)
	return summary.ExitCode
}

func executeTelegramWorker(
	ctx context.Context,
	config telegramworkerconfig.RunConfig,
) (worker.RunSummary, error) {
	dynamoConfig, err := loadAWSConfig(ctx, config.AWSRegion, config.DynamoDBEndpoint)
	if err != nil {
		return worker.RunSummary{}, fmt.Errorf("load Telegram DynamoDB config: %w", err)
	}
	sqsConfig, err := loadAWSConfig(ctx, config.AWSRegion, config.SQSEndpoint)
	if err != nil {
		return worker.RunSummary{}, fmt.Errorf("load Telegram SQS config: %w", err)
	}
	botToken := config.BotToken
	if config.BotTokenSecretARN != "" {
		secretConfig, secretErr := loadAWSConfig(ctx, config.AWSRegion, "")
		if secretErr != nil {
			return worker.RunSummary{}, fmt.Errorf("load Telegram secret config: %w", secretErr)
		}
		botToken, err = loadTelegramBotToken(
			ctx,
			secretsmanager.NewFromConfig(secretConfig),
			config.BotTokenSecretARN,
		)
		if err != nil {
			return worker.RunSummary{}, err
		}
	}

	dynamoClient := awssdk.NewFromConfig(dynamoConfig, func(options *awssdk.Options) {
		if config.DynamoDBEndpoint != "" {
			options.BaseEndpoint = aws.String(config.DynamoDBEndpoint)
		}
	})
	sqsClient := awssqs.NewFromConfig(sqsConfig, func(options *awssqs.Options) {
		if config.SQSEndpoint != "" {
			options.BaseEndpoint = aws.String(config.SQSEndpoint)
		}
	})
	redisOptions := &redis.Options{
		Addr: config.RedisAddr, Password: config.RedisPassword, DB: config.RedisDB,
		DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		MaxRetries: 1,
	}
	if config.RedisTLS {
		host, _, splitErr := net.SplitHostPort(config.RedisAddr)
		if splitErr != nil || host == "" {
			return worker.RunSummary{}, fmt.Errorf("derive Redis TLS server name")
		}
		redisOptions.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	startupCtx, cancelStartup := context.WithTimeout(ctx, 5*time.Second)
	defer cancelStartup()
	if err := redisClient.Ping(startupCtx).Err(); err != nil {
		return worker.RunSummary{}, fmt.Errorf("connect Telegram rate limiter: %w", err)
	}
	limiter, err := telegramworker.NewRedisLimiter(redisClient, telegramworker.RedisLimiterConfig{
		BotToken: botToken, GlobalRate: config.GlobalRate, GlobalBurst: config.GlobalBurst,
		DestinationRate: config.DestinationRate, DestinationBurst: config.DestinationBurst,
		UnavailableRetryAfter: config.LimiterUnavailableRetryAfter,
	})
	if err != nil {
		return worker.RunSummary{}, err
	}
	queue := workersqs.Queue{
		Client: sqsClient, QueueURL: config.SQSQueueURL,
		ReceiveTimeout: config.SQSReceiveTimeout, MutationTimeout: config.SQSRequestTimeout,
	}
	store := telegramworkerdynamo.New(config.DynamoDBTable, dynamoClient)
	sender := telegramworker.Sender{
		Client: &http.Client{}, BotToken: botToken, Timeout: config.ProviderTimeout,
		BaseURL: config.BotAPIBaseURL,
	}
	recorder, telemetryErr := workertelemetry.New(ctx, config.OTLPEndpoint)
	metrics, telemetryState := newWorkerMetrics(config.GlobalRate, recorder, telemetryErr)
	processor := worker.Processor{
		Store: store, Sender: sender, Limiter: limiter, Owner: "telegram_worker_" + uuid.NewString(),
		ProcessingLease: config.ProcessingLease, ProviderTimeout: config.ProviderTimeout,
		InvalidVisibility: config.InvalidVisibility, Now: func() time.Time { return time.Now().UTC() },
		NewAttemptID:   func() string { return "att_" + uuid.NewString() },
		JitterFraction: rand.Float64, Metrics: metrics,
	}
	processor.Guard = worker.RenewalGuard{
		Store: store, Queue: queue, Interval: config.RenewalInterval,
		LeaseTTL: config.ProcessingLease, Visibility: config.VisibilityTimeout,
		Now: func() time.Time { return time.Now().UTC() },
	}
	summary := (worker.Runner{
		Queue: queue, Handler: processor, Concurrency: config.SendConcurrency,
		ReceiveBatch: config.ReceiveBatch, ReceiveWait: config.ReceiveWait,
		Visibility: config.VisibilityTimeout, DrainTimeout: config.DrainTimeout,
	}).Run(ctx)
	summary.Metrics = metrics.Snapshot()
	if telemetryState != "" {
		summary.TelemetryExportError = telemetryState
	} else {
		flushCtx, cancelFlush := context.WithTimeout(context.WithoutCancel(ctx), config.OTLPFlushTimeout)
		defer cancelFlush()
		if err := recorder.Shutdown(flushCtx); err != nil {
			summary.TelemetryExportError = "export_failed"
		}
	}
	return summary, nil
}

func loadTelegramBotToken(
	ctx context.Context,
	client telegramSecretClient,
	secretARN string,
) (string, error) {
	if client == nil || strings.TrimSpace(secretARN) == "" {
		return "", fmt.Errorf("Telegram bot secret configuration is invalid")
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
		return "", errors.New("Telegram bot secret value is invalid")
	}
	return value, nil
}
