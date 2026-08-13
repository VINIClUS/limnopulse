package config

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultSendConcurrency       = 4
	DefaultGlobalRate            = 25.0
	DefaultGlobalBurst           = 25
	DefaultDestinationRate       = 1.0
	DefaultDestinationBurst      = 1
	ReceiveWait                  = 20 * time.Second
	ReceiveBatch                 = 10
	VisibilityTimeout            = 60 * time.Second
	ProcessingLease              = 60 * time.Second
	ProviderTimeout              = 10 * time.Second
	DrainTimeout                 = 30 * time.Second
	RenewalInterval              = 20 * time.Second
	SQSReceiveTimeout            = 25 * time.Second
	SQSRequestTimeout            = 5 * time.Second
	InvalidVisibility            = 60 * time.Second
	OTLPFlushTimeout             = 2 * time.Second
	LimiterUnavailableRetryAfter = 30 * time.Second
	DefaultBotAPIBaseURL         = "https://api.telegram.org"
)

type LookupEnv func(string) (string, bool)

type RunConfig struct {
	AppEnv                       string
	Enabled                      bool
	SendConcurrency              int
	GlobalRate                   float64
	GlobalBurst                  int
	DestinationRate              float64
	DestinationBurst             int
	ReceiveWait                  time.Duration
	ReceiveBatch                 int
	VisibilityTimeout            time.Duration
	ProcessingLease              time.Duration
	ProviderTimeout              time.Duration
	DrainTimeout                 time.Duration
	RenewalInterval              time.Duration
	SQSRequestTimeout            time.Duration
	SQSReceiveTimeout            time.Duration
	InvalidVisibility            time.Duration
	OTLPFlushTimeout             time.Duration
	LimiterUnavailableRetryAfter time.Duration
	AWSRegion                    string
	DynamoDBTable                string
	DynamoDBEndpoint             string
	SQSQueueURL                  string
	SQSEndpoint                  string
	RedisAddr                    string
	RedisPassword                string
	RedisDB                      int
	RedisTLS                     bool
	BotToken                     string
	BotTokenSecretARN            string
	BotAPIBaseURL                string
	OTLPEndpoint                 string
}

func Load(args []string, lookup LookupEnv) (RunConfig, error) {
	config := RunConfig{
		AppEnv: "local", SendConcurrency: DefaultSendConcurrency,
		GlobalRate: DefaultGlobalRate, GlobalBurst: DefaultGlobalBurst,
		DestinationRate: DefaultDestinationRate, DestinationBurst: DefaultDestinationBurst,
		ReceiveWait: ReceiveWait, ReceiveBatch: ReceiveBatch,
		VisibilityTimeout: VisibilityTimeout, ProcessingLease: ProcessingLease,
		ProviderTimeout: ProviderTimeout, DrainTimeout: DrainTimeout,
		RenewalInterval: RenewalInterval, SQSRequestTimeout: SQSRequestTimeout,
		SQSReceiveTimeout: SQSReceiveTimeout, InvalidVisibility: InvalidVisibility,
		OTLPFlushTimeout: OTLPFlushTimeout, LimiterUnavailableRetryAfter: LimiterUnavailableRetryAfter,
		BotAPIBaseURL: DefaultBotAPIBaseURL,
	}
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	for key, target := range map[string]*string{
		"APP_ENV": &config.AppEnv, "AWS_REGION": &config.AWSRegion,
		"DYNAMODB_DOMAIN_TABLE": &config.DynamoDBTable, "DYNAMODB_ENDPOINT_URL": &config.DynamoDBEndpoint,
		"SQS_TELEGRAM_JOBS_URL": &config.SQSQueueURL, "SQS_ENDPOINT_URL": &config.SQSEndpoint,
		"REDIS_ADDR": &config.RedisAddr, "REDIS_PASSWORD": &config.RedisPassword,
		"TELEGRAM_BOT_TOKEN": &config.BotToken, "TELEGRAM_BOT_TOKEN_SECRET_ARN": &config.BotTokenSecretARN,
		"TELEGRAM_BOT_API_BASE_URL":   &config.BotAPIBaseURL,
		"OTEL_EXPORTER_OTLP_ENDPOINT": &config.OTLPEndpoint,
	} {
		if value, ok := lookup(key); ok {
			*target = strings.TrimSpace(value)
		}
	}
	explicitEnabled := false
	if value, ok := lookup("TELEGRAM_DELIVERY_ENABLED"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return RunConfig{}, fmt.Errorf("TELEGRAM_DELIVERY_ENABLED must be a boolean")
		}
		config.Enabled = parsed
		explicitEnabled = true
	}
	for key, target := range map[string]*float64{
		"TELEGRAM_GLOBAL_RATE":      &config.GlobalRate,
		"TELEGRAM_DESTINATION_RATE": &config.DestinationRate,
	} {
		if value, ok := lookup(key); ok && strings.TrimSpace(value) != "" {
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil {
				return RunConfig{}, fmt.Errorf("%s must be a positive number", key)
			}
			*target = parsed
		}
	}
	for key, target := range map[string]*int{
		"TELEGRAM_SEND_CONCURRENCY":  &config.SendConcurrency,
		"TELEGRAM_GLOBAL_BURST":      &config.GlobalBurst,
		"TELEGRAM_DESTINATION_BURST": &config.DestinationBurst,
		"REDIS_DB":                   &config.RedisDB,
	} {
		if value, ok := lookup(key); ok && strings.TrimSpace(value) != "" {
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return RunConfig{}, fmt.Errorf("%s must be an integer", key)
			}
			*target = parsed
		}
	}
	if value, ok := lookup("REDIS_TLS_ENABLED"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return RunConfig{}, fmt.Errorf("REDIS_TLS_ENABLED must be a boolean")
		}
		config.RedisTLS = parsed
	}

	fs := flag.NewFlagSet("notifications telegram-worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.IntVar(&config.SendConcurrency, "send-concurrency", config.SendConcurrency, "concurrent Telegram sends")
	fs.DurationVar(&config.ReceiveWait, "receive-wait", config.ReceiveWait, "SQS long-poll wait")
	fs.DurationVar(&config.InvalidVisibility, "invalid-visibility", config.InvalidVisibility, "visibility for invalid messages")
	if err := fs.Parse(args); err != nil {
		return RunConfig{}, err
	}
	if len(fs.Args()) != 0 {
		return RunConfig{}, fmt.Errorf("Telegram worker does not accept positional arguments")
	}

	hosted := config.AppEnv == "staging" || config.AppEnv == "prod"
	if !hosted && config.AppEnv != "local" && config.AppEnv != "test" {
		return RunConfig{}, fmt.Errorf("APP_ENV must be local, test, staging or prod")
	}
	if hosted && !explicitEnabled {
		return RunConfig{}, fmt.Errorf("TELEGRAM_DELIVERY_ENABLED must be explicit in staging and prod")
	}
	if !config.Enabled {
		return RunConfig{}, fmt.Errorf("TELEGRAM_DELIVERY_ENABLED must be true to run the Telegram worker")
	}
	for _, required := range []struct{ name, value string }{
		{"AWS_REGION", config.AWSRegion}, {"DYNAMODB_DOMAIN_TABLE", config.DynamoDBTable},
		{"SQS_TELEGRAM_JOBS_URL", config.SQSQueueURL}, {"REDIS_ADDR", config.RedisAddr},
	} {
		if required.value == "" {
			return RunConfig{}, fmt.Errorf("%s is required", required.name)
		}
	}
	if (config.BotToken == "") == (config.BotTokenSecretARN == "") {
		return RunConfig{}, fmt.Errorf("exactly one of TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN_SECRET_ARN is required")
	}
	if hosted {
		if config.BotToken != "" {
			return RunConfig{}, fmt.Errorf("TELEGRAM_BOT_TOKEN is allowed only in local or test")
		}
		if config.BotAPIBaseURL != DefaultBotAPIBaseURL {
			return RunConfig{}, fmt.Errorf("TELEGRAM_BOT_API_BASE_URL override is allowed only in local or test")
		}
		if config.ReceiveWait == 0 || config.InvalidVisibility != InvalidVisibility {
			return RunConfig{}, fmt.Errorf("short queue timings are allowed only in local or test")
		}
	}
	if config.SendConcurrency < 1 || config.GlobalRate <= 0 || math.IsNaN(config.GlobalRate) || math.IsInf(config.GlobalRate, 0) ||
		config.DestinationRate <= 0 || math.IsNaN(config.DestinationRate) || math.IsInf(config.DestinationRate, 0) ||
		config.GlobalBurst < 1 || config.DestinationBurst < 1 || config.RedisDB < 0 {
		return RunConfig{}, fmt.Errorf("Telegram concurrency, rates, bursts and Redis DB are invalid")
	}
	if config.ReceiveWait < 0 || config.ReceiveWait > 20*time.Second || config.ReceiveWait%time.Second != 0 ||
		config.InvalidVisibility <= 0 || config.InvalidVisibility > 12*time.Hour || config.InvalidVisibility%time.Second != 0 {
		return RunConfig{}, fmt.Errorf("Telegram queue timing is invalid")
	}
	parsedBase, err := url.Parse(config.BotAPIBaseURL)
	if err != nil || parsedBase.Host == "" || (parsedBase.Scheme != "https" && parsedBase.Scheme != "http") ||
		parsedBase.User != nil || parsedBase.RawQuery != "" || parsedBase.Fragment != "" {
		return RunConfig{}, fmt.Errorf("TELEGRAM_BOT_API_BASE_URL is invalid")
	}
	return config, nil
}
