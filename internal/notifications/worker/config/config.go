package config

import (
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultSendConcurrency     = 4
	DefaultFeedbackConcurrency = 2
	DefaultMaxSendRate         = 1.0
	DefaultSendBurst           = 1
	ReceiveWait                = 20 * time.Second
	ReceiveBatch               = 10
	VisibilityTimeout          = 60 * time.Second
	ProcessingLease            = 60 * time.Second
	ProviderTimeout            = 15 * time.Second
	DrainTimeout               = 30 * time.Second
	RenewalInterval            = 20 * time.Second
	SQSReceiveTimeout          = 25 * time.Second
	SQSRequestTimeout          = 5 * time.Second
	InvalidVisibility          = 60 * time.Second
	OTLPFlushTimeout           = 2 * time.Second
	ConfigurationSet           = "limnopulse-notifications"
)

type LookupEnv func(string) (string, bool)

type RunConfig struct {
	AppEnv              string
	SendConcurrency     int
	FeedbackConcurrency int
	MaxSendRate         float64
	SendBurst           int
	ReceiveWait         time.Duration
	ReceiveBatch        int
	VisibilityTimeout   time.Duration
	ProcessingLease     time.Duration
	ProviderTimeout     time.Duration
	DrainTimeout        time.Duration
	RenewalInterval     time.Duration
	SQSRequestTimeout   time.Duration
	SQSReceiveTimeout   time.Duration
	InvalidVisibility   time.Duration
	OTLPFlushTimeout    time.Duration
	ConfigurationSet    string
	AWSRegion           string
	DynamoDBTable       string
	DynamoDBEndpoint    string
	SQSQueueURL         string
	SQSFeedbackURL      string
	SQSEndpoint         string
	SESFromEmail        string
	SESEndpoint         string
	OTLPEndpoint        string
	EmailSenderMode     string
}

func Load(args []string, lookup LookupEnv) (RunConfig, error) {
	config := RunConfig{
		AppEnv: "local", SendConcurrency: DefaultSendConcurrency,
		FeedbackConcurrency: DefaultFeedbackConcurrency,
		EmailSenderMode:     "aws",
		MaxSendRate:         DefaultMaxSendRate, SendBurst: DefaultSendBurst,
		ReceiveWait: ReceiveWait, ReceiveBatch: ReceiveBatch,
		VisibilityTimeout: VisibilityTimeout, ProcessingLease: ProcessingLease,
		ProviderTimeout: ProviderTimeout, DrainTimeout: DrainTimeout,
		RenewalInterval: RenewalInterval, SQSRequestTimeout: SQSRequestTimeout,
		SQSReceiveTimeout: SQSReceiveTimeout,
		InvalidVisibility: InvalidVisibility, OTLPFlushTimeout: OTLPFlushTimeout,
		ConfigurationSet: ConfigurationSet,
	}
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	for key, target := range map[string]*string{
		"APP_ENV": &config.AppEnv, "AWS_REGION": &config.AWSRegion,
		"DYNAMODB_DOMAIN_TABLE":     &config.DynamoDBTable,
		"DYNAMODB_ENDPOINT_URL":     &config.DynamoDBEndpoint,
		"SQS_NOTIFICATION_JOBS_URL": &config.SQSQueueURL,
		"SQS_SES_EVENTS_URL":        &config.SQSFeedbackURL,
		"SQS_ENDPOINT_URL":          &config.SQSEndpoint,
		"SES_FROM_EMAIL":            &config.SESFromEmail, "SES_ENDPOINT_URL": &config.SESEndpoint,
		"OTEL_EXPORTER_OTLP_ENDPOINT":    &config.OTLPEndpoint,
		"NOTIFICATION_EMAIL_SENDER_MODE": &config.EmailSenderMode,
	} {
		if value, ok := lookup(key); ok {
			*target = strings.TrimSpace(value)
		}
	}
	explicitRate := false
	if value, ok := lookup("NOTIFICATION_MAX_SEND_RATE"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return RunConfig{}, fmt.Errorf("NOTIFICATION_MAX_SEND_RATE must be a positive number")
		}
		config.MaxSendRate = parsed
		explicitRate = true
	}
	if value, ok := lookup("NOTIFICATION_SEND_CONCURRENCY"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return RunConfig{}, fmt.Errorf("NOTIFICATION_SEND_CONCURRENCY must be a positive integer")
		}
		config.SendConcurrency = parsed
	}
	if value, ok := lookup("NOTIFICATION_FEEDBACK_CONCURRENCY"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return RunConfig{}, fmt.Errorf("NOTIFICATION_FEEDBACK_CONCURRENCY must be a positive integer")
		}
		config.FeedbackConcurrency = parsed
	}
	if value, ok := lookup("NOTIFICATION_SEND_BURST"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return RunConfig{}, fmt.Errorf("NOTIFICATION_SEND_BURST must be a positive integer")
		}
		config.SendBurst = parsed
	}

	fs := flag.NewFlagSet("notifications worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.IntVar(&config.SendConcurrency, "send-concurrency", config.SendConcurrency, "concurrent email sends")
	fs.IntVar(&config.FeedbackConcurrency, "feedback-concurrency", config.FeedbackConcurrency, "concurrent SES feedback messages")
	fs.Float64Var(&config.MaxSendRate, "max-send-rate", config.MaxSendRate, "maximum provider calls per second")
	fs.IntVar(&config.SendBurst, "send-burst", config.SendBurst, "send rate limiter burst")
	if err := fs.Parse(args); err != nil {
		return RunConfig{}, err
	}
	if len(fs.Args()) != 0 {
		return RunConfig{}, fmt.Errorf("worker does not accept positional arguments")
	}
	for _, argument := range args {
		if argument == "--max-send-rate" || strings.HasPrefix(argument, "--max-send-rate=") {
			explicitRate = true
		}
	}
	if config.AppEnv == "staging" || config.AppEnv == "prod" {
		if !explicitRate {
			return RunConfig{}, fmt.Errorf("NOTIFICATION_MAX_SEND_RATE must be explicit in staging and prod")
		}
	} else if config.AppEnv != "local" && config.AppEnv != "test" {
		return RunConfig{}, fmt.Errorf("APP_ENV must be local, test, staging or prod")
	}
	switch config.EmailSenderMode {
	case "aws":
	case "success", "retryable", "permanent", "ambiguous_timeout", "connection_reset":
		if config.AppEnv != "local" && config.AppEnv != "test" {
			return RunConfig{}, fmt.Errorf("fake email sender is allowed only in local or test")
		}
	default:
		return RunConfig{}, fmt.Errorf("NOTIFICATION_EMAIL_SENDER_MODE is invalid")
	}
	for _, required := range []struct{ name, value string }{
		{"AWS_REGION", config.AWSRegion}, {"DYNAMODB_DOMAIN_TABLE", config.DynamoDBTable},
		{"SQS_NOTIFICATION_JOBS_URL", config.SQSQueueURL}, {"SES_FROM_EMAIL", config.SESFromEmail},
		{"SQS_SES_EVENTS_URL", config.SQSFeedbackURL},
	} {
		if required.value == "" {
			return RunConfig{}, fmt.Errorf("%s is required", required.name)
		}
	}
	if config.SQSQueueURL == config.SQSFeedbackURL {
		return RunConfig{}, fmt.Errorf("SQS_NOTIFICATION_JOBS_URL and SQS_SES_EVENTS_URL must be distinct")
	}
	if config.SendConcurrency < 1 || config.FeedbackConcurrency < 1 || config.MaxSendRate <= 0 || math.IsNaN(config.MaxSendRate) ||
		math.IsInf(config.MaxSendRate, 0) || config.SendBurst < 1 {
		return RunConfig{}, fmt.Errorf("send concurrency, rate and burst must be positive")
	}
	return config, nil
}
