package config

import (
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
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
	FakeMessageID       string
}

// Load reads the combined jobs and SES-feedback worker of cmd/notifications.
func Load(args []string, lookup LookupEnv) (RunConfig, error) {
	return load("notifications worker", args, lookup, true)
}

// LoadEmail reads the email-jobs consumer alone (ADR-020 email-worker). It needs no SES
// events queue and accepts no feedback setting; cmd/ses-feedback-worker has its own loader.
func LoadEmail(args []string, lookup LookupEnv) (RunConfig, error) {
	return load("email-worker", args, lookup, false)
}

func load(name string, args []string, lookup LookupEnv, withFeedback bool) (RunConfig, error) {
	config := RunConfig{
		AppEnv: "local", SendConcurrency: DefaultSendConcurrency,
		EmailSenderMode: "aws",
		MaxSendRate:     DefaultMaxSendRate, SendBurst: DefaultSendBurst,
		ReceiveWait: ReceiveWait, ReceiveBatch: ReceiveBatch,
		VisibilityTimeout: VisibilityTimeout, ProcessingLease: ProcessingLease,
		ProviderTimeout: ProviderTimeout, DrainTimeout: DrainTimeout,
		RenewalInterval: RenewalInterval, SQSRequestTimeout: SQSRequestTimeout,
		SQSReceiveTimeout: SQSReceiveTimeout,
		InvalidVisibility: InvalidVisibility, OTLPFlushTimeout: OTLPFlushTimeout,
		ConfigurationSet: ConfigurationSet,
	}
	if withFeedback {
		config.FeedbackConcurrency = DefaultFeedbackConcurrency
	}
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	textValues := map[string]*string{
		"APP_ENV": &config.AppEnv, "AWS_REGION": &config.AWSRegion,
		"DYNAMODB_DOMAIN_TABLE":     &config.DynamoDBTable,
		"DYNAMODB_ENDPOINT_URL":     &config.DynamoDBEndpoint,
		"SQS_NOTIFICATION_JOBS_URL": &config.SQSQueueURL,
		"SQS_ENDPOINT_URL":          &config.SQSEndpoint,
		"SES_FROM_EMAIL":            &config.SESFromEmail, "SES_ENDPOINT_URL": &config.SESEndpoint,
		"SES_CONFIGURATION_SET_NAME":     &config.ConfigurationSet,
		"OTEL_EXPORTER_OTLP_ENDPOINT":    &config.OTLPEndpoint,
		"NOTIFICATION_EMAIL_SENDER_MODE": &config.EmailSenderMode,
		"NOTIFICATION_FAKE_MESSAGE_ID":   &config.FakeMessageID,
	}
	integerValues := map[string]*int{
		"NOTIFICATION_SEND_CONCURRENCY": &config.SendConcurrency,
		"NOTIFICATION_SEND_BURST":       &config.SendBurst,
	}
	if withFeedback {
		textValues["SQS_SES_EVENTS_URL"] = &config.SQSFeedbackURL
		integerValues["NOTIFICATION_FEEDBACK_CONCURRENCY"] = &config.FeedbackConcurrency
	}
	for key, target := range textValues {
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
	for key, target := range integerValues {
		if value, ok := lookup(key); ok && strings.TrimSpace(value) != "" {
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return RunConfig{}, fmt.Errorf("%s must be a positive integer", key)
			}
			*target = parsed
		}
	}

	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.IntVar(&config.SendConcurrency, "send-concurrency", config.SendConcurrency, "concurrent email sends")
	if withFeedback {
		fs.IntVar(&config.FeedbackConcurrency, "feedback-concurrency", config.FeedbackConcurrency, "concurrent SES feedback messages")
	}
	fs.Float64Var(&config.MaxSendRate, "max-send-rate", config.MaxSendRate, "maximum provider calls per second")
	fs.IntVar(&config.SendBurst, "send-burst", config.SendBurst, "send rate limiter burst")
	fs.DurationVar(&config.ReceiveWait, "receive-wait", config.ReceiveWait, "SQS long-poll wait (0s disables only in local/test)")
	fs.DurationVar(&config.InvalidVisibility, "invalid-visibility", config.InvalidVisibility, "visibility delay for invalid messages (short overrides only in local/test)")
	if err := workerproc.ParseFlags(fs, args); err != nil {
		return RunConfig{}, err
	}
	if len(fs.Args()) != 0 {
		return RunConfig{}, fmt.Errorf("worker does not accept positional arguments")
	}
	fs.Visit(func(item *flag.Flag) {
		if item.Name == "max-send-rate" {
			explicitRate = true
		}
	})
	hosted, err := HostedEnvironment(config.AppEnv)
	if err != nil {
		return RunConfig{}, err
	}
	if hosted && !explicitRate {
		return RunConfig{}, fmt.Errorf("NOTIFICATION_MAX_SEND_RATE must be explicit in staging and prod")
	}
	switch config.EmailSenderMode {
	case "aws":
	case "success", "retryable", "permanent", "ambiguous_timeout", "connection_reset":
		if hosted {
			return RunConfig{}, fmt.Errorf("fake email sender is allowed only in local or test")
		}
	default:
		return RunConfig{}, fmt.Errorf("NOTIFICATION_EMAIL_SENDER_MODE is invalid")
	}
	if strings.ContainsRune(config.FakeMessageID, '\x00') {
		return RunConfig{}, fmt.Errorf("NOTIFICATION_FAKE_MESSAGE_ID is invalid")
	}
	if config.FakeMessageID != "" && config.EmailSenderMode == "aws" {
		return RunConfig{}, fmt.Errorf("fake message ID requires a fake email sender")
	}
	required := []struct{ name, value string }{
		{"AWS_REGION", config.AWSRegion}, {"DYNAMODB_DOMAIN_TABLE", config.DynamoDBTable},
		{"SQS_NOTIFICATION_JOBS_URL", config.SQSQueueURL}, {"SES_FROM_EMAIL", config.SESFromEmail},
		{"SES_CONFIGURATION_SET_NAME", config.ConfigurationSet},
	}
	if withFeedback {
		required = append(required, struct{ name, value string }{"SQS_SES_EVENTS_URL", config.SQSFeedbackURL})
	}
	for _, item := range required {
		if item.value == "" {
			return RunConfig{}, fmt.Errorf("%s is required", item.name)
		}
	}
	if err := (worker.SESConfigurationSetName(config.ConfigurationSet)).Validate(); err != nil {
		return RunConfig{}, fmt.Errorf("SES_CONFIGURATION_SET_NAME is invalid: %w", err)
	}
	if withFeedback && config.SQSQueueURL == config.SQSFeedbackURL {
		return RunConfig{}, fmt.Errorf("SQS_NOTIFICATION_JOBS_URL and SQS_SES_EVENTS_URL must be distinct")
	}
	if config.SendConcurrency < 1 || (withFeedback && config.FeedbackConcurrency < 1) || config.MaxSendRate <= 0 ||
		math.IsNaN(config.MaxSendRate) || math.IsInf(config.MaxSendRate, 0) || config.SendBurst < 1 {
		return RunConfig{}, fmt.Errorf("send concurrency, rate and burst must be positive")
	}
	if err := ValidateQueueTiming(hosted, config.ReceiveWait, config.InvalidVisibility); err != nil {
		return RunConfig{}, err
	}
	return config, nil
}

// HostedEnvironment reports whether APP_ENV names a hosted environment (staging or prod),
// and rejects anything but local, test, staging and prod.
func HostedEnvironment(appEnv string) (bool, error) {
	switch appEnv {
	case "staging", "prod":
		return true, nil
	case "local", "test":
		return false, nil
	default:
		return false, fmt.Errorf("APP_ENV must be local, test, staging or prod")
	}
}

// ValidateQueueTiming checks the receive wait and the invalid-message visibility every SQS
// consumer shares. Hosted environments keep the long poll and the default visibility.
func ValidateQueueTiming(hosted bool, receiveWait, invalidVisibility time.Duration) error {
	if hosted && receiveWait == 0 {
		return fmt.Errorf("zero receive wait is allowed only in local or test")
	}
	if hosted && invalidVisibility != InvalidVisibility {
		return fmt.Errorf("short invalid visibility is allowed only in local or test")
	}
	if receiveWait < 0 || receiveWait > 20*time.Second || receiveWait%time.Second != 0 {
		return fmt.Errorf("receive wait must be a whole number of seconds between 0s and 20s")
	}
	if invalidVisibility <= 0 || invalidVisibility > 12*time.Hour || invalidVisibility%time.Second != 0 {
		return fmt.Errorf("invalid visibility must be a whole number of seconds between 1s and 12h")
	}
	return nil
}
