// Package config loads the SES-feedback consumer (ADR-020 ses-feedback-worker): SQS SES
// events → delivery state. It reads only what that consumer uses; the email-jobs consumer
// has its own loader, worker/config.LoadEmail.
package config

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

const DefaultConcurrency = workerconfig.DefaultFeedbackConcurrency

type LookupEnv func(string) (string, bool)

type RunConfig struct {
	AppEnv            string
	Concurrency       int
	ReceiveWait       time.Duration
	ReceiveBatch      int
	VisibilityTimeout time.Duration
	DrainTimeout      time.Duration
	SQSRequestTimeout time.Duration
	SQSReceiveTimeout time.Duration
	InvalidVisibility time.Duration
	AWSRegion         string
	DynamoDBTable     string
	DynamoDBEndpoint  string
	SQSQueueURL       string
	SQSEndpoint       string
}

func Load(args []string, lookup LookupEnv) (RunConfig, error) {
	config := RunConfig{
		AppEnv: "local", Concurrency: DefaultConcurrency,
		ReceiveWait: workerconfig.ReceiveWait, ReceiveBatch: workerconfig.ReceiveBatch,
		VisibilityTimeout: workerconfig.VisibilityTimeout, DrainTimeout: workerconfig.DrainTimeout,
		SQSRequestTimeout: workerconfig.SQSRequestTimeout, SQSReceiveTimeout: workerconfig.SQSReceiveTimeout,
		InvalidVisibility: workerconfig.InvalidVisibility,
	}
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	for key, target := range map[string]*string{
		"APP_ENV": &config.AppEnv, "AWS_REGION": &config.AWSRegion,
		"DYNAMODB_DOMAIN_TABLE": &config.DynamoDBTable, "DYNAMODB_ENDPOINT_URL": &config.DynamoDBEndpoint,
		"SQS_SES_EVENTS_URL": &config.SQSQueueURL, "SQS_ENDPOINT_URL": &config.SQSEndpoint,
	} {
		if value, ok := lookup(key); ok {
			*target = strings.TrimSpace(value)
		}
	}
	if value, ok := lookup("NOTIFICATION_FEEDBACK_CONCURRENCY"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return RunConfig{}, fmt.Errorf("NOTIFICATION_FEEDBACK_CONCURRENCY must be a positive integer")
		}
		config.Concurrency = parsed
	}

	fs := flag.NewFlagSet("ses-feedback-worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.IntVar(&config.Concurrency, "feedback-concurrency", config.Concurrency, "concurrent SES feedback messages")
	fs.DurationVar(&config.ReceiveWait, "receive-wait", config.ReceiveWait, "SQS long-poll wait (0s disables only in local/test)")
	fs.DurationVar(&config.InvalidVisibility, "invalid-visibility", config.InvalidVisibility, "visibility delay for invalid messages (short overrides only in local/test)")
	if err := workerproc.ParseFlags(fs, args); err != nil {
		return RunConfig{}, err
	}
	if len(fs.Args()) != 0 {
		return RunConfig{}, fmt.Errorf("SES feedback worker does not accept positional arguments")
	}
	hosted, err := workerconfig.HostedEnvironment(config.AppEnv)
	if err != nil {
		return RunConfig{}, err
	}
	for _, required := range []struct{ name, value string }{
		{"AWS_REGION", config.AWSRegion}, {"DYNAMODB_DOMAIN_TABLE", config.DynamoDBTable},
		{"SQS_SES_EVENTS_URL", config.SQSQueueURL},
	} {
		if required.value == "" {
			return RunConfig{}, fmt.Errorf("%s is required", required.name)
		}
	}
	if config.Concurrency < 1 {
		return RunConfig{}, fmt.Errorf("feedback concurrency must be positive")
	}
	if err := workerconfig.ValidateQueueTiming(hosted, config.ReceiveWait, config.InvalidVisibility); err != nil {
		return RunConfig{}, err
	}
	return config, nil
}
