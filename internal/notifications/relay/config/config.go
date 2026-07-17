package config

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

const (
	GlobalDeadline    = 45 * time.Second
	SoftDeadline      = 40 * time.Second
	DrainBudget       = 5 * time.Second
	ItemTimeout       = 10 * time.Second
	LeaseTTL          = 20 * time.Second
	SQSRequestTimeout = 5 * time.Second
	OTLPFlushTimeout  = 2 * time.Second
)

type LookupEnv func(string) (string, bool)

type RunConfig struct {
	RelayTime        *time.Time
	BudgetStartedAt  *time.Time
	Shard            int
	ShardCount       int
	QueryParallelism int
	WorkParallelism  int
	MaxWork          int
	FanoutPageSize   int

	GlobalDeadline    time.Duration
	SoftDeadline      time.Duration
	DrainBudget       time.Duration
	ItemTimeout       time.Duration
	LeaseTTL          time.Duration
	SQSRequestTimeout time.Duration
	OTLPFlushTimeout  time.Duration

	AWSRegion        string
	DynamoDBTable    string
	DynamoDBEndpoint string
	SQSQueueURL      string
	SQSEndpoint      string
	OTLPEndpoint     string
}

func Load(args []string, lookup LookupEnv) (RunConfig, error) {
	config := RunConfig{
		ShardCount:        1,
		QueryParallelism:  4,
		WorkParallelism:   8,
		MaxWork:           250,
		FanoutPageSize:    20,
		GlobalDeadline:    GlobalDeadline,
		SoftDeadline:      SoftDeadline,
		DrainBudget:       DrainBudget,
		ItemTimeout:       ItemTimeout,
		LeaseTTL:          LeaseTTL,
		SQSRequestTimeout: SQSRequestTimeout,
		OTLPFlushTimeout:  OTLPFlushTimeout,
	}
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	var err error
	for key, target := range map[string]*string{
		"AWS_REGION":                  &config.AWSRegion,
		"DYNAMODB_DOMAIN_TABLE":       &config.DynamoDBTable,
		"DYNAMODB_ENDPOINT_URL":       &config.DynamoDBEndpoint,
		"SQS_NOTIFICATION_JOBS_URL":   &config.SQSQueueURL,
		"SQS_ENDPOINT_URL":            &config.SQSEndpoint,
		"OTEL_EXPORTER_OTLP_ENDPOINT": &config.OTLPEndpoint,
	} {
		if value, ok := lookup(key); ok {
			*target = strings.TrimSpace(value)
		}
	}
	for _, required := range []struct {
		name  string
		value string
	}{
		{name: "AWS_REGION", value: config.AWSRegion},
		{name: "DYNAMODB_DOMAIN_TABLE", value: config.DynamoDBTable},
		{name: "SQS_NOTIFICATION_JOBS_URL", value: config.SQSQueueURL},
	} {
		if required.value == "" {
			return RunConfig{}, fmt.Errorf("%s is required", required.name)
		}
	}

	fs := flag.NewFlagSet("notifications relay", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	relayTime := ""
	fs.StringVar(&relayTime, "relay-time", "", "logical RFC3339 relay time")
	fs.IntVar(&config.Shard, "shard", config.Shard, "logical shard number")
	fs.IntVar(&config.ShardCount, "shard-count", config.ShardCount, "logical shard count")
	fs.IntVar(&config.QueryParallelism, "query-parallelism", config.QueryParallelism, "parallel GSI queries")
	fs.IntVar(&config.WorkParallelism, "work-parallelism", config.WorkParallelism, "parallel relay work")
	fs.IntVar(&config.MaxWork, "max-work", config.MaxWork, "maximum work items per run")
	fs.IntVar(&config.FanoutPageSize, "fanout-page-size", config.FanoutPageSize, "fanout transaction page size")
	if err = fs.Parse(args); err != nil {
		return RunConfig{}, err
	}
	if len(fs.Args()) != 0 {
		return RunConfig{}, fmt.Errorf("relay does not accept positional arguments")
	}
	if relayTime != "" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, relayTime)
		if parseErr != nil {
			return RunConfig{}, fmt.Errorf("relay-time: %w", parseErr)
		}
		parsed = parsed.UTC()
		config.RelayTime = &parsed
	}
	if _, err = notifications.OwnedRelayBuckets(config.Shard, config.ShardCount); err != nil {
		return RunConfig{}, err
	}
	if config.QueryParallelism < 1 || config.WorkParallelism < 1 || config.MaxWork < 1 {
		return RunConfig{}, fmt.Errorf("parallelism and work limits must be positive")
	}
	if config.FanoutPageSize < 1 || config.FanoutPageSize > 99 {
		return RunConfig{}, fmt.Errorf("fanout-page-size must be between 1 and 99")
	}
	return config, nil
}
