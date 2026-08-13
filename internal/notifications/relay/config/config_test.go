package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadAcceptsRelayFlagsAndRequiredEnvironment(t *testing.T) {
	environment := map[string]string{
		"AWS_REGION":                  "sa-east-1",
		"DYNAMODB_DOMAIN_TABLE":       "domain",
		"DYNAMODB_ENDPOINT_URL":       "http://dynamodb:8000",
		"SQS_NOTIFICATION_JOBS_URL":   "http://sqs:9324/queue/jobs",
		"SQS_TELEGRAM_JOBS_URL":       "http://sqs:9324/queue/telegram-jobs",
		"TELEGRAM_DELIVERY_ENABLED":   "true",
		"LIMNOPULSE_WEB_URL":          "https://app.example.com",
		"SQS_ENDPOINT_URL":            "http://sqs:9324",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel:4318",
	}
	lookup := func(key string) (string, bool) {
		value, ok := environment[key]
		return value, ok
	}

	loaded, err := Load([]string{
		"--relay-time=2026-07-16T12:34:56.123456789-03:00",
		"--shard=2",
		"--shard-count=4",
		"--query-parallelism=3",
		"--work-parallelism=7",
		"--max-work=99",
		"--fanout-page-size=17",
	}, lookup)
	if err != nil {
		t.Fatal(err)
	}
	wantRelayTime := time.Date(2026, 7, 16, 15, 34, 56, 123456789, time.UTC)
	if loaded.RelayTime == nil || !loaded.RelayTime.Equal(wantRelayTime) {
		t.Fatalf("relay time = %#v, want %s", loaded.RelayTime, wantRelayTime)
	}
	if loaded.Shard != 2 || loaded.ShardCount != 4 || loaded.QueryParallelism != 3 ||
		loaded.WorkParallelism != 7 || loaded.MaxWork != 99 || loaded.FanoutPageSize != 17 {
		t.Fatalf("flags not loaded: %#v", loaded)
	}
	if loaded.GlobalDeadline != 45*time.Second || loaded.SoftDeadline != 40*time.Second ||
		loaded.DrainBudget != 5*time.Second || loaded.ItemTimeout != 10*time.Second ||
		loaded.LeaseTTL != 20*time.Second || loaded.SQSRequestTimeout != 5*time.Second ||
		loaded.OTLPFlushTimeout != 2*time.Second {
		t.Fatalf("fixed deadlines changed: %#v", loaded)
	}
	if loaded.AWSRegion != "sa-east-1" || loaded.DynamoDBTable != "domain" ||
		loaded.DynamoDBEndpoint != "http://dynamodb:8000" ||
		loaded.SQSQueueURL != "http://sqs:9324/queue/jobs" ||
		loaded.SQSTelegramQueueURL != "http://sqs:9324/queue/telegram-jobs" ||
		!loaded.TelegramDeliveryEnabled || !loaded.TelegramDeliveryConfigured || loaded.WebURL != "https://app.example.com" ||
		loaded.SQSEndpoint != "http://sqs:9324" || loaded.OTLPEndpoint != "http://otel:4318" {
		t.Fatalf("environment not loaded: %#v", loaded)
	}
}

func TestLoadKeepsTelegramFeatureOffWithoutQueueAndRequiresDependenciesWhenEnabled(t *testing.T) {
	loaded, err := Load(nil, requiredLookup())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TelegramDeliveryEnabled || loaded.TelegramDeliveryConfigured || loaded.SQSTelegramQueueURL != "" {
		t.Fatalf("Telegram defaults = %#v", loaded)
	}
	values := map[string]string{
		"AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "email", "TELEGRAM_DELIVERY_ENABLED": "true",
	}
	_, err = Load(nil, func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err == nil || !strings.Contains(err.Error(), "SQS_TELEGRAM_JOBS_URL") {
		t.Fatalf("enabled Telegram missing queue error = %v", err)
	}
}

func TestLoadDefaultsOneShotLimitsAndAllowsClockCapturedRelayTime(t *testing.T) {
	loaded, err := Load(nil, requiredLookup())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.RelayTime != nil || loaded.Shard != 0 || loaded.ShardCount != 1 ||
		loaded.QueryParallelism != 4 || loaded.WorkParallelism != 8 ||
		loaded.MaxWork != 250 || loaded.FanoutPageSize != 20 {
		t.Fatalf("defaults = %#v", loaded)
	}
}

func TestLoadAllowsCloudDynamoWithoutLocalEndpoint(t *testing.T) {
	values := map[string]string{
		"AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs.sa-east-1.amazonaws.com/123/jobs",
	}
	loaded, err := Load(nil, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.DynamoDBEndpoint != "" || loaded.SQSEndpoint != "" {
		t.Fatalf("cloud endpoints = %#v", loaded)
	}
}

func TestLoadRejectsMissingEnvironmentAndInvalidFlags(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		lookup LookupEnv
	}{
		{name: "missing environment", lookup: func(string) (string, bool) { return "", false }},
		{name: "blank environment", lookup: func(string) (string, bool) { return "  ", true }},
		{name: "bad relay time", args: []string{"--relay-time=tomorrow"}, lookup: requiredLookup()},
		{name: "negative shard", args: []string{"--shard=-1"}, lookup: requiredLookup()},
		{name: "shard outside count", args: []string{"--shard=1", "--shard-count=1"}, lookup: requiredLookup()},
		{name: "too many shards", args: []string{"--shard-count=65"}, lookup: requiredLookup()},
		{name: "zero query parallelism", args: []string{"--query-parallelism=0"}, lookup: requiredLookup()},
		{name: "zero work parallelism", args: []string{"--work-parallelism=0"}, lookup: requiredLookup()},
		{name: "zero max work", args: []string{"--max-work=0"}, lookup: requiredLookup()},
		{name: "zero page", args: []string{"--fanout-page-size=0"}, lookup: requiredLookup()},
		{name: "transaction overflow page", args: []string{"--fanout-page-size=100"}, lookup: requiredLookup()},
		{name: "positional argument", args: []string{"extra"}, lookup: requiredLookup()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(test.args, test.lookup)
			if err == nil {
				t.Fatal("Load() error = nil")
			}
			if strings.Contains(err.Error(), "http://sqs:9324/queue/private") {
				t.Fatalf("configuration error leaked queue URL: %v", err)
			}
		})
	}
}

func requiredLookup() LookupEnv {
	values := map[string]string{
		"AWS_REGION":                "us-east-1",
		"DYNAMODB_DOMAIN_TABLE":     "domain",
		"DYNAMODB_ENDPOINT_URL":     "http://dynamodb:8000",
		"SQS_NOTIFICATION_JOBS_URL": "http://sqs:9324/queue/private",
	}
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
