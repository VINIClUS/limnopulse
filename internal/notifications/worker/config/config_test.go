package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndWorkerEnvironment(t *testing.T) {
	loaded, err := Load(nil, lookup(map[string]string{
		"APP_ENV":                     "local",
		"AWS_REGION":                  "sa-east-1",
		"DYNAMODB_DOMAIN_TABLE":       "domain",
		"DYNAMODB_ENDPOINT_URL":       "http://dynamodb:8000",
		"SQS_NOTIFICATION_JOBS_URL":   "http://sqs:9324/queue/jobs",
		"SQS_ENDPOINT_URL":            "http://sqs:9324",
		"SES_FROM_EMAIL":              "alerts@example.com",
		"SES_ENDPOINT_URL":            "http://ses:8080",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel:4318",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SendConcurrency != 4 || loaded.MaxSendRate != 1 || loaded.SendBurst != 1 {
		t.Fatalf("send defaults = %#v", loaded)
	}
	if loaded.ReceiveWait != 20*time.Second || loaded.ReceiveBatch != 10 ||
		loaded.VisibilityTimeout != 60*time.Second || loaded.ProcessingLease != 60*time.Second ||
		loaded.ProviderTimeout != 15*time.Second || loaded.DrainTimeout != 30*time.Second {
		t.Fatalf("lifecycle defaults = %#v", loaded)
	}
	if loaded.ConfigurationSet != "limnopulse-notifications" {
		t.Fatalf("configuration set = %q", loaded.ConfigurationSet)
	}
	if loaded.SESFromEmail != "alerts@example.com" || loaded.SESEndpoint != "http://ses:8080" ||
		loaded.SQSEndpoint != "http://sqs:9324" || loaded.DynamoDBEndpoint != "http://dynamodb:8000" {
		t.Fatalf("environment = %#v", loaded)
	}
}

func TestLoadRequiresExplicitProductionRateAndValidatesFlags(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "prod", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SES_FROM_EMAIL": "alerts@example.com",
	}
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "NOTIFICATION_MAX_SEND_RATE") {
		t.Fatalf("missing explicit production rate error = %v", err)
	}
	base["NOTIFICATION_MAX_SEND_RATE"] = "12.5"
	loaded, err := Load([]string{"--send-concurrency=7", "--send-burst=3"}, lookup(base))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.MaxSendRate != 12.5 || loaded.SendConcurrency != 7 || loaded.SendBurst != 3 {
		t.Fatalf("overrides = %#v", loaded)
	}
	for _, args := range [][]string{
		{"--send-concurrency=0"}, {"--max-send-rate=0"}, {"--max-send-rate=NaN"},
		{"--max-send-rate=+Inf"}, {"--send-burst=0"}, {"extra"},
	} {
		if _, err := Load(args, lookup(base)); err == nil {
			t.Fatalf("Load(%v) error = nil", args)
		}
	}
}

func lookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
