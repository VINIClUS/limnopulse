package config

import (
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

func feedbackEnvironment() map[string]string {
	return map[string]string{
		"APP_ENV": "local", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb:8000", "SQS_SES_EVENTS_URL": "http://sqs:9324/queue/ses-events",
		"SQS_ENDPOINT_URL": "http://sqs:9324",
	}
}

func lookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestLoadReadsOnlyTheFeedbackConsumer(t *testing.T) {
	environment := feedbackEnvironment()
	// Email settings belong to the email-jobs consumer; this loader never needs them.
	environment["SES_FROM_EMAIL"] = ""
	environment["NOTIFICATION_MAX_SEND_RATE"] = "not-a-rate"
	loaded, err := Load(nil, lookup(environment))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Concurrency != DefaultConcurrency || loaded.ReceiveWait != 20*time.Second ||
		loaded.ReceiveBatch != 10 || loaded.VisibilityTimeout != time.Minute ||
		loaded.DrainTimeout != 30*time.Second || loaded.SQSReceiveTimeout != 25*time.Second ||
		loaded.SQSRequestTimeout != 5*time.Second || loaded.InvalidVisibility != time.Minute {
		t.Fatalf("defaults = %#v", loaded)
	}
	if loaded.AWSRegion != "sa-east-1" || loaded.DynamoDBTable != "domain" ||
		loaded.DynamoDBEndpoint != "http://dynamodb:8000" || loaded.SQSEndpoint != "http://sqs:9324" ||
		loaded.SQSQueueURL != "http://sqs:9324/queue/ses-events" {
		t.Fatalf("environment = %#v", loaded)
	}
}

func TestLoadTakesConcurrencyFromEnvironmentAndFlags(t *testing.T) {
	environment := feedbackEnvironment()
	environment["NOTIFICATION_FEEDBACK_CONCURRENCY"] = "3"
	loaded, err := Load(nil, lookup(environment))
	if err != nil || loaded.Concurrency != 3 {
		t.Fatalf("config=%#v err=%v", loaded, err)
	}
	loaded, err = Load([]string{"--feedback-concurrency=5", "--receive-wait=0s", "--invalid-visibility=1s"}, lookup(environment))
	if err != nil || loaded.Concurrency != 5 || loaded.ReceiveWait != 0 || loaded.InvalidVisibility != time.Second {
		t.Fatalf("config=%#v err=%v", loaded, err)
	}
	environment["NOTIFICATION_FEEDBACK_CONCURRENCY"] = "x"
	if _, err := Load(nil, lookup(environment)); err == nil || !strings.Contains(err.Error(), "NOTIFICATION_FEEDBACK_CONCURRENCY") {
		t.Fatalf("bad concurrency error = %v", err)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	hosted := feedbackEnvironment()
	hosted["APP_ENV"] = "prod"
	if _, err := Load(nil, lookup(hosted)); err != nil {
		t.Fatalf("hosted defaults rejected: %v", err)
	}
	for name, test := range map[string]struct {
		args        []string
		environment map[string]string
		fragment    string
	}{
		"missing queue":    {environment: map[string]string{"AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain"}, fragment: "SQS_SES_EVENTS_URL"},
		"missing table":    {environment: map[string]string{"AWS_REGION": "sa-east-1", "SQS_SES_EVENTS_URL": "queue"}, fragment: "DYNAMODB_DOMAIN_TABLE"},
		"unknown env":      {environment: map[string]string{"APP_ENV": "dev"}, fragment: "APP_ENV"},
		"zero concurrency": {args: []string{"--feedback-concurrency=0"}, environment: feedbackEnvironment(), fragment: "concurrency"},
		"positional":       {args: []string{"extra"}, environment: feedbackEnvironment(), fragment: "positional"},
		"hosted zero wait": {args: []string{"--receive-wait=0s"}, environment: hosted, fragment: "zero receive wait"},
		"fractional wait":  {args: []string{"--receive-wait=500ms"}, environment: feedbackEnvironment(), fragment: "receive wait"},
	} {
		if _, err := Load(test.args, lookup(test.environment)); err == nil || !strings.Contains(err.Error(), test.fragment) {
			t.Fatalf("%s: error = %v, want %q", name, err, test.fragment)
		}
	}
}

func TestLoadReportsBadFlagsAsConfigurationErrors(t *testing.T) {
	for _, args := range [][]string{{"--send-concurrency=1"}, {"--feedback-concurrency=x"}, {"--max-send-rate=1"}} {
		if _, err := Load(args, lookup(feedbackEnvironment())); !workerproc.IsConfiguration(err) {
			t.Fatalf("Load(%v) error = %v, want a configuration error", args, err)
		}
	}
	if _, err := Load(nil, nil); err == nil {
		t.Fatal("an empty environment was accepted")
	}
}
