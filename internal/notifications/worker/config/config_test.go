package config

import (
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

func TestLoadDefaultsAndWorkerEnvironment(t *testing.T) {
	loaded, err := Load(nil, lookup(map[string]string{
		"APP_ENV":                        "local",
		"AWS_REGION":                     "sa-east-1",
		"DYNAMODB_DOMAIN_TABLE":          "domain",
		"DYNAMODB_ENDPOINT_URL":          "http://dynamodb:8000",
		"SQS_NOTIFICATION_JOBS_URL":      "http://sqs:9324/queue/jobs",
		"SQS_SES_EVENTS_URL":             "http://sqs:9324/queue/ses-events",
		"SQS_ENDPOINT_URL":               "http://sqs:9324",
		"SES_FROM_EMAIL":                 "alerts@example.com",
		"SES_CONFIGURATION_SET_NAME":     "custom-notifications",
		"SES_ENDPOINT_URL":               "http://ses:8080",
		"OTEL_EXPORTER_OTLP_ENDPOINT":    "http://otel:4318",
		"NOTIFICATION_EMAIL_SENDER_MODE": "success",
		"NOTIFICATION_FAKE_MESSAGE_ID":   "provider_message_local_1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SendConcurrency != 4 || loaded.FeedbackConcurrency != 2 || loaded.MaxSendRate != 1 || loaded.SendBurst != 1 {
		t.Fatalf("send defaults = %#v", loaded)
	}
	if loaded.ReceiveWait != 20*time.Second || loaded.ReceiveBatch != 10 ||
		loaded.VisibilityTimeout != 60*time.Second || loaded.ProcessingLease != 60*time.Second ||
		loaded.ProviderTimeout != 15*time.Second || loaded.DrainTimeout != 30*time.Second {
		t.Fatalf("lifecycle defaults = %#v", loaded)
	}
	if loaded.ConfigurationSet != "custom-notifications" {
		t.Fatalf("configuration set = %q", loaded.ConfigurationSet)
	}
	if loaded.SESFromEmail != "alerts@example.com" || loaded.SESEndpoint != "http://ses:8080" ||
		loaded.SQSEndpoint != "http://sqs:9324" || loaded.DynamoDBEndpoint != "http://dynamodb:8000" ||
		loaded.SQSFeedbackURL != "http://sqs:9324/queue/ses-events" ||
		loaded.FakeMessageID != "provider_message_local_1" {
		t.Fatalf("environment = %#v", loaded)
	}
}

func TestLoadAllowsFakeMessageIDOnlyForLocalFakeSender(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SQS_SES_EVENTS_URL": "https://sqs/events",
		"SES_FROM_EMAIL": "alerts@example.com", "NOTIFICATION_FAKE_MESSAGE_ID": "provider_message_1",
	}
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "fake message ID") {
		t.Fatalf("AWS sender accepted fake message ID: %v", err)
	}
	base["NOTIFICATION_EMAIL_SENDER_MODE"] = "success"
	if _, err := Load(nil, lookup(base)); err != nil {
		t.Fatalf("local fake sender rejected configured message ID: %v", err)
	}
	base["APP_ENV"] = "prod"
	base["NOTIFICATION_MAX_SEND_RATE"] = "1"
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "fake email sender") {
		t.Fatalf("production accepted fake sender: %v", err)
	}
}

func TestLoadRequiresExplicitProductionRateAndValidatesFlags(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "prod", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SQS_SES_EVENTS_URL": "https://sqs/ses-events",
		"SES_FROM_EMAIL": "alerts@example.com",
	}
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "NOTIFICATION_MAX_SEND_RATE") {
		t.Fatalf("missing explicit production rate error = %v", err)
	}
	base["NOTIFICATION_MAX_SEND_RATE"] = "12.5"
	loaded, err := Load([]string{"--send-concurrency=7", "--feedback-concurrency=5", "--send-burst=3"}, lookup(base))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.MaxSendRate != 12.5 || loaded.SendConcurrency != 7 || loaded.FeedbackConcurrency != 5 || loaded.SendBurst != 3 {
		t.Fatalf("overrides = %#v", loaded)
	}
	local := maps.Clone(base)
	local["APP_ENV"] = "test"
	loaded, err = Load([]string{"--receive-wait=0s"}, lookup(local))
	if err != nil || loaded.ReceiveWait != 0 {
		t.Fatalf("zero receive wait for deterministic local execution = %#v, %v", loaded, err)
	}
	loaded, err = Load([]string{"--invalid-visibility=1s"}, lookup(local))
	if err != nil || loaded.InvalidVisibility != time.Second {
		t.Fatalf("short invalid visibility for bounded local redrive = %#v, %v", loaded, err)
	}
	if _, err := Load([]string{"--invalid-visibility=1s"}, lookup(base)); err == nil {
		t.Fatal("short invalid visibility was accepted in production")
	}
	for _, args := range [][]string{
		{"--send-concurrency=0"}, {"--max-send-rate=0"}, {"--max-send-rate=NaN"},
		{"--max-send-rate=+Inf"}, {"--send-burst=0"}, {"--feedback-concurrency=0"},
		{"--receive-wait=0s"}, {"--receive-wait=-1s"}, {"--receive-wait=21s"},
		{"--receive-wait=500ms"}, {"--invalid-visibility=0s"},
		{"--invalid-visibility=500ms"}, {"extra"},
	} {
		if _, err := Load(args, lookup(base)); err == nil {
			t.Fatalf("Load(%v) error = nil", args)
		}
	}
}

func TestLoadRequiresIndependentSESFeedbackQueue(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SES_FROM_EMAIL": "alerts@example.com",
	}
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "SQS_SES_EVENTS_URL") {
		t.Fatalf("missing feedback queue error = %v", err)
	}
	base["SQS_SES_EVENTS_URL"] = "https://sqs/ses-events"
	base["NOTIFICATION_FEEDBACK_CONCURRENCY"] = "3"
	loaded, err := Load(nil, lookup(base))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SQSFeedbackURL != "https://sqs/ses-events" || loaded.FeedbackConcurrency != 3 {
		t.Fatalf("feedback config = %#v", loaded)
	}
	base["SQS_SES_EVENTS_URL"] = base["SQS_NOTIFICATION_JOBS_URL"]
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("shared jobs and feedback queue error = %v", err)
	}
}

func TestLoadRejectsInvalidSESConfigurationSetName(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SQS_SES_EVENTS_URL": "https://sqs/events",
		"SES_FROM_EMAIL": "alerts@example.com",
	}
	for _, name := range []string{"contains spaces", "contains/slash", strings.Repeat("a", 65)} {
		base["SES_CONFIGURATION_SET_NAME"] = name
		if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "SES_CONFIGURATION_SET_NAME") {
			t.Fatalf("configuration set %q error = %v", name, err)
		}
	}
}

func TestLoadEmailNeedsNoFeedbackQueueAndAcceptsNoFeedbackSetting(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SES_FROM_EMAIL": "alerts@example.com",
		// The email-only loader never reads these, so neither a shared queue nor a bad
		// number can fail it.
		"SQS_SES_EVENTS_URL": "https://sqs/jobs", "NOTIFICATION_FEEDBACK_CONCURRENCY": "many",
	}
	loaded, err := LoadEmail([]string{"--send-concurrency=3"}, lookup(base))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SQSFeedbackURL != "" || loaded.FeedbackConcurrency != 0 ||
		loaded.SQSQueueURL != "https://sqs/jobs" || loaded.SendConcurrency != 3 {
		t.Fatalf("email config = %#v", loaded)
	}
	if _, err := LoadEmail([]string{"--feedback-concurrency=2"}, lookup(base)); !workerproc.IsConfiguration(err) {
		t.Fatalf("email-only loader accepted a feedback flag: %v", err)
	}
}

func TestLoadReportsBadFlagsAsConfigurationErrors(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "local", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SES_FROM_EMAIL": "alerts@example.com",
	}
	for _, args := range [][]string{{"--unknown"}, {"--send-concurrency=x"}, {"--receive-wait"}} {
		if _, err := LoadEmail(args, lookup(base)); !workerproc.IsConfiguration(err) {
			t.Fatalf("LoadEmail(%v) error = %v, want a configuration error", args, err)
		}
	}
	base["NOTIFICATION_SEND_BURST"] = "x"
	if _, err := LoadEmail(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "NOTIFICATION_SEND_BURST") {
		t.Fatalf("bad burst error = %v", err)
	}
}

func TestLoadTakesAnExplicitProductionRateFromTheFlag(t *testing.T) {
	loaded, err := LoadEmail([]string{"--max-send-rate=2"}, lookup(map[string]string{
		"APP_ENV": "prod", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_NOTIFICATION_JOBS_URL": "https://sqs/jobs", "SES_FROM_EMAIL": "alerts@example.com",
	}))
	if err != nil || loaded.MaxSendRate != 2 {
		t.Fatalf("config=%#v err=%v", loaded, err)
	}
}

func TestHostedEnvironmentAndQueueTiming(t *testing.T) {
	for appEnv, want := range map[string]bool{"local": false, "test": false, "staging": true, "prod": true} {
		if hosted, err := HostedEnvironment(appEnv); err != nil || hosted != want {
			t.Fatalf("HostedEnvironment(%q) = %t, %v", appEnv, hosted, err)
		}
	}
	if _, err := HostedEnvironment("dev"); err == nil {
		t.Fatal("unknown APP_ENV was accepted")
	}
	for _, test := range []struct {
		hosted            bool
		wait, visibility  time.Duration
		wantErrorFragment string
	}{
		{hosted: true, wait: 0, visibility: InvalidVisibility, wantErrorFragment: "zero receive wait"},
		{hosted: true, wait: ReceiveWait, visibility: time.Second, wantErrorFragment: "short invalid visibility"},
		{wait: 21 * time.Second, visibility: InvalidVisibility, wantErrorFragment: "receive wait"},
		{wait: ReceiveWait, visibility: 13 * time.Hour, wantErrorFragment: "invalid visibility"},
	} {
		err := ValidateQueueTiming(test.hosted, test.wait, test.visibility)
		if err == nil || !strings.Contains(err.Error(), test.wantErrorFragment) {
			t.Fatalf("ValidateQueueTiming(%+v) = %v", test, err)
		}
	}
	if err := ValidateQueueTiming(true, ReceiveWait, InvalidVisibility); err != nil {
		t.Fatal(err)
	}
}

func lookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
