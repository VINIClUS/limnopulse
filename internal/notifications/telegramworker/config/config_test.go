package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadLocalTelegramWorkerDefaults(t *testing.T) {
	loaded, err := Load(nil, lookup(map[string]string{
		"APP_ENV": "local", "TELEGRAM_DELIVERY_ENABLED": "true",
		"AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb:8000",
		"SQS_TELEGRAM_JOBS_URL": "http://sqs:9324/queue/telegram-jobs",
		"SQS_ENDPOINT_URL":      "http://sqs:9324", "REDIS_ADDR": "redis:6379",
		"TELEGRAM_BOT_TOKEN": "local-token", "TELEGRAM_BOT_API_BASE_URL": "http://telegram:8080",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Enabled || loaded.SendConcurrency != 4 || loaded.GlobalRate != 25 ||
		loaded.GlobalBurst != 25 || loaded.DestinationRate != 1 || loaded.DestinationBurst != 1 ||
		loaded.ReceiveWait != 20*time.Second || loaded.ReceiveBatch != 10 ||
		loaded.VisibilityTimeout != time.Minute || loaded.ProcessingLease != time.Minute ||
		loaded.ProviderTimeout != 10*time.Second || loaded.BotToken != "local-token" ||
		loaded.BotAPIBaseURL != "http://telegram:8080" {
		t.Fatalf("loaded = %#v", loaded)
	}
}

func TestLoadHostedRequiresSecretAndExplicitFlag(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "prod", "AWS_REGION": "sa-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"SQS_TELEGRAM_JOBS_URL": "https://sqs/telegram", "REDIS_ADDR": "redis.internal:6379",
		"TELEGRAM_BOT_TOKEN_SECRET_ARN": "arn:aws:secretsmanager:sa-east-1:123:secret:telegram",
		"TELEGRAM_GLOBAL_RATE":          "30", "TELEGRAM_GLOBAL_BURST": "30",
		"TELEGRAM_DESTINATION_RATE": "1", "TELEGRAM_DESTINATION_BURST": "1",
	}
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "TELEGRAM_DELIVERY_ENABLED") {
		t.Fatalf("missing explicit flag error = %v", err)
	}
	base["TELEGRAM_DELIVERY_ENABLED"] = "true"
	loaded, err := Load([]string{"--send-concurrency=7"}, lookup(base))
	if err != nil || loaded.SendConcurrency != 7 || loaded.BotTokenSecretARN == "" || loaded.BotToken != "" {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	base["TELEGRAM_BOT_TOKEN"] = "forbidden"
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "TELEGRAM_BOT_TOKEN") {
		t.Fatalf("hosted direct token error = %v", err)
	}
	delete(base, "TELEGRAM_BOT_TOKEN")
	base["TELEGRAM_BOT_API_BASE_URL"] = "https://example.invalid"
	if _, err := Load(nil, lookup(base)); err == nil || !strings.Contains(err.Error(), "BOT_API_BASE_URL") {
		t.Fatalf("hosted base URL override error = %v", err)
	}
}

func TestLoadRejectsInvalidTelegramWorkerConfiguration(t *testing.T) {
	base := map[string]string{
		"APP_ENV": "test", "TELEGRAM_DELIVERY_ENABLED": "true", "AWS_REGION": "us-east-1",
		"DYNAMODB_DOMAIN_TABLE": "domain", "SQS_TELEGRAM_JOBS_URL": "queue",
		"REDIS_ADDR": "redis:6379", "TELEGRAM_BOT_TOKEN": "token",
	}
	for _, mutation := range []func(map[string]string){
		func(values map[string]string) { values["TELEGRAM_GLOBAL_RATE"] = "0" },
		func(values map[string]string) { values["TELEGRAM_DESTINATION_RATE"] = "NaN" },
		func(values map[string]string) { values["TELEGRAM_GLOBAL_BURST"] = "0" },
		func(values map[string]string) { values["REDIS_DB"] = "-1" },
		func(values map[string]string) { values["TELEGRAM_BOT_TOKEN_SECRET_ARN"] = "also-set" },
	} {
		values := clone(base)
		mutation(values)
		if _, err := Load(nil, lookup(values)); err == nil {
			t.Fatalf("invalid config accepted: %#v", values)
		}
	}
	if _, err := Load([]string{"extra"}, lookup(base)); err == nil {
		t.Fatal("positional argument accepted")
	}
}

func lookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func clone(values map[string]string) map[string]string {
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func TestLoadNamesTheSettingThatFailed(t *testing.T) {
	local := map[string]string{
		"APP_ENV": "test", "TELEGRAM_DELIVERY_ENABLED": "true", "AWS_REGION": "us-east-1",
		"DYNAMODB_DOMAIN_TABLE": "domain", "SQS_TELEGRAM_JOBS_URL": "queue",
		"REDIS_ADDR": "redis:6379", "TELEGRAM_BOT_TOKEN": "token",
	}
	hosted := map[string]string{
		"APP_ENV": "prod", "TELEGRAM_DELIVERY_ENABLED": "true", "AWS_REGION": "sa-east-1",
		"DYNAMODB_DOMAIN_TABLE": "domain", "SQS_TELEGRAM_JOBS_URL": "https://sqs/telegram",
		"REDIS_ADDR":                    "redis.internal:6379",
		"TELEGRAM_BOT_TOKEN_SECRET_ARN": "arn:aws:secretsmanager:sa-east-1:123:secret:telegram",
	}
	for _, test := range []struct {
		base   map[string]string
		key    string
		value  string
		args   []string
		prefix string
	}{
		{base: local, key: "TELEGRAM_SEND_CONCURRENCY", value: "0", prefix: "--send-concurrency (TELEGRAM_SEND_CONCURRENCY) must be positive"},
		{base: local, key: "TELEGRAM_GLOBAL_RATE", value: "+Inf", prefix: "TELEGRAM_GLOBAL_RATE must be a positive number"},
		{base: local, key: "TELEGRAM_DESTINATION_RATE", value: "NaN", prefix: "TELEGRAM_DESTINATION_RATE must be a positive number"},
		{base: local, key: "TELEGRAM_GLOBAL_BURST", value: "0", prefix: "TELEGRAM_GLOBAL_BURST must be positive"},
		{base: local, key: "TELEGRAM_DESTINATION_BURST", value: "0", prefix: "TELEGRAM_DESTINATION_BURST must be positive"},
		{base: local, key: "REDIS_DB", value: "-1", prefix: "REDIS_DB must not be negative"},
		{base: local, args: []string{"--receive-wait=500ms"}, prefix: "--receive-wait must be a whole number"},
		{base: local, args: []string{"--invalid-visibility=0s"}, prefix: "--invalid-visibility must be a whole number"},
		{base: hosted, args: []string{"--receive-wait=0s"}, prefix: "--receive-wait may be 0s only in local or test"},
		{base: hosted, args: []string{"--invalid-visibility=1s"}, prefix: "--invalid-visibility may be overridden"},
	} {
		values := clone(test.base)
		if test.key != "" {
			values[test.key] = test.value
		}
		if _, err := Load(test.args, lookup(values)); err == nil || !strings.HasPrefix(err.Error(), test.prefix) {
			t.Fatalf("Load(%v, %s=%s) error = %v, want %q", test.args, test.key, test.value, err, test.prefix)
		}
	}
	if _, err := Load(nil, lookup(hosted)); err != nil {
		t.Fatalf("hosted defaults rejected: %v", err)
	}
}
