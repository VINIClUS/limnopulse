package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	telegramworkerconfig "github.com/VINIClUS/limnopulse/internal/notifications/telegramworker/config"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

func environment(extra map[string]string) func(string) (string, bool) {
	values := map[string]string{
		"APP_ENV": "test", "TELEGRAM_DELIVERY_ENABLED": "true", "AWS_REGION": "us-east-1",
		"DYNAMODB_DOMAIN_TABLE": "private-domain", "SQS_TELEGRAM_JOBS_URL": "private-telegram-queue",
		"REDIS_ADDR": "private-redis:6379", "TELEGRAM_BOT_TOKEN": "private-bot-token",
	}
	for key, value := range extra {
		values[key] = value
	}
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func supervisor(signals chan os.Signal) workerproc.Continuous {
	return workerproc.Continuous{
		StallAfter: time.Second, Lookup: func(string) (string, bool) { return "", false }, Signals: signals,
	}
}

func credentialsDirectory(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, value := range map[string]string{
		"aws-access-key-id": "AKIDTELEGRAM", "aws-secret-access-key": "telegram-secret",
		"redis-password": "credential-redis-password\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// run starts the worker with the given environment and returns the configuration its consumer
// received, or false when it never started.
func run(t *testing.T, lookup func(string) (string, bool)) (telegramworkerconfig.RunConfig, bool, int, string) {
	t.Helper()
	signals := make(chan os.Signal, 1)
	var captured telegramworkerconfig.RunConfig
	started := false
	var output bytes.Buffer
	code := runMain(context.Background(), []string{"--send-concurrency=2"}, dependencies{
		Output: &output, LookupEnv: lookup, Supervisor: supervisor(signals),
		Run: func(ctx context.Context, config telegramworkerconfig.RunConfig, _ workercmd.AWSLoader, _ worker.Loop) (worker.RunSummary, error) {
			captured, started = config, true
			signals <- syscall.SIGTERM
			<-ctx.Done()
			return worker.RunSummary{Result: "success", Graceful: true}, nil
		},
	})
	return captured, started, code, output.String()
}

func TestTelegramWorkerReadsTheRedisPasswordFromItsCredential(t *testing.T) {
	config, started, code, output := run(t, environment(map[string]string{
		workerproc.SupervisorEnv: "systemd", "CREDENTIALS_DIRECTORY": credentialsDirectory(t),
	}))
	if !started || code != 0 || config.RedisPassword != "credential-redis-password" || config.SendConcurrency != 2 {
		t.Fatalf("started=%t exit=%d config=%#v output=%s", started, code, config, output)
	}
	for _, secret := range []string{"credential-redis-password", "private-bot-token", "private-redis"} {
		if strings.Contains(output, secret) {
			t.Fatalf("summary leaked %q: %s", secret, output)
		}
	}
}

func TestTelegramWorkerRefusesAnEnvFilePasswordNextToTheCredential(t *testing.T) {
	_, started, code, output := run(t, environment(map[string]string{
		"CREDENTIALS_DIRECTORY": credentialsDirectory(t), "REDIS_PASSWORD": "env-file-password",
	}))
	if started || code != workerproc.ExitConfiguration || strings.Contains(output, "password\"") {
		t.Fatalf("started=%t exit=%d output=%s", started, code, output)
	}
}

func TestTelegramWorkerKeepsTheEnvPasswordForLocalRuns(t *testing.T) {
	config, started, code, _ := run(t, environment(map[string]string{"REDIS_PASSWORD": "local-password"}))
	if !started || code != 0 || config.RedisPassword != "local-password" {
		t.Fatalf("started=%t exit=%d password set=%t", started, code, config.RedisPassword != "")
	}
}

func TestTelegramWorkerExits78OnConfigurationErrors(t *testing.T) {
	for name, lookup := range map[string]func(string) (string, bool){
		"delivery disabled": environment(map[string]string{"TELEGRAM_DELIVERY_ENABLED": "false"}),
		"no credentials":    environment(map[string]string{workerproc.SupervisorEnv: "systemd"}),
		"missing password credential": environment(map[string]string{
			"CREDENTIALS_DIRECTORY": func() string {
				dir := credentialsDirectory(t)
				if err := os.Remove(filepath.Join(dir, "redis-password")); err != nil {
					t.Fatal(err)
				}
				return dir
			}(),
		}),
	} {
		if _, started, code, output := run(t, lookup); started || code != workerproc.ExitConfiguration {
			t.Fatalf("%s: started=%t exit=%d output=%s", name, started, code, output)
		}
	}
}
