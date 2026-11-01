package workercmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

func env(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func credentialsDirectory(t *testing.T, files map[string]string) workerproc.Credentials {
	t.Helper()
	dir := t.TempDir()
	for name, value := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	creds, err := workerproc.CredentialsFromEnv(env(map[string]string{"CREDENTIALS_DIRECTORY": dir}))
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func TestNewAWSLoaderPrefersTheLoadCredentialKeys(t *testing.T) {
	creds := credentialsDirectory(t, map[string]string{
		workerproc.CredentialAWSAccessKeyID:     "AKIDCREDENTIAL\n",
		workerproc.CredentialAWSSecretAccessKey: "credential-secret\n",
	})
	load, err := NewAWSLoader(creds)
	if err != nil {
		t.Fatal(err)
	}
	// Even an endpoint override keeps the keys systemd supplied.
	config, err := load(context.Background(), "sa-east-1", "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	value, err := config.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value.AccessKeyID != "AKIDCREDENTIAL" || value.SecretAccessKey != "credential-secret" {
		t.Fatalf("credentials came from %q", value.AccessKeyID)
	}
	if config.Region != "sa-east-1" || config.BaseEndpoint == nil || *config.BaseEndpoint != "http://127.0.0.1:9" {
		t.Fatalf("region=%q endpoint=%v", config.Region, config.BaseEndpoint)
	}
}

func TestNewAWSLoaderWithoutCredentialsKeepsTheLocalSources(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDENVIRONMENT")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "environment-secret")
	load, err := NewAWSLoader(workerproc.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	local, err := load(context.Background(), "us-east-1", "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if value, err := local.Credentials.Retrieve(context.Background()); err != nil || value.AccessKeyID != "local" {
		t.Fatalf("emulator credentials = %q, %v", value.AccessKeyID, err)
	}
	hosted, err := load(context.Background(), "us-east-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if hosted.BaseEndpoint != nil {
		t.Fatalf("endpoint = %q", *hosted.BaseEndpoint)
	}
	if value, err := hosted.Credentials.Retrieve(context.Background()); err != nil || value.AccessKeyID != "AKIDENVIRONMENT" {
		t.Fatalf("default chain credentials = %q, %v", value.AccessKeyID, err)
	}
}

func TestAWSLoaderFromEnvRefusesAMissingCredentialsDirectoryUnderSystemd(t *testing.T) {
	_, _, err := AWSLoaderFromEnv(env(map[string]string{workerproc.SupervisorEnv: "systemd"}))
	if !workerproc.IsConfiguration(err) {
		t.Fatalf("error = %v, want a configuration error", err)
	}
	creds := credentialsDirectory(t, map[string]string{workerproc.CredentialAWSAccessKeyID: "AKID"})
	if _, err := NewAWSLoader(creds); !workerproc.IsConfiguration(err) {
		t.Fatalf("a missing secret key gave %v, want a configuration error", err)
	}
	load, local, err := AWSLoaderFromEnv(env(nil))
	if err != nil || load == nil || local.Available() {
		t.Fatalf("local run: load=%v available=%t err=%v", load != nil, local.Available(), err)
	}
	dir := t.TempDir()
	if _, _, err := AWSLoaderFromEnv(env(map[string]string{"CREDENTIALS_DIRECTORY": dir})); !workerproc.IsConfiguration(err) {
		t.Fatalf("a directory without AWS keys gave %v, want a configuration error", err)
	}
}

func TestSecretLookupServesTheCredentialAndNeverTheEnvFile(t *testing.T) {
	plain := env(map[string]string{"REDIS_ADDR": "redis:6379", "REDIS_PASSWORD": "env-file-password"})
	unchanged, err := SecretLookup(plain, workerproc.Credentials{}, workerproc.CredentialRedisPassword, "REDIS_PASSWORD")
	if err != nil {
		t.Fatal(err)
	}
	if value, _ := unchanged("REDIS_PASSWORD"); value != "env-file-password" {
		t.Fatalf("local run lost its env value: %q", value)
	}

	creds := credentialsDirectory(t, map[string]string{workerproc.CredentialRedisPassword: "credential-password\n"})
	if _, err := SecretLookup(plain, creds, workerproc.CredentialRedisPassword, "REDIS_PASSWORD"); !workerproc.IsConfiguration(err) ||
		strings.Contains(err.Error(), "env-file-password") || strings.Contains(err.Error(), "credential-password") {
		t.Fatalf("both sources set: %v", err)
	}
	served, err := SecretLookup(env(map[string]string{"REDIS_ADDR": "redis:6379"}), creds,
		workerproc.CredentialRedisPassword, "REDIS_PASSWORD")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := served("REDIS_PASSWORD"); !ok || value != "credential-password" {
		t.Fatalf("served %q, %t", value, ok)
	}
	if value, ok := served("REDIS_ADDR"); !ok || value != "redis:6379" {
		t.Fatalf("other keys: %q, %t", value, ok)
	}

	missing := credentialsDirectory(t, nil)
	if _, err := SecretLookup(plain, missing, workerproc.CredentialRedisPassword, "REDIS_PASSWORD"); !workerproc.IsConfiguration(err) {
		t.Fatalf("missing credential: %v", err)
	}
}

func TestFailMapsConfigurationTo78AndNeverWritesTheError(t *testing.T) {
	for _, test := range []struct {
		err       error
		wantCode  int
		wantRetry bool
	}{
		{err: workerproc.Configuration(errors.New("queue private-queue is missing")), wantCode: 78},
		{err: errors.New("private-queue unreachable"), wantCode: 1, wantRetry: true},
		{err: nil, wantCode: 1, wantRetry: true},
	} {
		var output bytes.Buffer
		if code := Fail(&output, test.err, "startup"); code != test.wantCode {
			t.Fatalf("Fail(%v) = %d", test.err, code)
		}
		var failure Failure
		if err := json.Unmarshal(output.Bytes(), &failure); err != nil {
			t.Fatal(err)
		}
		if failure.Result != "fatal_failure" || failure.ExitCode != test.wantCode ||
			failure.RetryRecommended != test.wantRetry || failure.ErrorCategories["startup"] != 1 ||
			strings.Contains(output.String(), "private-queue") {
			t.Fatalf("line = %s", output.String())
		}
	}
}
