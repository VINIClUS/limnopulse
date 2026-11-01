package workerproc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func credentialDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, value := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o400); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCredentialsWithoutDirectoryKeepTheDefaultChain(t *testing.T) {
	creds, err := CredentialsFromEnv(env(nil))
	if err != nil || creds.Available() {
		t.Fatalf("credentials available without CREDENTIALS_DIRECTORY: %v", err)
	}
	if value, ok, err := creds.Lookup(CredentialInfluxDBToken); value != "" || ok || err != nil {
		t.Fatalf("got %q, %v, %v", value, ok, err)
	}
	provider, ok, err := creds.AWSProvider()
	if provider != nil || ok || err != nil {
		t.Fatalf("got %v, %v, %v", provider, ok, err)
	}
	options, err := creds.AWSLoadOptions()
	if len(options) != 0 || err != nil {
		t.Fatalf("got %d options, %v", len(options), err)
	}
}

func TestCredentialsReadLoadCredentialFiles(t *testing.T) {
	dir := credentialDir(t, map[string]string{
		CredentialAWSAccessKeyID:     "AKIAEXAMPLE\n",
		CredentialAWSSecretAccessKey: "secret-example\n",
		CredentialInfluxDBToken:      "token-example",
	})
	creds, err := CredentialsFromEnv(env(map[string]string{"CREDENTIALS_DIRECTORY": dir}))
	if err != nil {
		t.Fatal(err)
	}
	token, ok, err := creds.Lookup(CredentialInfluxDBToken)
	if token != "token-example" || !ok || err != nil {
		t.Fatalf("got %q, %v, %v", token, ok, err)
	}
	provider, ok, err := creds.AWSProvider()
	if !ok || err != nil {
		t.Fatalf("got %v, %v", ok, err)
	}
	value, err := provider.Retrieve(context.Background())
	if err != nil || value.AccessKeyID != "AKIAEXAMPLE" || value.SecretAccessKey != "secret-example" {
		t.Fatalf("got %+v, %v", value, err)
	}
	options, err := creds.AWSLoadOptions()
	if len(options) != 1 || err != nil {
		t.Fatalf("got %d options, %v", len(options), err)
	}
}

func TestMissingOrEmptyCredentialIsAConfigurationError(t *testing.T) {
	cases := map[string]map[string]string{
		"missing secret": {CredentialAWSAccessKeyID: "AKIAEXAMPLE"},
		"empty key id":   {CredentialAWSAccessKeyID: " \n", CredentialAWSSecretAccessKey: "secret"},
	}
	for name, files := range cases {
		dir := credentialDir(t, files)
		creds, err := CredentialsFromEnv(env(map[string]string{"CREDENTIALS_DIRECTORY": dir}))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := creds.AWSProvider(); !IsConfiguration(err) || ExitCode(err) != ExitConfiguration {
			t.Errorf("%s: want exit 78, got %v", name, err)
		}
	}
}

func TestCredentialNamesAndDirectoryAreValidated(t *testing.T) {
	if _, err := CredentialsFromEnv(env(map[string]string{"CREDENTIALS_DIRECTORY": "relative"})); !IsConfiguration(err) {
		t.Fatalf("relative directory accepted: %v", err)
	}
	creds, err := CredentialsFromEnv(env(map[string]string{"CREDENTIALS_DIRECTORY": t.TempDir()}))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../shadow", "a/b", "", "Upper"} {
		if _, _, err := creds.Lookup(name); !IsConfiguration(err) {
			t.Errorf("credential name %q accepted: %v", name, err)
		}
	}
}
