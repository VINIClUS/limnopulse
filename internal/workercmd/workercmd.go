// Package workercmd is the plumbing every ADR-020 Go role entrypoint shares on top of
// workerproc: AWS configuration that takes the LoadCredential= keys, secrets served from a
// credential instead of the env file, and the one JSON line a run ends with.
package workercmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

// AWSLoader loads the AWS configuration of one service. A non-empty endpoint overrides the
// service endpoint (the local emulators) for every client built from the configuration.
type AWSLoader func(ctx context.Context, region, endpoint string) (aws.Config, error)

// AWSLoaderFromEnv reads CREDENTIALS_DIRECTORY with workerproc.CredentialsFromEnv and returns
// the loader for its AWS keys. Under systemd, a missing directory or key is a configuration
// error.
func AWSLoaderFromEnv(lookup workerproc.LookupEnv) (AWSLoader, workerproc.Credentials, error) {
	creds, err := workerproc.CredentialsFromEnv(lookup)
	if err != nil {
		return nil, workerproc.Credentials{}, err
	}
	load, err := NewAWSLoader(creds)
	if err != nil {
		return nil, workerproc.Credentials{}, err
	}
	return load, creds, nil
}

// NewAWSLoader uses the aws-access-key-id and aws-secret-access-key credentials when systemd
// supplied them. Without them, an endpoint override gets fixed local keys and every other
// service the AWS default chain, as local runs always have.
func NewAWSLoader(creds workerproc.Credentials) (AWSLoader, error) {
	supplied, err := creds.AWSLoadOptions()
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, region, endpoint string) (aws.Config, error) {
		options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
		if endpoint != "" {
			options = append(options, awsconfig.WithBaseEndpoint(endpoint))
		}
		switch {
		case len(supplied) > 0:
			options = append(options, supplied...)
		case endpoint != "":
			options = append(options, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider("local", "local", ""),
			))
		}
		return awsconfig.LoadDefaultConfig(ctx, options...)
	}, nil
}

// SecretLookup serves the env key from the named credential when systemd supplied a
// credentials directory, so the role never reads the env file's copy of a secret; an env
// file that still sets the key is a configuration error. Without a directory (local runs)
// it returns lookup unchanged.
func SecretLookup(
	lookup func(string) (string, bool),
	creds workerproc.Credentials,
	credential, key string,
) (func(string) (string, bool), error) {
	value, ok, err := creds.Lookup(credential)
	if err != nil {
		return nil, err
	}
	if !ok {
		return lookup, nil
	}
	if _, set := lookup(key); set {
		return nil, workerproc.Configuration(
			fmt.Errorf("%s must not be set when the %s credential is loaded", key, credential),
		)
	}
	return func(name string) (string, bool) {
		if name == key {
			return value, true
		}
		return lookup(name)
	}, nil
}

// Failure is the line of a run that stopped before it produced its own summary.
type Failure struct {
	Result           string         `json:"result"`
	ExitCode         int            `json:"exit_code"`
	ScopeCompleted   bool           `json:"scope_completed"`
	RetryRecommended bool           `json:"retry_recommended"`
	ErrorCategories  map[string]int `json:"error_categories"`
}

// Fail writes the failure line for err under category and returns its exit code: 78 for a
// configuration error, which no restart can fix, and 1 otherwise. The error text is never
// written; it can name a queue, a table or a secret.
func Fail(output io.Writer, err error, category string) int {
	code := workerproc.ExitCode(err)
	if code == workerproc.ExitSuccess {
		code = workerproc.ExitFailure
	}
	WriteJSON(output, Failure{
		Result: "fatal_failure", ExitCode: code,
		RetryRecommended: code != workerproc.ExitConfiguration,
		ErrorCategories:  map[string]int{category: 1},
	})
	return code
}

// WriteJSON writes value as one JSON line.
func WriteJSON(output io.Writer, value any) {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}
