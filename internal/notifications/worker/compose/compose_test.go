package compose

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	workerconfig "github.com/VINIClUS/limnopulse/internal/notifications/worker/config"
	"github.com/VINIClUS/limnopulse/internal/workercmd"
	"github.com/VINIClUS/limnopulse/internal/workerproc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type refusingHTTPClient struct{ calls atomic.Int32 }

func (client *refusingHTTPClient) Do(*http.Request) (*http.Response, error) {
	client.calls.Add(1)
	return nil, errors.New("connection refused")
}

func emailConfig(t *testing.T, mode string) workerconfig.RunConfig {
	t.Helper()
	values := map[string]string{
		"APP_ENV": "test", "AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb.test", "SQS_ENDPOINT_URL": "http://sqs.test",
		"SQS_NOTIFICATION_JOBS_URL": "http://sqs.test/queue/jobs", "SES_FROM_EMAIL": "alerts@example.test",
		"SES_ENDPOINT_URL": "http://ses.test", "NOTIFICATION_EMAIL_SENDER_MODE": mode,
	}
	config, err := workerconfig.LoadEmail(nil, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func refusingLoader(client *refusingHTTPClient, endpoints *[]string) workercmd.AWSLoader {
	return func(_ context.Context, region, endpoint string) (aws.Config, error) {
		*endpoints = append(*endpoints, endpoint)
		return aws.Config{
			Region: region, BaseEndpoint: aws.String(endpoint), HTTPClient: client,
			Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
			Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	}
}

func TestRunConsumesTheJobsQueueWithAFakeSender(t *testing.T) {
	config := emailConfig(t, "success")
	client := &refusingHTTPClient{}
	var endpoints []string
	summary, err := Run(context.Background(), config, refusingLoader(client, &endpoints), nil)
	if err != nil {
		t.Fatal(err)
	}
	// A fake sender needs no SES configuration.
	if len(endpoints) != 2 || endpoints[0] != "http://dynamodb.test" || endpoints[1] != "http://sqs.test" {
		t.Fatalf("endpoints = %v", endpoints)
	}
	if client.calls.Load() == 0 || summary.ExitCode != 1 || summary.ErrorCategories["queue_receive_failure"] != 1 {
		t.Fatalf("calls=%d summary=%#v", client.calls.Load(), summary)
	}
	if summary.Metrics.ConfiguredRate != config.MaxSendRate || summary.TelemetryExportError != "" {
		t.Fatalf("metrics=%#v telemetry=%q", summary.Metrics, summary.TelemetryExportError)
	}
}

func TestRunComposesTheSESSenderAndFlushesTelemetry(t *testing.T) {
	config := emailConfig(t, "aws")
	config.OTLPEndpoint = "http://127.0.0.1:9"
	config.OTLPFlushTimeout = 200 * time.Millisecond
	client := &refusingHTTPClient{}
	var endpoints []string
	summary, err := Run(context.Background(), config, refusingLoader(client, &endpoints), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 3 || endpoints[2] != "http://ses.test" {
		t.Fatalf("endpoints = %v", endpoints)
	}
	if summary.ExitCode != 1 || summary.TelemetryExportError != "export_failed" {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestRunReportsLoaderAndLimiterErrors(t *testing.T) {
	for failAt := 1; failAt <= 3; failAt++ {
		calls := 0
		_, err := Run(context.Background(), emailConfig(t, "aws"), func(context.Context, string, string) (aws.Config, error) {
			calls++
			if calls == failAt {
				return aws.Config{}, errors.New("no region")
			}
			return aws.Config{Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, nil
		}, nil)
		if err == nil || calls != failAt {
			t.Fatalf("fail at %d: err=%v calls=%d", failAt, err, calls)
		}
	}
	config := emailConfig(t, "success")
	config.SendBurst = 0
	var endpoints []string
	if _, err := Run(context.Background(), config, refusingLoader(&refusingHTTPClient{}, &endpoints), nil); !workerproc.IsConfiguration(err) {
		t.Fatalf("limiter error = %v, want a configuration error", err)
	}
}

func TestPrepareSESRetrievesTheCredentialsBeforeAnyCall(t *testing.T) {
	calls := 0
	provider := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		calls++
		return aws.Credentials{}, errors.New("credentials unavailable")
	})
	client := &refusingHTTPClient{}
	if _, err := PrepareSES(context.Background(), aws.Config{Credentials: provider, HTTPClient: client}); err == nil ||
		calls != 1 || client.calls.Load() != 0 {
		t.Fatalf("err=%v credential_calls=%d HTTP_calls=%d", err, calls, client.calls.Load())
	}
	if _, err := PrepareSES(context.Background(), aws.Config{}); err == nil {
		t.Fatal("a missing credentials provider was accepted")
	}
	prepared, err := PrepareSES(context.Background(), aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "secret", ""),
	})
	if err != nil || prepared.Credentials == nil {
		t.Fatalf("prepared=%#v err=%v", prepared, err)
	}
}
