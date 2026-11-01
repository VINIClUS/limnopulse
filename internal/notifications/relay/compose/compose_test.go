package compose

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	relayconfig "github.com/VINIClUS/limnopulse/internal/notifications/relay/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type refusingHTTPClient struct{ calls atomic.Int32 }

func (client *refusingHTTPClient) Do(*http.Request) (*http.Response, error) {
	client.calls.Add(1)
	return nil, errors.New("connection refused")
}

func relayConfig(t *testing.T) relayconfig.RunConfig {
	t.Helper()
	values := map[string]string{
		"AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb.test", "SQS_ENDPOINT_URL": "http://sqs.test",
		"SQS_NOTIFICATION_JOBS_URL": "http://sqs.test/queue/jobs",
	}
	config, err := relayconfig.Load(nil, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	config.BudgetStartedAt = &started
	return config
}

func TestRunComposesTheStoreAndLanesFromTheirOwnEndpoints(t *testing.T) {
	config := relayConfig(t)
	config.OTLPEndpoint = "http://127.0.0.1:9"
	config.OTLPFlushTimeout = 200 * time.Millisecond
	client := &refusingHTTPClient{}
	var endpoints []string
	summary, err := Run(context.Background(), config, func(_ context.Context, region, endpoint string) (aws.Config, error) {
		endpoints = append(endpoints, endpoint)
		return aws.Config{
			Region: region, BaseEndpoint: aws.String(endpoint), HTTPClient: client,
			Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
			Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 2 || endpoints[0] != "http://dynamodb.test" || endpoints[1] != "http://sqs.test" {
		t.Fatalf("endpoints = %v", endpoints)
	}
	// Discovery reached the composed DynamoDB client, which refused every call.
	if client.calls.Load() == 0 || summary.ExitCode != relay.ExitFatal || summary.ErrorCategories["discovery"] != 1 {
		t.Fatalf("calls=%d summary=%#v", client.calls.Load(), summary)
	}
	if summary.TelemetryExportError != "export_failed" {
		t.Fatalf("telemetry = %q", summary.TelemetryExportError)
	}
}

func TestRunReturnsTheLoaderError(t *testing.T) {
	for failAt := 1; failAt <= 2; failAt++ {
		calls := 0
		_, err := Run(context.Background(), relayConfig(t), func(context.Context, string, string) (aws.Config, error) {
			calls++
			if calls == failAt {
				return aws.Config{}, errors.New("no region")
			}
			return aws.Config{}, nil
		})
		if err == nil || calls != failAt {
			t.Fatalf("fail at %d: err=%v calls=%d", failAt, err, calls)
		}
	}
}

func TestTelemetryFlushContextOutlivesTheRun(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	flushCtx, cancel := TelemetryFlushContext(parent, 2*time.Second)
	defer cancel()
	if flushCtx.Err() != nil {
		t.Fatalf("flush context inherited the cancellation: %v", flushCtx.Err())
	}
	if deadline, ok := flushCtx.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
		t.Fatalf("deadline = %s, %t", deadline, ok)
	}
}
