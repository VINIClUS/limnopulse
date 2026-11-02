package compose

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	feedbackconfig "github.com/VINIClUS/limnopulse/internal/notifications/feedback/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type refusingHTTPClient struct{ calls atomic.Int32 }

func (client *refusingHTTPClient) Do(*http.Request) (*http.Response, error) {
	client.calls.Add(1)
	return nil, errors.New("connection refused")
}

func feedbackConfig(t *testing.T) feedbackconfig.RunConfig {
	t.Helper()
	values := map[string]string{
		"APP_ENV": "test", "AWS_REGION": "us-east-1", "DYNAMODB_DOMAIN_TABLE": "domain",
		"DYNAMODB_ENDPOINT_URL": "http://dynamodb.test", "SQS_ENDPOINT_URL": "http://sqs.test",
		"SQS_SES_EVENTS_URL": "http://sqs.test/queue/ses-events",
	}
	config, err := feedbackconfig.Load(nil, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func TestRunConsumesTheSESEventsQueue(t *testing.T) {
	client := &refusingHTTPClient{}
	var endpoints []string
	summary, err := Run(context.Background(), feedbackConfig(t), func(_ context.Context, region, endpoint string) (aws.Config, error) {
		endpoints = append(endpoints, endpoint)
		return aws.Config{
			Region: region, BaseEndpoint: aws.String(endpoint), HTTPClient: client,
			Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
			Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 2 || endpoints[0] != "http://dynamodb.test" || endpoints[1] != "http://sqs.test" {
		t.Fatalf("endpoints = %v", endpoints)
	}
	if client.calls.Load() == 0 || summary.ExitCode != 1 || summary.ErrorCategories["queue_receive_failure"] != 1 ||
		summary.FeedbackMetrics.Applied != 0 {
		t.Fatalf("calls=%d summary=%#v", client.calls.Load(), summary)
	}
}

func TestRunReturnsTheLoaderError(t *testing.T) {
	for failAt := 1; failAt <= 2; failAt++ {
		calls := 0
		_, err := Run(context.Background(), feedbackConfig(t), func(context.Context, string, string) (aws.Config, error) {
			calls++
			if calls == failAt {
				return aws.Config{}, errors.New("no region")
			}
			return aws.Config{}, nil
		}, nil)
		if err == nil || calls != failAt {
			t.Fatalf("fail at %d: err=%v calls=%d", failAt, err, calls)
		}
	}
}
