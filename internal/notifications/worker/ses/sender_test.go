package ses

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssignerv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsses "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/smithy-go"
)

func TestSenderUsesImmutableDeliveryAndExactNoPIITags(t *testing.T) {
	client := &fakeSESClient{output: &awsses.SendEmailOutput{MessageId: aws.String("ses_message_1")}}
	sender := Sender{Client: client, FromEmail: "alerts@example.com", ConfigurationSet: "custom-notifications_1", Timeout: 15 * time.Second}
	request := testSendRequest(t)
	result, err := sender.Send(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProviderMessageID != "ses_message_1" || client.input == nil {
		t.Fatalf("result=%#v input=%#v", result, client.input)
	}
	input := client.input
	if aws.ToString(input.FromEmailAddress) != "alerts@example.com" ||
		aws.ToString(input.ConfigurationSetName) != "custom-notifications_1" ||
		len(input.Destination.ToAddresses) != 1 || input.Destination.ToAddresses[0] != request.Delivery.NormalizedEmail {
		t.Fatalf("SES addressing/config = %#v", input)
	}
	simple := input.Content.Simple
	if simple == nil || aws.ToString(simple.Subject.Data) != request.Delivery.Content.Subject ||
		aws.ToString(simple.Body.Text.Data) != request.Delivery.Content.Text ||
		aws.ToString(simple.Body.Html.Data) != request.Delivery.Content.HTML {
		t.Fatalf("SES immutable content = %#v", simple)
	}
	if aws.ToString(simple.Subject.Charset) != "UTF-8" || aws.ToString(simple.Body.Text.Charset) != "UTF-8" ||
		aws.ToString(simple.Body.Html.Charset) != "UTF-8" {
		t.Fatalf("SES charsets = subject=%q text=%q html=%q", aws.ToString(simple.Subject.Charset),
			aws.ToString(simple.Body.Text.Charset), aws.ToString(simple.Body.Html.Charset))
	}
	tags := map[string]string{}
	for _, tag := range input.EmailTags {
		tags[aws.ToString(tag.Name)] = aws.ToString(tag.Value)
	}
	want := map[string]string{
		"delivery_id": request.Delivery.DeliveryID, "attempt_id": request.AttemptID,
		"event_id": request.Delivery.EventID, "notification_kind": "opening", "channel": "email",
	}
	if len(tags) != len(want) {
		t.Fatalf("tags = %#v", tags)
	}
	for name, value := range want {
		if tags[name] != value {
			t.Errorf("tag %s = %q", name, tags[name])
		}
	}
	serialized := strings.ToLower(strings.TrimSpace(strings.Join(mapValues(tags), " ")))
	for _, forbidden := range []string{"owner@example.com", "subject", "rule_1", "tnt_1"} {
		if strings.Contains(serialized, forbidden) {
			t.Errorf("tags leaked %q: %#v", forbidden, tags)
		}
	}
	if !client.deadlineSet || client.deadlineRemaining > 15*time.Second || client.deadlineRemaining < 14*time.Second {
		t.Fatalf("provider deadline set=%t remaining=%s", client.deadlineSet, client.deadlineRemaining)
	}
}

func TestSenderCredentialPreflightFailureIsFatalAndNeverCallsSES(t *testing.T) {
	client := &fakeSESClient{output: &awsses.SendEmailOutput{MessageId: aws.String("must_not_send")}}
	provider := &failingCredentialsProvider{err: errors.New("credential refresh unavailable")}
	sender := Sender{
		Client: client, Credentials: provider, FromEmail: "alerts@example.com",
		ConfigurationSet: "limnopulse-notifications", Timeout: 15 * time.Second,
	}
	_, err := sender.Send(context.Background(), testSendRequest(t))
	var sendErr *worker.SendError
	if !errors.As(err, &sendErr) || sendErr.Category != worker.ErrorFatalCredentials ||
		provider.calls.Load() != 1 || client.calls.Load() != 0 {
		t.Fatalf("err=%#v credential_calls=%d SES_calls=%d", err, provider.calls.Load(), client.calls.Load())
	}
}

func TestWrappedCredentialProviderFailureRemainsTypedThroughOperationErrors(t *testing.T) {
	provider := WrapCredentials(&failingCredentialsProvider{err: errors.New("refresh failed")})
	_, err := provider.Retrieve(context.Background())
	classified := ClassifyError(fmt.Errorf("SES operation: %w", err))
	var sendErr *worker.SendError
	if !errors.As(classified, &sendErr) || sendErr.Category != worker.ErrorFatalCredentials {
		t.Fatalf("classified = %#v", classified)
	}
}

func TestClassifyErrorConservativelyMapsSESAndTransportFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want worker.SendErrorCategory
	}{
		{"recipient marker", RecipientError{Err: errors.New("bad recipient")}, worker.ErrorPermanentRecipient},
		{"generic rejection is not recipient permanent", &smithy.GenericAPIError{Code: "MessageRejected", Message: "rejected"}, worker.ErrorRetryableUnknown},
		{"throttling", &smithy.GenericAPIError{Code: "TooManyRequestsException", Message: "slow"}, worker.ErrorRetryableThrottling},
		{"SES daily quota throttling", &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Daily message quota exceeded"}, worker.ErrorRetryableQuota},
		{"SES maximum rate throttling", &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Maximum sending rate exceeded"}, worker.ErrorRetryableThrottling},
		{"SES unknown throttle detail", &smithy.GenericAPIError{Code: "ThrottlingException", Message: "unrecognized provider detail"}, worker.ErrorRetryableThrottling},
		{"quota", &smithy.GenericAPIError{Code: "LimitExceededException", Message: "quota"}, worker.ErrorRetryableQuota},
		{"service", &smithy.GenericAPIError{Code: "ServiceUnavailableException", Message: "down"}, worker.ErrorRetryableService},
		{"account", &smithy.GenericAPIError{Code: "AccountSuspendedException", Message: "suspended"}, worker.ErrorFatalAccountSuspended},
		{"identity", &smithy.GenericAPIError{Code: "MailFromDomainNotVerifiedException", Message: "identity"}, worker.ErrorFatalFromIdentity},
		{"config set", &smithy.GenericAPIError{Code: "NotFoundException", Message: "config"}, worker.ErrorFatalConfigurationSet},
		{"credentials", &smithy.GenericAPIError{Code: "InvalidClientTokenId", Message: "credentials"}, worker.ErrorFatalCredentials},
		{"wrapped signing credentials", fmt.Errorf("operation: %w", &awssignerv4.SigningError{Err: errors.New("provider failed")}), worker.ErrorFatalCredentials},
		{"timeout", context.DeadlineExceeded, worker.ErrorAmbiguousTimeout},
		{"reset", syscall.ECONNRESET, worker.ErrorAmbiguousConnectionReset},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classified := ClassifyError(test.err)
			var sendErr *worker.SendError
			if !errors.As(classified, &sendErr) || sendErr.Category != test.want {
				t.Fatalf("ClassifyError(%T) = %#v, want %q", test.err, classified, test.want)
			}
			if strings.Contains(classified.Error(), "Daily message quota exceeded") ||
				strings.Contains(classified.Error(), "Maximum sending rate exceeded") {
				t.Fatalf("classified error leaked provider message: %v", classified)
			}
		})
	}
}

func TestNewClientDisablesSDKRetries(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		response.WriteHeader(http.StatusInternalServerError)
		_, _ = response.Write([]byte(`{"message":"unavailable"}`))
	}))
	defer server.Close()
	client := NewClient(aws.Config{
		Region: "us-east-1", HTTPClient: server.Client(),
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}, server.URL)
	_, _ = (Sender{
		Client: client, FromEmail: "alerts@example.com",
		ConfigurationSet: "limnopulse-notifications", Timeout: time.Second,
	}).Send(context.Background(), testSendRequest(t))
	if got := requests.Load(); got != 1 {
		t.Fatalf("SDK HTTP requests = %d, want 1 (MaxAttempts=1)", got)
	}
}

func TestFakeSenderModesAreDeterministic(t *testing.T) {
	request := testSendRequest(t)
	tests := []struct {
		mode         FakeMode
		wantID       string
		wantCategory worker.SendErrorCategory
	}{
		{FakeSuccess, "fake_message_att_1", ""},
		{FakeRetryable, "", worker.ErrorRetryableService},
		{FakePermanent, "", worker.ErrorPermanentRecipient},
		{FakeAmbiguousTimeout, "", worker.ErrorAmbiguousTimeout},
		{FakeConnectionReset, "", worker.ErrorAmbiguousConnectionReset},
	}
	for _, test := range tests {
		result, err := (FakeSender{Mode: test.mode}).Send(context.Background(), request)
		if result.ProviderMessageID != test.wantID {
			t.Fatalf("mode %s result = %#v", test.mode, result)
		}
		if test.wantCategory == "" {
			if err != nil {
				t.Fatalf("mode %s err = %v", test.mode, err)
			}
			continue
		}
		var sendErr *worker.SendError
		if !errors.As(err, &sendErr) || sendErr.Category != test.wantCategory {
			t.Fatalf("mode %s err = %#v", test.mode, err)
		}
	}
}

func TestFakeSenderCanUseConfiguredProviderMessageID(t *testing.T) {
	request := testSendRequest(t)
	result, err := (FakeSender{Mode: FakeSuccess, MessageID: "provider_message_replay_1"}).Send(
		context.Background(), request,
	)
	if err != nil || result.ProviderMessageID != "provider_message_replay_1" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

type fakeSESClient struct {
	input             *awsses.SendEmailInput
	output            *awsses.SendEmailOutput
	err               error
	deadlineSet       bool
	deadlineRemaining time.Duration
	calls             atomic.Int32
}

func (client *fakeSESClient) SendEmail(ctx context.Context, input *awsses.SendEmailInput, _ ...func(*awsses.Options)) (*awsses.SendEmailOutput, error) {
	client.calls.Add(1)
	client.input = input
	deadline, ok := ctx.Deadline()
	client.deadlineSet = ok
	client.deadlineRemaining = time.Until(deadline)
	return client.output, client.err
}

type failingCredentialsProvider struct {
	err   error
	calls atomic.Int32
}

func (provider *failingCredentialsProvider) Retrieve(context.Context) (aws.Credentials, error) {
	provider.calls.Add(1)
	return aws.Credentials{}, provider.err
}

func testSendRequest(t *testing.T) worker.SendRequest {
	t.Helper()
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 15, 0, 0, 0, time.UTC)
	content, err := renderer.Render(notifications.TemplateAlertOpeningV1, notifications.LocalePTBR,
		notifications.EmailTemplateData{RuleName: "Rule", Severity: "critical", TenantID: "tnt_1",
			PondID: "pond_1", DeviceID: "dev_1", Metric: "temperature", Unit: "°C", Operator: ">",
			Threshold: 30, EvaluationWindow: time.Minute, WindowStart: now.Add(-time.Minute), WindowEnd: now,
			EvaluatedAt: now, EventID: "alert_1"})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, _ := notifications.NewDeliveryID("alert_1", notifications.NotificationKindOpening, notifications.ChannelEmail, "user_1")
	delivery, err := notifications.NewPendingDelivery(notifications.DeliveryParams{
		TenantID: "tnt_1", OutboxID: "outbox_1", DeliveryID: deliveryID, EventID: "alert_1", RuleID: "rule_1",
		Kind: notifications.NotificationKindOpening, Channel: notifications.ChannelEmail,
		RecipientID: "user_1", NormalizedEmail: "owner@example.com",
		MembershipSnapshot: notifications.MembershipSnapshot{Role: "owner", Status: "active", Version: 1},
		Content:            content, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := delivery.Snapshot()
	snapshot.State = notifications.DeliveryStateProcessing
	return worker.SendRequest{Delivery: snapshot, AttemptID: "att_1", AttemptNumber: 1}
}

func mapValues(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}
