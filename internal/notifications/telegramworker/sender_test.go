package telegramworker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (failingReadCloser) Close() error             { return nil }

func TestSenderPostsPlainTextWithoutPreviewOrPIIInErrors(t *testing.T) {
	var body string
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://api.telegram.org/botsecret-token/sendMessage" {
			t.Fatalf("URL = %q", request.URL.String())
		}
		encoded, _ := io.ReadAll(request.Body)
		body = string(encoded)
		return &http.Response{
			StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
				`{"ok":true,"result":{"message_id":42}}`,
			)),
		}, nil
	})}
	result, err := (Sender{Client: client, BotToken: "secret-token", Timeout: time.Second}).Send(
		context.Background(), telegramSendRequest(t),
	)
	if err != nil || result.ProviderMessageID != "42" {
		t.Fatalf("Send() = %#v, %v", result, err)
	}
	for _, want := range []string{`"chat_id":123`, `"text":"Alerta simples"`, `"link_preview_options":{"is_disabled":true}`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q: %s", want, body)
		}
	}
	for _, forbidden := range []string{"parse_mode", "disable_web_page_preview"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("body contains %q: %s", forbidden, body)
		}
	}
}

func TestSenderSupportsLocalBotAPIBaseURL(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "http://telegram:8080/botsecret-token/sendMessage" {
			t.Fatalf("URL = %q", request.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":42}}`)),
		}, nil
	})}
	_, err := (Sender{
		Client: client, BotToken: "secret-token", Timeout: time.Second,
		BaseURL: "http://telegram:8080",
	}).Send(context.Background(), telegramSendRequest(t))
	if err != nil {
		t.Fatal(err)
	}
}

func TestSenderClassifiesTelegramResponseMatrix(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		transport  error
		category   worker.SendErrorCategory
		retryAfter time.Duration
		noRetry    bool
	}{
		{name: "rate limit", status: 429, body: `{"ok":false,"error_code":429,"parameters":{"retry_after":7}}`, category: worker.ErrorTelegramRateLimited, retryAfter: 7 * time.Second},
		{name: "blocked", status: 403, body: `{"ok":false,"error_code":403,"description":"Forbidden"}`, category: worker.ErrorTelegramDestinationUnavailable},
		{name: "credential", status: 401, body: `{"ok":false,"error_code":401}`, category: worker.ErrorTelegramCredentials},
		{name: "bad request is not a destination verdict", status: 400, body: `{"ok":false,"error_code":400}`, category: worker.ErrorFatalConfigurationSet},
		{name: "not found is not a destination verdict", status: 404, body: `{"ok":false,"error_code":404}`, category: worker.ErrorFatalConfigurationSet},
		{name: "service", status: 500, body: `{"ok":false,"error_code":500}`, category: worker.ErrorRetryableService},
		{name: "service with non-JSON body", status: 503, body: `<html>unavailable</html>`, category: worker.ErrorRetryableService},
		{name: "lost response", transport: context.DeadlineExceeded, category: worker.ErrorTelegramAmbiguous, noRetry: true},
		{name: "malformed success", status: 200, body: `{"ok":true,"result":{}}`, category: worker.ErrorTelegramAmbiguous, noRetry: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				if test.transport != nil {
					return nil, test.transport
				}
				return &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body))}, nil
			})}
			_, err := (Sender{Client: client, BotToken: "secret-token", Timeout: time.Second}).Send(
				context.Background(), telegramSendRequest(t),
			)
			var sendErr *worker.SendError
			if !errors.As(err, &sendErr) || sendErr.Category != test.category ||
				sendErr.RetryAfter != test.retryAfter || sendErr.NoAutomaticRetry != test.noRetry {
				t.Fatalf("error = %#v, want category=%s retry=%s noRetry=%t", err, test.category, test.retryAfter, test.noRetry)
			}
		})
	}
}

func TestSenderClassifiesDefinitiveHTTPStatusBeforeDependingOnResponseBody(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     io.ReadCloser
		category worker.SendErrorCategory
	}{
		{name: "oversized 503", status: 503, body: io.NopCloser(strings.NewReader(strings.Repeat("x", maxResponseBytes+1))), category: worker.ErrorRetryableService},
		{name: "unreadable 502", status: 502, body: failingReadCloser{}, category: worker.ErrorRetryableService},
		{name: "unreadable 403", status: 403, body: failingReadCloser{}, category: worker.ErrorTelegramDestinationUnavailable},
		{name: "unreadable 429", status: 429, body: failingReadCloser{}, category: worker.ErrorTelegramRateLimited},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Body: test.body}, nil
			})}
			_, err := (Sender{Client: client, BotToken: "secret-token", Timeout: time.Second}).Send(
				context.Background(), telegramSendRequest(t),
			)
			var sendErr *worker.SendError
			if !errors.As(err, &sendErr) || sendErr.Category != test.category {
				t.Fatalf("error = %#v, want category=%s", err, test.category)
			}
		})
	}
}

func telegramSendRequest(t *testing.T) worker.SendRequest {
	t.Helper()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	content, err := notifications.NewTelegramRenderedContent(
		notifications.TemplateTelegramAlertOpeningV1, notifications.LocalePTBR, "Alerta simples",
	)
	if err != nil {
		t.Fatal(err)
	}
	return worker.SendRequest{AttemptID: "att_1", AttemptNumber: 1, Delivery: notifications.DeliverySnapshot{
		TenantID: "tnt_1", OutboxID: "outbox_1", DeliveryID: "delivery_1",
		EventID: "alert_1", RuleID: "rule_1", Kind: notifications.NotificationKindOpening,
		Channel: notifications.ChannelTelegram, RecipientID: "sub_1",
		DestinationID: "destination_hash", TelegramChatID: 123,
		MembershipSnapshot: notifications.MembershipSnapshot{Role: "member", Status: "active", Version: 1},
		State:              notifications.DeliveryStateProcessing, TelegramContent: content.Snapshot(),
		CreatedAt: now, UpdatedAt: now,
	}}
}
