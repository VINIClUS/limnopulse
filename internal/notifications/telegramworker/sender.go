package telegramworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
)

const maxResponseBytes = 64 * 1024

type Sender struct {
	Client   *http.Client
	BotToken string
	Timeout  time.Duration
	BaseURL  string
}

type sendMessagePayload struct {
	ChatID             int64              `json:"chat_id"`
	Text               string             `json:"text"`
	LinkPreviewOptions linkPreviewOptions `json:"link_preview_options"`
}

type linkPreviewOptions struct {
	IsDisabled bool `json:"is_disabled"`
}

type telegramResponse struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
	Result      struct {
		MessageID int64 `json:"message_id"`
	} `json:"result"`
	Parameters struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func (sender Sender) Preflight(request worker.SendRequest) error {
	if sender.Client == nil || sender.Timeout <= 0 || strings.TrimSpace(sender.BotToken) == "" ||
		strings.ContainsAny(sender.BotToken, "/?#") {
		return worker.NewSendError(worker.ErrorTelegramCredentials, errors.New("Telegram sender configuration is invalid"))
	}
	if _, err := sender.apiBaseURL(); err != nil {
		return worker.NewSendError(worker.ErrorTelegramCredentials, err)
	}
	if request.Delivery.Channel != notifications.ChannelTelegram ||
		request.Delivery.TelegramChatID <= 0 || request.Delivery.DestinationID == "" {
		return worker.NewSendError(worker.ErrorTelegramDestinationUnavailable, errors.New("Telegram destination is invalid"))
	}
	if _, err := notifications.RestoreTelegramRenderedContent(request.Delivery.TelegramContent); err != nil {
		return worker.NewSendError(worker.ErrorFatalConfigurationSet, err)
	}
	return nil
}

func (sender Sender) Send(
	ctx context.Context,
	request worker.SendRequest,
) (worker.SendResult, error) {
	if err := sender.Preflight(request); err != nil {
		return worker.SendResult{}, err
	}
	payload, err := json.Marshal(sendMessagePayload{
		ChatID:             request.Delivery.TelegramChatID,
		Text:               request.Delivery.TelegramContent.BodyText,
		LinkPreviewOptions: linkPreviewOptions{IsDisabled: true},
	})
	if err != nil {
		return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, sender.Timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		sender.mustAPIBaseURL()+"/bot"+sender.BotToken+"/sendMessage",
		bytes.NewReader(payload),
	)
	if err != nil {
		return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := sender.Client.Do(httpRequest)
	if err != nil {
		sendErr := worker.NewSendError(worker.ErrorTelegramAmbiguous, err)
		sendErr.NoAutomaticRetry = true
		return worker.SendResult{}, sendErr
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusUnauthorized:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorTelegramCredentials, errors.New("Telegram credentials rejected"))
	case response.StatusCode == http.StatusForbidden:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorTelegramDestinationUnavailable, errors.New("Telegram destination rejected"))
	case response.StatusCode >= 500:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorRetryableService, errors.New("Telegram service unavailable"))
	case response.StatusCode >= 400 && response.StatusCode < 500 &&
		response.StatusCode != http.StatusTooManyRequests && response.StatusCode != http.StatusBadRequest:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("Telegram request rejected"))
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if response.StatusCode == http.StatusBadRequest {
		if readErr != nil || len(responseBody) > maxResponseBytes {
			return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("Telegram request rejected"))
		}
		var decoded telegramResponse
		if json.Unmarshal(responseBody, &decoded) == nil &&
			isTelegramDestinationRejection(decoded.Description) {
			return worker.SendResult{}, worker.NewSendError(
				worker.ErrorTelegramDestinationUnavailable,
				errors.New("Telegram destination rejected"),
			)
		}
		return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("Telegram request rejected"))
	}
	if response.StatusCode == http.StatusTooManyRequests {
		sendErr := worker.NewSendError(worker.ErrorTelegramRateLimited, errors.New("Telegram rate limit"))
		var decoded telegramResponse
		if readErr == nil && len(responseBody) <= maxResponseBytes && json.Unmarshal(responseBody, &decoded) == nil &&
			decoded.Parameters.RetryAfter > 0 {
			sendErr.RetryAfter = time.Duration(decoded.Parameters.RetryAfter) * time.Second
		}
		return worker.SendResult{}, sendErr
	}
	if readErr != nil || len(responseBody) > maxResponseBytes {
		sendErr := worker.NewSendError(worker.ErrorTelegramAmbiguous, errors.New("Telegram response could not be read"))
		sendErr.NoAutomaticRetry = true
		return worker.SendResult{}, sendErr
	}
	var decoded telegramResponse
	decodeErr := json.Unmarshal(responseBody, &decoded)
	if decodeErr != nil {
		sendErr := worker.NewSendError(worker.ErrorTelegramAmbiguous, errors.New("Telegram response was malformed"))
		sendErr.NoAutomaticRetry = true
		return worker.SendResult{}, sendErr
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 && decoded.OK && decoded.Result.MessageID > 0 {
		return worker.SendResult{ProviderMessageID: strconv.FormatInt(decoded.Result.MessageID, 10)}, nil
	}
	switch {
	case decoded.ErrorCode == http.StatusTooManyRequests:
		sendErr := worker.NewSendError(worker.ErrorTelegramRateLimited, errors.New("Telegram rate limit"))
		if decoded.Parameters.RetryAfter > 0 {
			sendErr.RetryAfter = time.Duration(decoded.Parameters.RetryAfter) * time.Second
		}
		return worker.SendResult{}, sendErr
	case decoded.ErrorCode == http.StatusUnauthorized:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorTelegramCredentials, errors.New("Telegram credentials rejected"))
	case decoded.ErrorCode == http.StatusForbidden:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorTelegramDestinationUnavailable, errors.New("Telegram destination rejected"))
	default:
		sendErr := worker.NewSendError(worker.ErrorTelegramAmbiguous, fmt.Errorf("Telegram send was not confirmed"))
		sendErr.NoAutomaticRetry = true
		return worker.SendResult{}, sendErr
	}
}

func isTelegramDestinationRejection(description string) bool {
	normalized := strings.ToLower(strings.TrimSpace(description))
	return strings.Contains(normalized, "chat not found") ||
		strings.Contains(normalized, "chat is deactivated") ||
		strings.Contains(normalized, "user is deactivated")
}

func (sender Sender) apiBaseURL() (string, error) {
	base := strings.TrimRight(strings.TrimSpace(sender.BaseURL), "/")
	if base == "" {
		base = "https://api.telegram.org"
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("Telegram Bot API base URL is invalid")
	}
	return base, nil
}

func (sender Sender) mustAPIBaseURL() string {
	base, _ := sender.apiBaseURL()
	return base
}
