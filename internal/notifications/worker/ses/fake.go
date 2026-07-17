package ses

import (
	"context"
	"errors"
	"syscall"

	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
)

type FakeMode string

const (
	FakeSuccess          FakeMode = "success"
	FakeRetryable        FakeMode = "retryable"
	FakePermanent        FakeMode = "permanent"
	FakeAmbiguousTimeout FakeMode = "ambiguous_timeout"
	FakeConnectionReset  FakeMode = "connection_reset"
)

type FakeSender struct {
	Mode      FakeMode
	MessageID string
}

func (sender FakeSender) Send(_ context.Context, request worker.SendRequest) (worker.SendResult, error) {
	switch sender.Mode {
	case "", FakeSuccess:
		messageID := sender.MessageID
		if messageID == "" {
			messageID = "fake_message_" + request.AttemptID
		}
		return worker.SendResult{ProviderMessageID: messageID}, nil
	case FakeRetryable:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorRetryableService, errors.New("fake service unavailable"))
	case FakePermanent:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorPermanentRecipient, errors.New("fake bad recipient"))
	case FakeAmbiguousTimeout:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorAmbiguousTimeout, context.DeadlineExceeded)
	case FakeConnectionReset:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorAmbiguousConnectionReset, syscall.ECONNRESET)
	default:
		return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("fake sender mode is invalid"))
	}
}

var _ worker.EmailSender = FakeSender{}
