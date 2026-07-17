package ses

import (
	"context"
	"errors"
	"io"
	"net"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsses "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/smithy-go"
)

const DefaultTimeout = 15 * time.Second

var sesTagValue = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

type Client interface {
	SendEmail(context.Context, *awsses.SendEmailInput, ...func(*awsses.Options)) (*awsses.SendEmailOutput, error)
}

type Sender struct {
	Client           Client
	FromEmail        string
	ConfigurationSet string
	Timeout          time.Duration
}

func (sender Sender) Send(ctx context.Context, request worker.SendRequest) (worker.SendResult, error) {
	if sender.Client == nil || strings.TrimSpace(sender.FromEmail) == "" ||
		strings.ContainsAny(sender.FromEmail, "\r\n") || sender.ConfigurationSet != "limnopulse-notifications" ||
		request.AttemptID == "" || request.AttemptNumber < 1 {
		return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("SES sender configuration is invalid"))
	}
	if _, err := notifications.RestoreDelivery(request.Delivery); err != nil ||
		request.Delivery.State != notifications.DeliveryStateProcessing || request.Delivery.Channel != notifications.ChannelEmail {
		return worker.SendResult{}, worker.NewSendError(worker.ErrorRetryableUnknown, errors.New("delivery snapshot is invalid"))
	}
	tagValues := map[string]string{
		"delivery_id": request.Delivery.DeliveryID, "attempt_id": request.AttemptID,
		"event_id": request.Delivery.EventID, "notification_kind": string(request.Delivery.Kind),
		"channel": string(request.Delivery.Channel),
	}
	for _, value := range tagValues {
		if !sesTagValue.MatchString(value) {
			return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("SES tag identity is invalid"))
		}
	}
	content := request.Delivery.Content
	input := &awsses.SendEmailInput{
		FromEmailAddress: aws.String(sender.FromEmail), ConfigurationSetName: aws.String(sender.ConfigurationSet),
		Destination: &types.Destination{ToAddresses: []string{request.Delivery.NormalizedEmail}},
		Content: &types.EmailContent{Simple: &types.Message{
			Subject: &types.Content{Data: aws.String(content.Subject), Charset: aws.String("UTF-8")},
			Body: &types.Body{
				Text: &types.Content{Data: aws.String(content.Text), Charset: aws.String("UTF-8")},
				Html: &types.Content{Data: aws.String(content.HTML), Charset: aws.String("UTF-8")},
			},
		}},
		EmailTags: []types.MessageTag{
			{Name: aws.String("delivery_id"), Value: aws.String(tagValues["delivery_id"])},
			{Name: aws.String("attempt_id"), Value: aws.String(tagValues["attempt_id"])},
			{Name: aws.String("event_id"), Value: aws.String(tagValues["event_id"])},
			{Name: aws.String("notification_kind"), Value: aws.String(tagValues["notification_kind"])},
			{Name: aws.String("channel"), Value: aws.String(tagValues["channel"])},
		},
	}
	timeout := sender.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	providerCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	output, err := sender.Client.SendEmail(providerCtx, input)
	if err != nil {
		return worker.SendResult{}, ClassifyError(err)
	}
	if output == nil || strings.TrimSpace(aws.ToString(output.MessageId)) == "" {
		return worker.SendResult{}, worker.NewSendError(worker.ErrorRetryableUnknown, errors.New("SES did not return a message ID"))
	}
	return worker.SendResult{ProviderMessageID: aws.ToString(output.MessageId)}, nil
}

type RecipientError struct{ Err error }

func (err RecipientError) Error() string { return "provider rejected a definite recipient" }
func (err RecipientError) Unwrap() error { return err.Err }

func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	var recipient RecipientError
	if errors.As(err, &recipient) {
		return worker.NewSendError(worker.ErrorPermanentRecipient, err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return worker.NewSendError(worker.ErrorAmbiguousTimeout, err)
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return worker.NewSendError(worker.ErrorAmbiguousConnectionReset, err)
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return worker.NewSendError(worker.ErrorAmbiguousTimeout, err)
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		category := classifyAPICode(apiError.ErrorCode())
		return worker.NewSendError(category, err)
	}
	return worker.NewSendError(worker.ErrorRetryableUnknown, err)
}

func classifyAPICode(code string) worker.SendErrorCategory {
	switch code {
	case "TooManyRequestsException", "Throttling", "ThrottlingException":
		return worker.ErrorRetryableThrottling
	case "LimitExceededException", "AccountDailyQuotaExceeded":
		return worker.ErrorRetryableQuota
	case "ServiceUnavailableException", "InternalServerException", "InternalFailure":
		return worker.ErrorRetryableService
	case "AccountSuspendedException", "SendingPausedException":
		return worker.ErrorFatalAccountSuspended
	case "MailFromDomainNotVerifiedException", "EmailAddressNotVerifiedException":
		return worker.ErrorFatalFromIdentity
	case "NotFoundException":
		return worker.ErrorFatalConfigurationSet
	case "AccessDeniedException", "AccessDenied", "UnrecognizedClientException", "UnrecognizedClient",
		"InvalidClientTokenId", "ExpiredTokenException", "ExpiredToken":
		return worker.ErrorFatalCredentials
	default:
		return worker.ErrorRetryableUnknown
	}
}

func NewClient(config aws.Config, endpoint string) *awsses.Client {
	return awsses.NewFromConfig(config, func(options *awsses.Options) {
		options.RetryMaxAttempts = 1
		if strings.TrimSpace(endpoint) != "" {
			options.BaseEndpoint = aws.String(strings.TrimSpace(endpoint))
		}
	})
}

var _ worker.EmailSender = Sender{}
