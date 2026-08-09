package ses

import (
	"context"
	"errors"
	"io"
	"net"
	"net/mail"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssignerv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
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
	Credentials      aws.CredentialsProvider
	FromEmail        string
	ConfigurationSet string
	Timeout          time.Duration
}

type credentialRetrievalError struct{ err error }

func (err *credentialRetrievalError) Error() string { return "AWS credential retrieval failed" }
func (err *credentialRetrievalError) Unwrap() error { return err.err }

type classifyingCredentialsProvider struct{ provider aws.CredentialsProvider }

func (provider classifyingCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	if provider.provider == nil {
		return aws.Credentials{}, &credentialRetrievalError{err: errors.New("credentials provider is missing")}
	}
	credentials, err := provider.provider.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, &credentialRetrievalError{err: err}
	}
	return credentials, nil
}

func (provider classifyingCredentialsProvider) IsCredentialsProvider(target aws.CredentialsProvider) bool {
	return aws.IsCredentialsProvider(provider.provider, target)
}

func WrapCredentials(provider aws.CredentialsProvider) aws.CredentialsProvider {
	if _, alreadyWrapped := provider.(classifyingCredentialsProvider); alreadyWrapped {
		return provider
	}
	return classifyingCredentialsProvider{provider: provider}
}

func (sender Sender) Preflight(request worker.SendRequest) error {
	if sender.Client == nil || strings.TrimSpace(sender.FromEmail) == "" ||
		strings.ContainsAny(sender.FromEmail, "\r\n") ||
		(worker.SESConfigurationSetName(sender.ConfigurationSet)).Validate() != nil ||
		request.AttemptID == "" || request.AttemptNumber < 1 {
		return worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("SES sender configuration is invalid"))
	}
	if _, err := notifications.RestoreDelivery(request.Delivery); err != nil ||
		request.Delivery.State != notifications.DeliveryStateProcessing || request.Delivery.Channel != notifications.ChannelEmail {
		return worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("delivery snapshot is invalid"))
	}
	if err := validateRecipient(request.Delivery.NormalizedEmail); err != nil {
		return ClassifyError(RecipientError{Err: err})
	}
	for _, value := range requestTagValues(request) {
		if !sesTagValue.MatchString(value) {
			return worker.NewSendError(worker.ErrorFatalConfigurationSet, errors.New("SES tag identity is invalid"))
		}
	}
	return nil
}

func requestTagValues(request worker.SendRequest) map[string]string {
	return map[string]string{
		"delivery_id": request.Delivery.DeliveryID, "attempt_id": request.AttemptID,
		"event_id": request.Delivery.EventID, "notification_kind": string(request.Delivery.Kind),
		"channel": string(request.Delivery.Channel),
	}
}

func (sender Sender) Send(ctx context.Context, request worker.SendRequest) (worker.SendResult, error) {
	if err := sender.Preflight(request); err != nil {
		return worker.SendResult{}, err
	}
	tagValues := requestTagValues(request)
	// Retrieve through the same cache used by the SDK immediately before SendEmail.
	// This gives credential refresh failures a typed, PII-free fatal category
	// instead of depending on text in the SDK's operation-error wrappers.
	if sender.Credentials != nil {
		if _, err := sender.Credentials.Retrieve(ctx); err != nil {
			return worker.SendResult{}, worker.NewSendError(worker.ErrorFatalCredentials, err)
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

func validateRecipient(value string) error {
	if value != strings.TrimSpace(value) || strings.ContainsAny(value, "\r\n") {
		return errors.New("SES recipient is invalid")
	}
	for index := 0; index < len(value); index++ {
		if value[index] > 0x7f {
			return errors.New("SES recipient is invalid")
		}
	}
	parsed, err := mail.ParseAddress(value)
	if err != nil || parsed.Name != "" || parsed.Address != value {
		return errors.New("SES recipient is invalid")
	}
	at := strings.LastIndexByte(value, '@')
	if at < 1 || at == len(value)-1 {
		return errors.New("SES recipient is invalid")
	}
	domain := value[at+1:]
	if !strings.ContainsRune(domain, '.') || strings.HasPrefix(domain, ".") || strings.HasSuffix(domain, ".") {
		return errors.New("SES recipient is invalid")
	}
	return nil
}

func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	var recipient RecipientError
	if errors.As(err, &recipient) {
		return worker.NewSendError(worker.ErrorPermanentRecipient, err)
	}
	var credentialError *credentialRetrievalError
	if errors.As(err, &credentialError) {
		return worker.NewSendError(worker.ErrorFatalCredentials, err)
	}
	var signingError *awssignerv4.SigningError
	if errors.As(err, &signingError) {
		return worker.NewSendError(worker.ErrorFatalCredentials, err)
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
		category := classifyAPIError(apiError)
		return worker.NewSendError(category, err)
	}
	return worker.NewSendError(worker.ErrorRetryableUnknown, err)
}

func classifyAPIError(apiError smithy.APIError) worker.SendErrorCategory {
	code := apiError.ErrorCode()
	switch code {
	case "ThrottlingException":
		detail := strings.TrimSuffix(strings.TrimSpace(apiError.ErrorMessage()), ".")
		if detail == "Daily message quota exceeded" {
			return worker.ErrorRetryableQuota
		}
		return worker.ErrorRetryableThrottling
	case "TooManyRequestsException", "Throttling":
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
